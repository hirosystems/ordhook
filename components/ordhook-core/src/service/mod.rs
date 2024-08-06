mod http_api;
pub mod observers;
mod runloops;

use crate::config::{Config, PredicatesApi};
use crate::core::meta_protocols::brc20::brc20_activation_height;
use crate::core::meta_protocols::brc20::cache::Brc20MemoryCache;
use crate::core::meta_protocols::brc20::db::{
    open_readwrite_brc20_db_conn, write_augmented_block_to_brc20_db,
};
use crate::core::meta_protocols::brc20::parser::ParsedBrc20Operation;
use crate::core::meta_protocols::brc20::verifier::{
    verify_brc20_operation, verify_brc20_transfer, VerifiedBrc20Operation,
};
use crate::core::pipeline::download_and_pipeline_blocks;
use crate::core::pipeline::processors::block_archiving::start_block_archiving_processor;
use crate::core::pipeline::processors::inscription_indexing::process_block;
use crate::core::pipeline::processors::start_inscription_indexing_processor;
use crate::core::pipeline::processors::transfers_recomputing::start_transfers_recomputing_processor;
use crate::core::protocol::inscription_parsing::{
    get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
};
use crate::core::protocol::inscription_sequencing::SequenceCursor;
use crate::core::{new_traversals_lazy_cache, should_sync_ordhook_db, should_sync_rocks_db};
use crate::db::{
    delete_data_in_ordhook_db, find_latest_inscription_block_height, insert_entry_in_blocks,
    open_ordhook_db_conn_rocks_db_loop, open_readonly_ordhook_db_conn, open_readwrite_ordhook_dbs,
    update_ordinals_db_with_block, BlockBytesCursor, TransactionBytesCursor,
};
use crate::db::{find_missing_blocks, run_compaction, update_sequence_metadata_with_block};
use crate::scan::bitcoin::process_block_with_predicates;
use crate::service::observers::create_and_consolidate_chainhook_config_with_predicates;
use crate::service::runloops::start_bitcoin_scan_runloop;
use crate::{try_debug, try_error, try_info};
use chainhook_sdk::chainhooks::bitcoin::BitcoinChainhookOccurrencePayload;
use chainhook_sdk::chainhooks::types::{
    BitcoinChainhookSpecification, ChainhookConfig, ChainhookFullSpecification,
    ChainhookSpecification,
};
use chainhook_sdk::observer::{
    start_event_observer, BitcoinBlockDataCached, DataHandlerEvent, EventObserverConfig,
    HandleBlock, ObserverCommand, ObserverEvent, ObserverSidecar,
};
use chainhook_sdk::types::{
    BitcoinBlockData, BlockIdentifier, Brc20BalanceData, Brc20Operation, Brc20TokenDeployData,
    Brc20TransferData, OrdinalOperation,
};
use chainhook_sdk::utils::{BlockHeights, Context};
use crossbeam_channel::unbounded;
use crossbeam_channel::{select, Sender};
use dashmap::DashMap;
use fxhash::FxHasher;
use http_api::start_observers_http_server;
use rusqlite::Transaction;

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;
use std::sync::mpsc::channel;
use std::sync::Arc;

pub struct Service {
    pub config: Config,
    pub ctx: Context,
}

impl Service {
    pub fn new(config: Config, ctx: Context) -> Self {
        Self { config, ctx }
    }

    pub async fn run(
        &mut self,
        observer_specs: Vec<BitcoinChainhookSpecification>,
        predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
        check_blocks_integrity: bool,
        stream_indexing_to_observers: bool,
    ) -> Result<(), String> {
        let mut event_observer_config = self.config.get_event_observer_config();

        let block_post_processor = if stream_indexing_to_observers && !observer_specs.is_empty() {
            let mut chainhook_config: ChainhookConfig = ChainhookConfig::new();
            let specs = observer_specs.clone();
            for mut observer_spec in specs.into_iter() {
                observer_spec.enabled = true;
                let spec = ChainhookSpecification::Bitcoin(observer_spec);
                chainhook_config.register_specification(spec)?;
            }
            event_observer_config.chainhook_config = Some(chainhook_config);
            let block_tx = start_observer_forwarding(&event_observer_config, &self.ctx);
            Some(block_tx)
        } else {
            None
        };

        // Catch-up with chain tip
        self.catch_up_with_chain_tip(false, check_blocks_integrity, block_post_processor)
            .await?;
        info!(
            self.ctx.expect_logger(),
            "Database up to date, service will start streaming blocks"
        );

        // Sidecar channels setup
        let observer_sidecar = self.set_up_observer_sidecar_runloop()?;

        // Create the chainhook runloop tx/rx comms
        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let ordhook_config = self.config.get_ordhook_config();
        let inner_ctx = if ordhook_config.logs.chainhook_internals {
            self.ctx.clone()
        } else {
            Context::empty()
        };

        // Observers handling
        let ordhook_db =
            open_readonly_ordhook_db_conn(&self.config.expected_cache_path(), &self.ctx)
                .expect("unable to retrieve ordhook db");
        let chain_tip_height =
            find_latest_inscription_block_height(&ordhook_db, &self.ctx)?.unwrap();
        // 1) update event_observer_config with observers ready to be used
        // 2) catch-up outdated observers by dispatching replays
        let (chainhook_config, outdated_observers) =
            create_and_consolidate_chainhook_config_with_predicates(
                observer_specs,
                chain_tip_height,
                predicate_activity_relayer.is_some(),
                &self.config,
                &self.ctx,
            )?;
        // Dispatch required replays
        for outdated_observer_spec in outdated_observers.into_iter() {
            let _ = observer_command_tx.send(ObserverCommand::RegisterPredicate(
                ChainhookFullSpecification::Bitcoin(outdated_observer_spec),
            ));
        }
        event_observer_config.chainhook_config = Some(chainhook_config);

        let _ = start_event_observer(
            event_observer_config,
            observer_command_tx.clone(),
            observer_command_rx,
            Some(observer_event_tx),
            Some(observer_sidecar),
            None,
            inner_ctx,
        );

        // If HTTP Predicates API is on, we start:
        // - Thread pool in charge of performing replays
        // - API server
        self.start_main_runloop_with_dynamic_predicates(
            &observer_command_tx,
            observer_event_rx,
            predicate_activity_relayer,
        )?;
        Ok(())
    }

    pub async fn start_event_observer(
        &mut self,
        observer_sidecar: ObserverSidecar,
    ) -> Result<
        (
            std::sync::mpsc::Sender<ObserverCommand>,
            crossbeam_channel::Receiver<ObserverEvent>,
        ),
        String,
    > {
        let mut event_observer_config = self.config.get_event_observer_config();
        let (chainhook_config, _) = create_and_consolidate_chainhook_config_with_predicates(
            vec![],
            0,
            true,
            &self.config,
            &self.ctx,
        )?;

        event_observer_config.chainhook_config = Some(chainhook_config);

        let ordhook_config = self.config.get_ordhook_config();

        // Create the chainhook runloop tx/rx comms
        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();

        let inner_ctx = if ordhook_config.logs.chainhook_internals {
            self.ctx.clone()
        } else {
            Context::empty()
        };

        let _ = start_event_observer(
            event_observer_config.clone(),
            observer_command_tx.clone(),
            observer_command_rx,
            Some(observer_event_tx),
            Some(observer_sidecar),
            None,
            inner_ctx,
        );

        Ok((observer_command_tx, observer_event_rx))
    }

    pub fn start_main_runloop(
        &self,
        _observer_command_tx: &std::sync::mpsc::Sender<ObserverCommand>,
        observer_event_rx: crossbeam_channel::Receiver<ObserverEvent>,
        predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
    ) -> Result<(), String> {
        loop {
            let event = match observer_event_rx.recv() {
                Ok(cmd) => cmd,
                Err(e) => {
                    error!(
                        self.ctx.expect_logger(),
                        "Error: broken channel {}",
                        e.to_string()
                    );
                    break;
                }
            };
            match event {
                ObserverEvent::BitcoinPredicateTriggered(data) => {
                    if let Some(ref tx) = predicate_activity_relayer {
                        let _ = tx.send(data);
                    }
                }
                ObserverEvent::Terminate => {
                    info!(self.ctx.expect_logger(), "Terminating runloop");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn start_main_runloop_with_dynamic_predicates(
        &self,
        observer_command_tx: &std::sync::mpsc::Sender<ObserverCommand>,
        observer_event_rx: crossbeam_channel::Receiver<ObserverEvent>,
        _predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
    ) -> Result<(), String> {
        let (bitcoin_scan_op_tx, bitcoin_scan_op_rx) = crossbeam_channel::unbounded();
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let observer_command_tx_moved = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Bitcoin scan runloop")
            .spawn(move || {
                start_bitcoin_scan_runloop(
                    &config,
                    bitcoin_scan_op_rx,
                    observer_command_tx_moved,
                    &ctx,
                );
            })
            .expect("unable to spawn thread");

        if let PredicatesApi::On(_) = self.config.http_api {
            let moved_config = self.config.clone();
            let moved_ctx = self.ctx.clone();
            let moved_observer_commands_tx = observer_command_tx.clone();
            let _ = hiro_system_kit::thread_named("HTTP Observers API").spawn(move || {
                let _ = hiro_system_kit::nestable_block_on(start_observers_http_server(
                    &moved_config,
                    &moved_observer_commands_tx,
                    observer_event_rx,
                    bitcoin_scan_op_tx,
                    &moved_ctx,
                ));
            });
        }

        Ok(())
    }

    pub fn set_up_observer_config(
        &self,
        predicates: Vec<BitcoinChainhookSpecification>,
        enable_internal_trigger: bool,
    ) -> Result<
        (
            EventObserverConfig,
            Option<crossbeam_channel::Receiver<DataHandlerEvent>>,
        ),
        String,
    > {
        let mut event_observer_config = self.config.get_event_observer_config();
        let (chainhook_config, _) = create_and_consolidate_chainhook_config_with_predicates(
            predicates,
            0,
            enable_internal_trigger,
            &self.config,
            &self.ctx,
        )?;
        event_observer_config.chainhook_config = Some(chainhook_config);
        let data_rx = if enable_internal_trigger {
            let (tx, rx) = crossbeam_channel::bounded(256);
            event_observer_config.data_handler_tx = Some(tx);
            Some(rx)
        } else {
            None
        };
        Ok((event_observer_config, data_rx))
    }

    pub fn set_up_observer_sidecar_runloop(&self) -> Result<ObserverSidecar, String> {
        let (block_mutator_in_tx, block_mutator_in_rx) = crossbeam_channel::unbounded();
        let (block_mutator_out_tx, block_mutator_out_rx) = crossbeam_channel::unbounded();
        let (chain_event_notifier_tx, chain_event_notifier_rx) = crossbeam_channel::unbounded();
        let observer_sidecar = ObserverSidecar {
            bitcoin_blocks_mutator: Some((block_mutator_in_tx, block_mutator_out_rx)),
            bitcoin_chain_event_notifier: Some(chain_event_notifier_tx),
        };
        let cache_l2 = Arc::new(new_traversals_lazy_cache(100_000));
        let ctx = self.ctx.clone();
        let config = self.config.clone();

        let _ = hiro_system_kit::thread_named("Observer Sidecar Runloop").spawn(move || loop {
            select! {
                recv(block_mutator_in_rx) -> msg => {
                    if let Ok((mut blocks_to_mutate, blocks_ids_to_rollback)) = msg {
                        chainhook_sidecar_mutate_blocks(
                            &mut blocks_to_mutate,
                            &blocks_ids_to_rollback,
                            &cache_l2,
                            &config,
                            &ctx,
                        );
                        let _ = block_mutator_out_tx.send(blocks_to_mutate);
                    }
                }
                recv(chain_event_notifier_rx) -> msg => {
                    if let Ok(command) = msg {
                        chainhook_sidecar_mutate_ordhook_db(command, &config, &ctx)
                    }
                }
            }
        });

        Ok(observer_sidecar)
    }

    pub async fn catch_up_with_chain_tip(
        &mut self,
        _rebuild_from_scratch: bool,
        compact_and_check_rocksdb_integrity: bool,
        block_post_processor: Option<crossbeam_channel::Sender<BitcoinBlockData>>,
    ) -> Result<(), String> {
        {
            if compact_and_check_rocksdb_integrity {
                let (tip, missing_blocks) = {
                    let blocks_db = open_ordhook_db_conn_rocks_db_loop(
                        false,
                        &self.config.expected_cache_path(),
                        self.config.resources.ulimit,
                        self.config.resources.memory_available,
                        &self.ctx,
                    );

                    let ordhook_db = open_readonly_ordhook_db_conn(
                        &self.config.expected_cache_path(),
                        &self.ctx,
                    )
                    .expect("unable to retrieve ordhook db");
                    let tip = find_latest_inscription_block_height(&ordhook_db, &self.ctx)?.unwrap()
                        as u32;
                    info!(
                        self.ctx.expect_logger(),
                        "Checking database integrity up to block #{tip}",
                    );
                    let missing_blocks = find_missing_blocks(&blocks_db, 0, tip, &self.ctx);
                    (tip, missing_blocks)
                };
                if !missing_blocks.is_empty() {
                    info!(
                        self.ctx.expect_logger(),
                        "{} missing blocks detected, will attempt to repair data",
                        missing_blocks.len()
                    );
                    let block_ingestion_processor =
                        start_block_archiving_processor(&self.config, &self.ctx, false, None);
                    download_and_pipeline_blocks(
                        &self.config,
                        missing_blocks.into_iter().map(|x| x as u64).collect(),
                        tip.into(),
                        Some(&block_ingestion_processor),
                        10_000,
                        &self.ctx,
                    )
                    .await?;
                }
                let blocks_db_rw = open_ordhook_db_conn_rocks_db_loop(
                    false,
                    &self.config.expected_cache_path(),
                    self.config.resources.ulimit,
                    self.config.resources.memory_available,
                    &self.ctx,
                );
                info!(self.ctx.expect_logger(), "Running database compaction",);
                run_compaction(&blocks_db_rw, tip);
            }
        }
        self.update_state(block_post_processor).await
    }

    pub async fn update_state(
        &self,
        block_post_processor: Option<crossbeam_channel::Sender<BitcoinBlockData>>,
    ) -> Result<(), String> {
        // First, make sure that rocksdb and sqlite are aligned.
        // If rocksdb.chain_tip.height <= sqlite.chain_tip.height
        // Perform some block compression until that height.
        if let Some((start_block, end_block)) = should_sync_rocks_db(&self.config, &self.ctx)? {
            let blocks_post_processor = start_block_archiving_processor(
                &self.config,
                &self.ctx,
                true,
                block_post_processor.clone(),
            );

            try_info!(
                self.ctx,
                "Compressing blocks (from #{start_block} to #{end_block})"
            );

            let ordhook_config = self.config.get_ordhook_config();
            let first_inscription_height = ordhook_config.first_inscription_height;
            let blocks = BlockHeights::BlockRange(start_block, end_block)
                .get_sorted_entries()
                .map_err(|_e| format!("Block start / end block spec invalid"))?;
            download_and_pipeline_blocks(
                &self.config,
                blocks.into(),
                first_inscription_height,
                Some(&blocks_post_processor),
                10_000,
                &self.ctx,
            )
            .await?;
        }

        // Start predicate processor
        let mut last_block_processed = 0;
        while let Some((start_block, end_block, speed)) =
            should_sync_ordhook_db(&self.config, &self.ctx)?
        {
            if last_block_processed == end_block {
                break;
            }
            let blocks_post_processor = start_inscription_indexing_processor(
                &self.config,
                &self.ctx,
                block_post_processor.clone(),
            );

            try_info!(
                self.ctx,
                "Indexing inscriptions from block #{start_block} to block #{end_block}"
            );

            let ordhook_config = self.config.get_ordhook_config();
            let first_inscription_height = ordhook_config.first_inscription_height;
            let blocks = BlockHeights::BlockRange(start_block, end_block)
                .get_sorted_entries()
                .map_err(|_e| format!("Block start / end block spec invalid"))?;
            download_and_pipeline_blocks(
                &self.config,
                blocks.into(),
                first_inscription_height,
                Some(&blocks_post_processor),
                speed,
                &self.ctx,
            )
            .await?;

            last_block_processed = end_block;
        }

        Ok(())
    }

    pub async fn replay_transfers(
        &self,
        blocks: Vec<u64>,
        block_post_processor: Option<crossbeam_channel::Sender<BitcoinBlockData>>,
    ) -> Result<(), String> {
        // Start predicate processor
        let blocks_post_processor =
            start_transfers_recomputing_processor(&self.config, &self.ctx, block_post_processor);

        let ordhook_config = self.config.get_ordhook_config();
        let first_inscription_height = ordhook_config.first_inscription_height;
        download_and_pipeline_blocks(
            &self.config,
            blocks,
            first_inscription_height,
            Some(&blocks_post_processor),
            100,
            &self.ctx,
        )
        .await?;

        Ok(())
    }
}

fn chainhook_sidecar_mutate_ordhook_db(command: HandleBlock, config: &Config, ctx: &Context) {
    let (blocks_db_rw, inscriptions_db_conn_rw) = match open_readwrite_ordhook_dbs(
        &config.expected_cache_path(),
        config.resources.ulimit,
        config.resources.memory_available,
        &ctx,
    ) {
        Ok(dbs) => dbs,
        Err(e) => {
            try_error!(ctx, "Unable to open readwrite connection: {e}");
            return;
        }
    };
    let brc_20_db_conn_rw = if config.meta_protocols.brc20 {
        match open_readwrite_brc20_db_conn(&config.expected_cache_path(), ctx) {
            Ok(dbs) => Some(dbs),
            Err(e) => {
                try_error!(ctx, "Unable to open readwrite brc20 connection: {e}");
                return;
            }
        }
    } else {
        None
    };

    match command {
        HandleBlock::UndoBlock(block) => {
            try_info!(
                ctx,
                "Re-org handling: reverting changes in block #{}",
                block.block_identifier.index
            );
            let res = delete_data_in_ordhook_db(
                block.block_identifier.index,
                block.block_identifier.index,
                &inscriptions_db_conn_rw,
                &blocks_db_rw,
                &brc_20_db_conn_rw,
                ctx,
            );

            if let Err(e) = res {
                try_error!(
                    ctx,
                    "Unable to rollback bitcoin block {}: {e}",
                    block.block_identifier
                );
            }
        }
        HandleBlock::ApplyBlock(block) => {
            let block_bytes = match BlockBytesCursor::from_standardized_block(&block) {
                Ok(block_bytes) => block_bytes,
                Err(e) => {
                    try_error!(
                        ctx,
                        "Unable to compress block #{}: #{}",
                        block.block_identifier.index,
                        e.to_string()
                    );
                    return;
                }
            };
            insert_entry_in_blocks(
                block.block_identifier.index as u32,
                &block_bytes,
                true,
                &blocks_db_rw,
                &ctx,
            );
            if let Err(e) = blocks_db_rw.flush() {
                try_error!(ctx, "{}", e.to_string());
            }

            update_ordinals_db_with_block(&block, &inscriptions_db_conn_rw, ctx);

            update_sequence_metadata_with_block(&block, &inscriptions_db_conn_rw, &ctx);

            if let Some(brc20_conn_rw) = brc_20_db_conn_rw {
                write_augmented_block_to_brc20_db(&block, &brc20_conn_rw, ctx);
            }
        }
    }
}

pub fn start_observer_forwarding(
    event_observer_config: &EventObserverConfig,
    ctx: &Context,
) -> Sender<BitcoinBlockData> {
    let (tx_replayer, rx_replayer) = unbounded();
    let mut moved_event_observer_config = event_observer_config.clone();
    let moved_ctx = ctx.clone();

    let _ = hiro_system_kit::thread_named("Initial predicate processing")
        .spawn(move || {
            if let Some(mut chainhook_config) = moved_event_observer_config.chainhook_config.take()
            {
                let mut bitcoin_predicates_ref: Vec<&BitcoinChainhookSpecification> = vec![];
                for bitcoin_predicate in chainhook_config.bitcoin_chainhooks.iter_mut() {
                    bitcoin_predicates_ref.push(bitcoin_predicate);
                }
                while let Ok(block) = rx_replayer.recv() {
                    let future = process_block_with_predicates(
                        block,
                        &bitcoin_predicates_ref,
                        &moved_event_observer_config,
                        &moved_ctx,
                    );
                    let res = hiro_system_kit::nestable_block_on(future);
                    if let Err(_) = res {
                        error!(moved_ctx.expect_logger(), "Initial ingestion failing");
                    }
                }
            }
        })
        .expect("unable to spawn thread");

    tx_replayer
}

pub fn chainhook_sidecar_mutate_blocks(
    blocks_to_mutate: &mut Vec<BitcoinBlockDataCached>,
    blocks_ids_to_rollback: &Vec<BlockIdentifier>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    config: &Config,
    ctx: &Context,
) {
    let mut updated_blocks_ids = vec![];

    let (blocks_db_rw, mut inscriptions_db_conn_rw) = match open_readwrite_ordhook_dbs(
        &config.expected_cache_path(),
        config.resources.ulimit,
        config.resources.memory_available,
        &ctx,
    ) {
        Ok(dbs) => dbs,
        Err(e) => {
            try_error!(ctx, "Unable to open readwrite connection: {e}");
            return;
        }
    };
    let mut brc_20_db_conn_rw = if config.meta_protocols.brc20 {
        match open_readwrite_brc20_db_conn(&config.expected_cache_path(), ctx) {
            Ok(db) => Some(db),
            Err(e) => {
                try_error!(ctx, "Unable to open readwrite brc20 connection: {e}");
                return;
            }
        }
    } else {
        None
    };

    let mut brc20_cache = Brc20MemoryCache::new(config.resources.brc20_lru_cache_size);

    for block_id_to_rollback in blocks_ids_to_rollback.iter() {
        if let Err(e) = delete_data_in_ordhook_db(
            block_id_to_rollback.index,
            block_id_to_rollback.index,
            &inscriptions_db_conn_rw,
            &blocks_db_rw,
            &brc_20_db_conn_rw,
            &ctx,
        ) {
            try_error!(
                ctx,
                "Unable to rollback bitcoin block {}: {e}",
                block_id_to_rollback.index
            );
        }
    }

    let brc20_db_tx = if let Some(ref mut db_conn) = brc_20_db_conn_rw {
        Some(db_conn.transaction().unwrap())
    } else {
        None
    };
    let inscriptions_db_tx = inscriptions_db_conn_rw.transaction().unwrap();

    let ordhook_config = config.get_ordhook_config();

    for cache in blocks_to_mutate.iter_mut() {
        let block_bytes = match BlockBytesCursor::from_standardized_block(&cache.block) {
            Ok(block_bytes) => block_bytes,
            Err(e) => {
                try_error!(
                    ctx,
                    "Unable to compress block #{}: #{}",
                    cache.block.block_identifier.index,
                    e.to_string()
                );
                continue;
            }
        };

        insert_entry_in_blocks(
            cache.block.block_identifier.index as u32,
            &block_bytes,
            true,
            &blocks_db_rw,
            &ctx,
        );
        if let Err(e) = blocks_db_rw.flush() {
            try_error!(ctx, "{}", e.to_string());
        }

        if cache.processed_by_sidecar {
            update_ordinals_db_with_block(&cache.block, &inscriptions_db_tx, &ctx);
            update_sequence_metadata_with_block(&cache.block, &inscriptions_db_tx, &ctx);
        } else {
            updated_blocks_ids.push(format!("{}", cache.block.block_identifier.index));

            let mut cache_l1 = BTreeMap::new();
            let mut sequence_cursor = SequenceCursor::new(&inscriptions_db_tx);

            let _ = process_block(
                &mut cache.block,
                &vec![],
                &mut sequence_cursor,
                &mut cache_l1,
                &cache_l2,
                &inscriptions_db_tx,
                brc20_db_tx.as_ref(),
                &mut brc20_cache,
                &ordhook_config,
                &ctx,
            );

            let inscription_numbers = get_inscriptions_revealed_in_block(&cache.block)
                .iter()
                .map(|d| d.get_inscription_number().to_string())
                .collect::<Vec<String>>();

            let inscriptions_transferred =
                get_inscriptions_transferred_in_block(&cache.block).len();

            try_info!(
                ctx,
                "Block #{} processed, mutated and revealed {} inscriptions [{}] and {inscriptions_transferred} transfers",
                cache.block.block_identifier.index,
                inscription_numbers.len(),
                inscription_numbers.join(", ")
            );
            cache.processed_by_sidecar = true;
        }
    }
    let _ = inscriptions_db_tx.rollback();

    if let Some(tx) = brc20_db_tx {
        let _ = tx.rollback();
    }
}

pub fn write_brc20_block_operations(
    block: &mut BitcoinBlockData,
    brc20_operation_map: &mut HashMap<String, ParsedBrc20Operation>,
    brc20_cache: &mut Brc20MemoryCache,
    db_tx: &Transaction,
    ctx: &Context,
) {
    if block.block_identifier.index < brc20_activation_height(&block.metadata.network) {
        return;
    }
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        for op in tx.metadata.ordinal_operations.iter() {
            match op {
                OrdinalOperation::InscriptionRevealed(reveal) => {
                    if let Some(parsed_brc20_operation) =
                        brc20_operation_map.get(&reveal.inscription_id)
                    {
                        match verify_brc20_operation(
                            parsed_brc20_operation,
                            reveal,
                            &block.block_identifier,
                            &block.metadata.network,
                            brc20_cache,
                            &db_tx,
                            &ctx,
                        ) {
                            Ok(op) => match op {
                                VerifiedBrc20Operation::TokenDeploy(token) => {
                                    let dec = token.dec as usize;
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Deploy(Brc20TokenDeployData {
                                            tick: token.tick.clone(),
                                            max: format!(
                                                "{:.precision$}",
                                                token.max,
                                                precision = dec
                                            ),
                                            lim: format!(
                                                "{:.precision$}",
                                                token.lim,
                                                precision = dec
                                            ),
                                            dec: token.dec.to_string(),
                                            address: token.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                            self_mint: token.self_mint,
                                        }));
                                    brc20_cache.insert_token_deploy(
                                        &token,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 deploy {} ({}) at block {}",
                                        token.tick,
                                        token.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenMint(balance) => {
                                    let Some(token) =
                                        brc20_cache.get_token(&balance.tick, db_tx, ctx)
                                    else {
                                        unreachable!();
                                    };
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Mint(Brc20BalanceData {
                                            tick: balance.tick.clone(),
                                            amt: format!(
                                                "{:.precision$}",
                                                balance.amt,
                                                precision = token.dec as usize
                                            ),
                                            address: balance.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                        }));
                                    brc20_cache.insert_token_mint(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 mint {} {} ({}) at block {}",
                                        balance.tick,
                                        balance.amt,
                                        balance.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenTransfer(balance) => {
                                    let Some(token) =
                                        brc20_cache.get_token(&balance.tick, db_tx, ctx)
                                    else {
                                        unreachable!();
                                    };
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Transfer(Brc20BalanceData {
                                            tick: balance.tick.clone(),
                                            amt: format!(
                                                "{:.precision$}",
                                                balance.amt,
                                                precision = token.dec as usize
                                            ),
                                            address: balance.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                        }));
                                    brc20_cache.insert_token_transfer(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 transfer {} {} ({}) at block {}",
                                        balance.tick,
                                        balance.amt,
                                        balance.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenTransferSend(_) => {
                                    unreachable!("BRC-20 token transfer send should never be generated on reveal")
                                }
                            },
                            Err(e) => {
                                try_debug!(ctx, "Error validating BRC-20 operation {}", e);
                            }
                        }
                    } else {
                        brc20_cache.ignore_inscription(reveal.ordinal_number);
                    }
                }
                OrdinalOperation::InscriptionTransferred(transfer) => {
                    match verify_brc20_transfer(transfer, brc20_cache, &db_tx, &ctx) {
                        Ok(data) => {
                            let Some(token) = brc20_cache.get_token(&data.tick, db_tx, ctx) else {
                                unreachable!();
                            };
                            let Some(unsent_transfer) = brc20_cache.get_unsent_token_transfer(
                                transfer.ordinal_number,
                                db_tx,
                                ctx,
                            ) else {
                                unreachable!();
                            };
                            tx.metadata.brc20_operation =
                                Some(Brc20Operation::TransferSend(Brc20TransferData {
                                    tick: data.tick.clone(),
                                    amt: format!(
                                        "{:.precision$}",
                                        data.amt * -1.0,
                                        precision = token.dec as usize
                                    ),
                                    sender_address: data.sender_address.clone(),
                                    receiver_address: data.receiver_address.clone(),
                                    inscription_id: unsent_transfer.inscription_id,
                                }));
                            brc20_cache.insert_token_transfer_send(
                                &data,
                                &transfer,
                                &block.block_identifier,
                                tx_index as u64,
                                db_tx,
                                ctx,
                            );
                            try_info!(
                                ctx,
                                "BRC-20 transfer_send {} {} ({} -> {}) at block {}",
                                data.tick,
                                data.amt,
                                data.sender_address,
                                data.receiver_address,
                                block.block_identifier.index
                            );
                        }
                        Err(e) => {
                            try_debug!(ctx, "Error validating BRC-20 transfer {}", e);
                        }
                    }
                }
            }
        }
    }
    brc20_cache.db_cache.flush(db_tx, ctx);
}
