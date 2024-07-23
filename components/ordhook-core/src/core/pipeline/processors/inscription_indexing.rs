use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{
    types::{BitcoinBlockData, TransactionIdentifier},
    utils::Context,
};
use crossbeam_channel::{Sender, TryRecvError};
use rusqlite::Transaction;

use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::Connection;
use std::hash::BuildHasherDefault;

use crate::{
    core::{
        meta_protocols::brc20::{cache::Brc20MemoryCache, db::open_readwrite_brc20_db_conn},
        pipeline::processors::block_archiving::store_compacted_blocks,
        protocol::{
            inscription_parsing::{
                get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
                parse_inscriptions_in_standardized_block,
            },
            inscription_sequencing::{
                augment_block_with_ordinals_inscriptions_data_and_write_to_db_tx,
                get_bitcoin_network, get_jubilee_block_height,
                parallelize_inscription_data_computations, SequenceCursor,
            },
            satoshi_tracking::augment_block_with_ordinals_transfer_data,
        },
        OrdhookConfig,
    },
    db::{
        get_any_entry_in_ordinal_activities, open_ordhook_db_conn_rocks_db_loop,
        open_readonly_ordhook_db_conn,
    },
    service::write_brc20_block_operations,
    try_error, try_info,
};

use crate::db::{TransactionBytesCursor, TraversalResult};

use crate::{
    config::Config,
    core::{
        new_traversals_lazy_cache,
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    },
    db::open_readwrite_ordhook_db_conn,
};

pub fn start_inscription_indexing_processor(
    config: &Config,
    ctx: &Context,
    post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Inscription indexing runloop")
        .spawn(move || {
            let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
            let garbage_collect_every_n_blocks = 100;
            let mut garbage_collect_nth_block = 0;

            let mut inscriptions_db_conn_rw =
                open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let ordhook_config = config.get_ordhook_config();
            let mut empty_cycles = 0;

            let inscriptions_db_conn =
                open_readonly_ordhook_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let mut sequence_cursor = SequenceCursor::new(&inscriptions_db_conn);

            loop {
                let (compacted_blocks, mut blocks) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        empty_cycles = 0;
                        (compacted_blocks, blocks)
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 180 {
                                try_info!(ctx, "Block processor reached expiration");
                                let _ = events_tx.send(PostProcessorEvent::Expired);
                                break;
                            }
                            sleep(Duration::from_secs(1));
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };

                {
                    let blocks_db_rw = open_ordhook_db_conn_rocks_db_loop(
                        true,
                        &config.expected_cache_path(),
                        config.resources.ulimit,
                        config.resources.memory_available,
                        &ctx,
                    );
                    store_compacted_blocks(
                        compacted_blocks,
                        true,
                        &blocks_db_rw,
                        &Context::empty(),
                    );
                }

                // Early return
                if blocks.is_empty() {
                    continue;
                }

                try_info!(ctx, "Processing {} blocks", blocks.len());
                blocks = process_blocks(
                    &mut blocks,
                    &mut sequence_cursor,
                    &cache_l2,
                    &mut inscriptions_db_conn_rw,
                    &ordhook_config,
                    &post_processor,
                    &ctx,
                );

                garbage_collect_nth_block += blocks.len();
                if garbage_collect_nth_block > garbage_collect_every_n_blocks {
                    try_info!(ctx, "Performing garbage collecting");

                    // Clear L2 cache on a regular basis
                    try_info!(ctx, "Clearing cache L2 ({} entries)", cache_l2.len());
                    cache_l2.clear();

                    // Recreate sqlite db connection on a regular basis
                    inscriptions_db_conn_rw =
                        open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx)
                            .unwrap();
                    inscriptions_db_conn_rw.flush_prepared_statement_cache();
                    garbage_collect_nth_block = 0;
                }
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}

pub fn process_blocks(
    next_blocks: &mut Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_conn_rw: &mut Connection,
    ordhook_config: &OrdhookConfig,
    post_processor: &Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) -> Vec<BitcoinBlockData> {
    let mut cache_l1 = BTreeMap::new();

    let mut updated_blocks = vec![];

    let mut brc20_db_conn_rw = match open_readwrite_brc20_db_conn(&ordhook_config.db_path, &ctx) {
        Ok(dbs) => dbs,
        Err(e) => {
            panic!("Unable to open readwrite connection: {e}");
        }
    };
    let mut brc20_cache = Brc20MemoryCache::new(ordhook_config.resources.brc20_lru_cache_size);

    for _cursor in 0..next_blocks.len() {
        let inscriptions_db_tx: rusqlite::Transaction<'_> =
            inscriptions_db_conn_rw.transaction().unwrap();
        let brc20_db_tx = brc20_db_conn_rw.transaction().unwrap();

        let mut block = next_blocks.remove(0);

        // We check before hand if some data were pre-existing, before processing
        // Always discard if we have some existing content at this block height (inscription or transfers)
        let any_existing_activity = get_any_entry_in_ordinal_activities(
            &block.block_identifier.index,
            &inscriptions_db_tx,
            ctx,
        );

        // Invalidate and recompute cursor when crossing the jubilee height
        let jubilee_height =
            get_jubilee_block_height(&get_bitcoin_network(&block.metadata.network));
        if block.block_identifier.index == jubilee_height {
            sequence_cursor.reset();
        }

        let _ = process_block(
            &mut block,
            &next_blocks,
            sequence_cursor,
            &mut cache_l1,
            cache_l2,
            &inscriptions_db_tx,
            Some(&brc20_db_tx),
            &mut brc20_cache,
            ordhook_config,
            ctx,
        );

        let inscriptions_revealed = get_inscriptions_revealed_in_block(&block)
            .iter()
            .map(|d| d.get_inscription_number().to_string())
            .collect::<Vec<String>>();

        let inscriptions_transferred = get_inscriptions_transferred_in_block(&block).len();

        try_info!(
            ctx,
            "Block #{} processed, revealed {} inscriptions [{}] and {inscriptions_transferred} transfers",
            block.block_identifier.index,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        );

        if any_existing_activity {
            try_error!(
                ctx,
                "Dropping updates for block #{}, activities present in database",
                block.block_identifier.index,
            );
            let _ = inscriptions_db_tx.rollback();
            let _ = brc20_db_tx.rollback();
        } else {
            match inscriptions_db_tx.commit() {
                Ok(_) => match brc20_db_tx.commit() {
                    Ok(_) => {}
                    Err(_) => {
                        // delete_data_in_ordhook_db(
                        //     block.block_identifier.index,
                        //     block.block_identifier.index,
                        //     ordhook_config,
                        //     ctx,
                        // );
                        todo!()
                    }
                },
                Err(e) => {
                    try_error!(
                        ctx,
                        "Unable to update changes in block #{}: {}",
                        block.block_identifier.index,
                        e.to_string()
                    );
                }
            }
        }

        if let Some(post_processor_tx) = post_processor {
            let _ = post_processor_tx.send(block.clone());
        }
        updated_blocks.push(block);
    }
    updated_blocks
}

pub fn process_block(
    block: &mut BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_tx: &Transaction,
    brc20_db_tx: Option<&Transaction>,
    brc20_cache: &mut Brc20MemoryCache,
    ordhook_config: &OrdhookConfig,
    ctx: &Context,
) -> Result<(), String> {
    // Parsed BRC20 ops will be deposited here for this block.
    let mut brc20_operation_map = HashMap::new();
    parse_inscriptions_in_standardized_block(block, &mut brc20_operation_map, &ctx);

    let any_processable_transactions = parallelize_inscription_data_computations(
        &block,
        &next_blocks,
        cache_l1,
        cache_l2,
        inscriptions_db_tx,
        &ordhook_config,
        ctx,
    )?;

    let inner_ctx = if ordhook_config.logs.ordinals_internals {
        ctx.clone()
    } else {
        Context::empty()
    };

    // Handle inscriptions
    if any_processable_transactions {
        let _ = augment_block_with_ordinals_inscriptions_data_and_write_to_db_tx(
            block,
            sequence_cursor,
            cache_l1,
            &inscriptions_db_tx,
            &inner_ctx,
        );
    }

    // Handle transfers
    let _ = augment_block_with_ordinals_transfer_data(block, inscriptions_db_tx, true, &inner_ctx);

    if let Some(brc20_db_tx) = brc20_db_tx {
        write_brc20_block_operations(
            block,
            &mut brc20_operation_map,
            brc20_cache,
            &brc20_db_tx,
            &ctx,
        );
    }

    Ok(())
}
