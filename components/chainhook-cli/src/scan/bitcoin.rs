use crate::config::Config;
use chainhook_event_observer::bitcoincore_rpc::bitcoin::BlockHash;
use chainhook_event_observer::bitcoincore_rpc::RpcApi;
use chainhook_event_observer::bitcoincore_rpc::{Auth, Client};
use chainhook_event_observer::chainhooks::bitcoin::{
    handle_bitcoin_hook_action, BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_event_observer::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinPredicateType, HookAction, OrdinalOperations,
    Protocols,
};
use chainhook_event_observer::indexer::bitcoin::{
    retrieve_block_hash, retrieve_full_block_breakdown_with_retry,
};
use chainhook_event_observer::indexer::ordinals::db::{
    initialize_ordinal_state_storage, open_readonly_ordinals_db_conn,
    retrieve_satoshi_point_using_local_storage, write_compacted_block_to_index, CompactedBlock,
};
use chainhook_event_observer::indexer::ordinals::ord::indexing::entry::Entry;
use chainhook_event_observer::indexer::ordinals::ord::indexing::{
    HEIGHT_TO_BLOCK_HASH, INSCRIPTION_NUMBER_TO_INSCRIPTION_ID,
};
use chainhook_event_observer::indexer::ordinals::ord::initialize_ordinal_index;
use chainhook_event_observer::indexer::ordinals::ord::inscription_id::InscriptionId;
use chainhook_event_observer::indexer::{self, BitcoinChainContext};
use chainhook_event_observer::observer::{
    BitcoinConfig, EventObserverConfig, DEFAULT_CONTROL_PORT, DEFAULT_INGESTION_PORT,
};
use chainhook_event_observer::redb::ReadableTable;
use chainhook_event_observer::utils::{file_append, send_request, Context};
use chainhook_types::{BitcoinTransactionData, BlockIdentifier, OrdinalOperation};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::mpsc::channel;

pub async fn scan_bitcoin_chain_with_predicate(
    predicate: BitcoinChainhookFullSpecification,
    apply: bool,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // build_tx_coord_cache();
    // return Ok(());

    // let block_id = 1;

    // let conn = initialize_block_cache();

    // let block = CompactedBlock(vec![(vec![(148903239, 2423, 323940940)], vec![243242394023])]);

    // write_compacted_block_to_index(block_id, &block, &conn);

    // println!("{:?}", retrieve_compacted_block_from_index(block_id, &conn));

    let auth = Auth::UserPass(
        config.network.bitcoin_node_rpc_username.clone(),
        config.network.bitcoin_node_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoin_node_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("Bitcoin RPC error: {}", message.to_string()));
        }
    };

    let predicate_uuid = predicate.uuid.clone();
    let predicate_spec =
        match predicate.into_selected_network_specification(&config.network.bitcoin_network) {
            Ok(predicate) => predicate,
            Err(e) => {
                return Err(format!(
                    "Specification missing for network {:?}: {e}",
                    config.network.bitcoin_network
                ));
            }
        };

    let start_block = match predicate_spec.start_block {
        Some(start_block) => start_block,
        None => {
            return Err(
                "Bitcoin chainhook specification must include a field start_block in replay mode"
                    .into(),
            );
        }
    };
    let tip_height = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks,
        Err(e) => {
            return Err(format!(
                "unable to retrieve Bitcoin chain tip ({})",
                e.to_string()
            ));
        }
    };
    let end_block = predicate_spec.end_block.unwrap_or(tip_height);

    info!(
        ctx.expect_logger(),
        "Processing Bitcoin chainhook {}, will scan blocks [{}; {}] (apply = {})",
        predicate_uuid,
        start_block,
        end_block,
        apply
    );

    // Optimization: we will use the ordinal storage to provide a set of hints.
    let mut inscriptions_hints = BTreeMap::new();
    let mut is_scanning_inscriptions = false;
    let event_observer_config = EventObserverConfig {
        normalization_enabled: true,
        grpc_server_enabled: false,
        hooks_enabled: true,
        bitcoin_rpc_proxy_enabled: true,
        event_handlers: vec![],
        chainhook_config: None,
        ingestion_port: DEFAULT_INGESTION_PORT,
        control_port: DEFAULT_CONTROL_PORT,
        bitcoin_node_username: config.network.bitcoin_node_rpc_username.clone(),
        bitcoin_node_password: config.network.bitcoin_node_rpc_password.clone(),
        bitcoin_node_rpc_url: config.network.bitcoin_node_rpc_url.clone(),
        stacks_node_rpc_url: config.network.stacks_node_rpc_url.clone(),
        operators: HashSet::new(),
        display_logs: false,
        cache_path: format!("{}", config.expected_cache_path().display()),
        bitcoin_network: config.network.bitcoin_network.clone(),
        stacks_network: config.network.stacks_network.clone(),
    };

    let ordinal_index = if let BitcoinPredicateType::Protocol(Protocols::Ordinal(
        OrdinalOperations::InscriptionRevealed,
    )) = &predicate_spec.predicate
    {
        let ordinal_index = match initialize_ordinal_index(&event_observer_config, None, &ctx) {
            Ok(index) => index,
            Err(e) => {
                panic!()
            }
        };

        for (_inscription_number, inscription_id) in ordinal_index
            .database
            .begin_read()
            .unwrap()
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)
            .unwrap()
            .iter()
            .unwrap()
        {
            let inscription = InscriptionId::load(*inscription_id.value());

            let entry = ordinal_index
                .get_inscription_entry(inscription)
                .unwrap()
                .unwrap();

            let blockhash = ordinal_index
                .database
                .begin_read()
                .unwrap()
                .open_table(HEIGHT_TO_BLOCK_HASH)
                .unwrap()
                .get(&entry.height)
                .unwrap()
                .map(|k| BlockHash::load(*k.value()))
                .unwrap();

            inscriptions_hints.insert(entry.height, blockhash);
        }
        is_scanning_inscriptions = true;
        Some(ordinal_index)
    } else {
        None
    };

    let mut total_hits = vec![];

    let (retrieve_ordinal_tx, retrieve_ordinal_rx) = channel::<
        std::option::Option<(BlockIdentifier, BitcoinTransactionData, Vec<HookAction>)>,
    >();
    let (process_ordinal_tx, process_ordinal_rx) = channel();
    let (cache_block_tx, cache_block_rx) = channel();

    let cache_path = config.expected_cache_path();
    let ctx_ = ctx.clone();
    let handle_1 = hiro_system_kit::thread_named("Ordinal retrieval")
        .spawn(move || {
            while let Ok(Some((block_identifier, mut transaction, actions))) =
                retrieve_ordinal_rx.recv()
            {
                info!(
                    ctx_.expect_logger(),
                    "Retrieving satoshi point for {}", transaction.transaction_identifier.hash
                );

                let storage_conn = open_readonly_ordinals_db_conn(&cache_path, &ctx_).unwrap();
                let res = retrieve_satoshi_point_using_local_storage(
                    &storage_conn,
                    &block_identifier,
                    &transaction.transaction_identifier,
                    &ctx_,
                );
                let (block_number, block_offset, _) = match res {
                    Ok(res) => res,
                    Err(err) => {
                        println!("{err}");
                        let _ = process_ordinal_tx.send(None);
                        return;
                    }
                };
                if let Some(OrdinalOperation::InscriptionRevealed(inscription)) =
                    transaction.metadata.ordinal_operations.get_mut(0)
                {
                    inscription.ordinal_offset = block_offset;
                    inscription.ordinal_block_height = block_number;
                }
                let _ = process_ordinal_tx.send(Some((transaction, actions)));
            }
            let _ = process_ordinal_tx.send(None);
        })
        .expect("unable to detach thread");

    let handle_2 = hiro_system_kit::thread_named("Ordinal ingestion")
        .spawn(move || {
            while let Ok(Some((transaction, actions))) = process_ordinal_rx.recv() {
                let txid = &transaction.transaction_identifier.hash[2..];
                if let Some(OrdinalOperation::InscriptionRevealed(inscription)) =
                    transaction.metadata.ordinal_operations.get(0)
                {
                    println!(
                        "Executing action for {txid} - {}:{}",
                        inscription.ordinal_block_height, inscription.ordinal_offset
                    );
                }
            }
        })
        .expect("unable to detach thread");

    let ctx_ = ctx.clone();
    let conn = initialize_ordinal_state_storage(&config.expected_cache_path(), &ctx_);
    let handle_3 = hiro_system_kit::thread_named("Ordinal ingestion")
        .spawn(move || {
            while let Ok(Some((height, compacted_block))) = cache_block_rx.recv() {
                info!(ctx_.expect_logger(), "Caching block #{height}");
                write_compacted_block_to_index(height, &compacted_block, &conn, &ctx_);
            }
        })
        .expect("unable to detach thread");

    let mut pipeline_started = false;

    let bitcoin_config = event_observer_config.get_bitcoin_config();

    for cursor in start_block..=end_block {
        debug!(
            ctx.expect_logger(),
            "Evaluating predicate #{} on block #{}", predicate_uuid, cursor
        );

        let block_hash = if false {
            match inscriptions_hints.remove(&cursor) {
                Some(block_hash) => block_hash.to_string(),
                None => continue,
            }
        } else {
            loop {
                match retrieve_block_hash(&bitcoin_config, &cursor).await {
                    Ok(res) => break res,
                    Err(e) => {
                        error!(ctx.expect_logger(), "Error retrieving block {}", cursor,);
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(3000));
            }
        };

        let block_breakdown =
            retrieve_full_block_breakdown_with_retry(&bitcoin_config, &block_hash, ctx).await?;

        let _ = cache_block_tx.send(Some((
            block_breakdown.height as u32,
            CompactedBlock::from_full_block(&block_breakdown),
        )));

        let block = indexer::bitcoin::standardize_bitcoin_block(
            &event_observer_config,
            block_breakdown,
            ctx,
        )?;

        let mut hits = vec![];
        for tx in block.transactions.iter() {
            if predicate_spec
                .predicate
                .evaluate_transaction_predicate(&tx, ctx)
            {
                info!(
                    ctx.expect_logger(),
                    "Action #{} triggered by transaction {} (block #{})",
                    predicate_uuid,
                    tx.transaction_identifier.hash,
                    cursor
                );
                if is_scanning_inscriptions {
                    pipeline_started = true;
                    let _ = retrieve_ordinal_tx.send(Some((
                        block.block_identifier.clone(),
                        tx.clone(),
                        vec![predicate_spec.action.clone()],
                    )));
                } else {
                    hits.push(tx);
                }
                total_hits.push(tx.transaction_identifier.hash.to_string());
            }
        }

        if hits.len() > 0 {
            if apply {
                if is_scanning_inscriptions {

                    // Start thread pool hitting bitdoind
                    // Emitting ordinal updates
                } else {
                    let trigger = BitcoinTriggerChainhook {
                        chainhook: &predicate_spec,
                        apply: vec![(hits, &block)],
                        rollback: vec![],
                    };

                    let proofs = HashMap::new();
                    match handle_bitcoin_hook_action(trigger, &proofs) {
                        Err(e) => {
                            error!(ctx.expect_logger(), "unable to handle action {}", e);
                        }
                        Ok(BitcoinChainhookOccurrence::Http(request)) => {
                            send_request(request, &ctx).await;
                        }
                        Ok(BitcoinChainhookOccurrence::File(path, bytes)) => {
                            file_append(path, bytes, &ctx)
                        }
                        Ok(BitcoinChainhookOccurrence::Data(_payload)) => unreachable!(),
                    }
                }
            }
        }
    }

    if pipeline_started {
        let _ = retrieve_ordinal_tx.send(None);
        handle_3.join();
        handle_1.join();
        handle_2.join();
    }

    Ok(())
}
