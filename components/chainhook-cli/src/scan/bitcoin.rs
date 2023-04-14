use crate::config::Config;
use chainhook_event_observer::bitcoincore_rpc::RpcApi;
use chainhook_event_observer::bitcoincore_rpc::{Auth, Client};
use chainhook_event_observer::chainhooks::bitcoin::{
    evaluate_bitcoin_chainhooks_on_chain_event, handle_bitcoin_hook_action,
    BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_event_observer::chainhooks::types::{
    BitcoinChainhookSpecification, BitcoinPredicateType,
};
use chainhook_event_observer::hord::db::{
    fetch_and_cache_blocks_in_hord_db, find_all_inscriptions, find_block_at_block_height,
    find_last_block_inserted, open_readonly_hord_db_conn, open_readonly_hord_db_conn_rocks_db,
    open_readwrite_hord_db_conn, open_readwrite_hord_db_conn_rocks_db,
};
use chainhook_event_observer::hord::{
    update_storage_and_augment_bitcoin_block_with_inscription_reveal_data,
    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data, Storage,
};
use chainhook_event_observer::indexer;
use chainhook_event_observer::indexer::bitcoin::{
    download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_event_observer::observer::{gather_proofs, EventObserverConfig};
use chainhook_event_observer::utils::{file_append, send_request, Context};
use chainhook_types::{BitcoinChainEvent, BitcoinChainUpdatedWithBlocksData};
use std::collections::{BTreeMap, HashMap};

pub async fn scan_bitcoin_chainstate_via_http_using_predicate(
    predicate_spec: BitcoinChainhookSpecification,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let auth = Auth::UserPass(
        config.network.bitcoind_rpc_username.clone(),
        config.network.bitcoind_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoind_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("Bitcoin RPC error: {}", message.to_string()));
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
    let end_block = match predicate_spec.end_block {
        Some(end_block) => end_block,
        None => match bitcoin_rpc.get_blockchain_info() {
            Ok(result) => result.blocks,
            Err(e) => {
                return Err(format!(
                    "unable to retrieve Bitcoin chain tip ({})",
                    e.to_string()
                ));
            }
        },
    };

    // Are we dealing with an ordinals-based predicate?
    // If so, we could use the ordinal storage to provide a set of hints.
    let mut inscriptions_cache = BTreeMap::new();
    let mut is_predicate_evaluating_ordinals = false;
    let mut hord_blocks_requires_update = false;

    if let BitcoinPredicateType::OrdinalsProtocol(_) = &predicate_spec.predicate {
        is_predicate_evaluating_ordinals = true;
        if let Ok(inscriptions_db_conn) =
            open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)
        {
            inscriptions_cache = find_all_inscriptions(&inscriptions_db_conn);
            // Will we have to update the blocks table?
            if let Ok(blocks_db) =
                open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)
            {
                if find_block_at_block_height(end_block as u32, &blocks_db).is_none() {
                    hord_blocks_requires_update = true;
                }
            }
        }
    }

    // Do we need a seeded hord db?
    if is_predicate_evaluating_ordinals && inscriptions_cache.is_empty() {
        // Do we need to update the blocks table first?
        if hord_blocks_requires_update {
            // Count how many entries in the table
            // Compute the right interval
            // Start the build local storage routine

            // TODO: make sure that we have a contiguous chain
            // check_compacted_blocks_chain_integrity(&hord_db_conn);

            let blocks_db_rw =
                open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), ctx)?;

            let start_block = find_last_block_inserted(&blocks_db_rw) as u64;
            if start_block < end_block {
                warn!(
                    ctx.expect_logger(),
                    "Database hord.sqlite appears to be outdated regarding the window of blocks provided. Syncing {} missing blocks",
                    (end_block - start_block)
                );

                let inscriptions_db_conn_rw =
                    open_readwrite_hord_db_conn(&config.expected_cache_path(), ctx)?;
                fetch_and_cache_blocks_in_hord_db(
                    &config.get_event_observer_config().get_bitcoin_config(),
                    &blocks_db_rw,
                    &inscriptions_db_conn_rw,
                    start_block,
                    end_block,
                    8,
                    &config.expected_cache_path(),
                    &ctx,
                )
                .await?;

                inscriptions_cache = find_all_inscriptions(&inscriptions_db_conn_rw);
            }
        }
    }

    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on Bitcoin blocks",
    );

    let mut blocks_scanned = 0;
    let mut actions_triggered = 0;
    let mut err_count = 0;

    let event_observer_config = config.get_event_observer_config();
    let bitcoin_config = event_observer_config.get_bitcoin_config();
    let mut traversals = HashMap::new();
    if is_predicate_evaluating_ordinals {
        let hord_db_conn = open_readonly_hord_db_conn(&config.expected_cache_path(), ctx)?;

        let mut storage = Storage::Memory(BTreeMap::new());
        for cursor in start_block..=end_block {
            // Only consider inscriptions in the interval specified
            let local_traverals = match inscriptions_cache.remove(&cursor) {
                Some(entry) => entry,
                None => continue
            };
            for (transaction_identifier, traversal_result) in local_traverals.into_iter() {
                traversals.insert(transaction_identifier, traversal_result);
            }

            blocks_scanned += 1;

            let block_hash = retrieve_block_hash_with_retry(&cursor, &bitcoin_config, ctx).await?;
            let block_breakdown =
                download_and_parse_block_with_retry(&block_hash, &bitcoin_config, ctx).await?;
            let mut block = match indexer::bitcoin::standardize_bitcoin_block(
                block_breakdown,
                &event_observer_config.bitcoin_network,
                ctx,
            ) {
                Ok(data) => data,
                Err(e) => {
                    warn!(
                        ctx.expect_logger(),
                        "Unable to standardize block#{} {}: {}", cursor, block_hash, e
                    );
                    continue;
                }
            };

            update_storage_and_augment_bitcoin_block_with_inscription_reveal_data(
                &mut block,
                &mut storage,
                &traversals,
                &hord_db_conn,
                &ctx,
            );

            let _ = update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
                &mut block,
                &mut storage,
                &ctx,
            );
            let chain_event =
                BitcoinChainEvent::ChainUpdatedWithBlocks(BitcoinChainUpdatedWithBlocksData {
                    new_blocks: vec![block],
                    confirmed_blocks: vec![],
                });

            let hits = evaluate_bitcoin_chainhooks_on_chain_event(
                &chain_event,
                vec![&predicate_spec],
                ctx,
            );
            info!(
                ctx.expect_logger(),
                "Processing block #{} through predicate {}: {} hits",
                cursor,
                predicate_spec.uuid,
                hits.len()
            );

            match execute_predicates_action(hits, &event_observer_config, &ctx).await {
                Ok(actions) => actions_triggered += actions,
                Err(_) => err_count += 1,
            }

            if err_count >= 3 {
                return Err(format!("Scan aborted (consecutive action errors >= 3)"));
            }
        }
    } else {
        let use_scan_to_seed_hord_db = true;

        if use_scan_to_seed_hord_db {
            // Start ingestion pipeline
        }

        for cursor in start_block..=end_block {
            blocks_scanned += 1;
            let block_hash = retrieve_block_hash_with_retry(&cursor, &bitcoin_config, ctx).await?;
            let block_breakdown =
                download_and_parse_block_with_retry(&block_hash, &bitcoin_config, ctx).await?;

            let block = match indexer::bitcoin::standardize_bitcoin_block(
                block_breakdown,
                &event_observer_config.bitcoin_network,
                ctx,
            ) {
                Ok(data) => data,
                Err(e) => {
                    warn!(
                        ctx.expect_logger(),
                        "Unable to standardize block#{} {}: {}", cursor, block_hash, e
                    );
                    continue;
                }
            };

            let chain_event =
                BitcoinChainEvent::ChainUpdatedWithBlocks(BitcoinChainUpdatedWithBlocksData {
                    new_blocks: vec![block],
                    confirmed_blocks: vec![],
                });

            let hits = evaluate_bitcoin_chainhooks_on_chain_event(
                &chain_event,
                vec![&predicate_spec],
                ctx,
            );

            match execute_predicates_action(hits, &event_observer_config, &ctx).await {
                Ok(actions) => actions_triggered += actions,
                Err(_) => err_count += 1,
            }

            if err_count >= 3 {
                return Err(format!("Scan aborted (consecutive action errors >= 3)"));
            }
        }
    }
    info!(
        ctx.expect_logger(),
        "{blocks_scanned} blocks scanned, {actions_triggered} actions triggered"
    );

    Ok(())
}

pub async fn execute_predicates_action<'a>(
    hits: Vec<BitcoinTriggerChainhook<'a>>,
    config: &EventObserverConfig,
    ctx: &Context,
) -> Result<u32, ()> {
    let mut actions_triggered = 0;
    let mut proofs = HashMap::new();
    for trigger in hits.into_iter() {
        if trigger.chainhook.include_proof {
            gather_proofs(&trigger, &mut proofs, &config, &ctx);
        }
        match handle_bitcoin_hook_action(trigger, &proofs) {
            Err(e) => {
                error!(ctx.expect_logger(), "unable to handle action {}", e);
            }
            Ok(action) => {
                actions_triggered += 1;
                match action {
                    BitcoinChainhookOccurrence::Http(request) => {
                        send_request(request, &ctx).await?
                    }
                    BitcoinChainhookOccurrence::File(path, bytes) => {
                        file_append(path, bytes, &ctx)?
                    }
                    BitcoinChainhookOccurrence::Data(_payload) => unreachable!(),
                };
            }
        }
    }

    Ok(actions_triggered)
}
