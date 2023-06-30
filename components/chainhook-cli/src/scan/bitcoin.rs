use crate::archive::download_ordinals_dataset_if_required;
use crate::config::{Config, PredicatesApi};
use crate::service::{
    open_readwrite_predicates_db_conn_or_panic, update_predicate_status, PredicateStatus,
    ScanningData,
};
use chainhook_sdk::bitcoincore_rpc::RpcApi;
use chainhook_sdk::bitcoincore_rpc::{Auth, Client};
use chainhook_sdk::chainhooks::bitcoin::{
    evaluate_bitcoin_chainhooks_on_chain_event, handle_bitcoin_hook_action,
    BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_sdk::chainhooks::types::{BitcoinChainhookSpecification, BitcoinPredicateType};
use chainhook_sdk::hord::db::{
    find_all_inscriptions_in_block, get_any_entry_in_ordinal_activities, open_readonly_hord_db_conn,
};
use chainhook_sdk::hord::{
    get_inscriptions_revealed_in_block,
    update_storage_and_augment_bitcoin_block_with_inscription_reveal_data,
    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data, Storage,
};
use chainhook_sdk::indexer;
use chainhook_sdk::indexer::bitcoin::{
    download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_sdk::observer::{gather_proofs, EventObserverConfig};
use chainhook_sdk::utils::{file_append, send_request, Context};
use chainhook_types::{BitcoinBlockData, BitcoinChainEvent, BitcoinChainUpdatedWithBlocksData};
use std::collections::{BTreeMap, HashMap};

pub async fn scan_bitcoin_chainstate_via_rpc_using_predicate(
    predicate_spec: &BitcoinChainhookSpecification,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let _ = download_ordinals_dataset_if_required(config, ctx).await;

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

    let (mut end_block, floating_end_block) = match predicate_spec.end_block {
        Some(end_block) => (end_block, false),
        None => match bitcoin_rpc.get_blockchain_info() {
            Ok(result) => (result.blocks - 1, true),
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
    let mut inscriptions_db_conn = None;

    if let BitcoinPredicateType::OrdinalsProtocol(_) = &predicate_spec.predicate {
        inscriptions_db_conn = Some(open_readonly_hord_db_conn(
            &config.expected_cache_path(),
            ctx,
        )?);
    }

    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on Bitcoin blocks",
    );

    let mut blocks_scanned = 0;
    let mut actions_triggered = 0;
    let mut occurrences_found = 0u64;
    let mut err_count = 0;

    let event_observer_config = config.get_event_observer_config();
    let bitcoin_config = event_observer_config.get_bitcoin_config();
    let mut traversals = HashMap::new();

    let mut storage = Storage::Memory(BTreeMap::new());
    let mut cursor = start_block.saturating_sub(1);

    while cursor <= end_block {
        cursor += 1;
        blocks_scanned += 1;

        if let Some(ref inscriptions_db_conn) = inscriptions_db_conn {
            if !get_any_entry_in_ordinal_activities(&cursor, &inscriptions_db_conn, &ctx) {
                continue;
            }
        }

        let block_hash = retrieve_block_hash_with_retry(&cursor, &bitcoin_config, ctx).await?;
        let block_breakdown =
            download_and_parse_block_with_retry(&block_hash, &bitcoin_config, ctx).await?;
        let mut block = match indexer::bitcoin::standardize_bitcoin_block(
            block_breakdown,
            &event_observer_config.bitcoin_network,
            ctx,
        ) {
            Ok(data) => data,
            Err((e, _)) => {
                warn!(
                    ctx.expect_logger(),
                    "Unable to standardize block#{} {}: {}", cursor, block_hash, e
                );
                continue;
            }
        };

        if let Some(ref inscriptions_db_conn) = inscriptions_db_conn {
            // Evaluating every single block is required for also keeping track of transfers.
            let local_traverals =
                match find_all_inscriptions_in_block(&cursor, &inscriptions_db_conn, &ctx)
                    .remove(&cursor)
                {
                    Some(entry) => entry,
                    None => vec![],
                };
            for (transaction_identifier, traversal_result) in local_traverals.into_iter() {
                traversals.insert(
                    (
                        transaction_identifier,
                        traversal_result.inscription_input_index,
                    ),
                    traversal_result,
                );
            }

            let _ = update_storage_and_augment_bitcoin_block_with_inscription_reveal_data(
                &mut block,
                &mut storage,
                &traversals,
                &inscriptions_db_conn,
                &ctx,
            )?;

            let _ = update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
                &mut block,
                &mut storage,
                &ctx,
            )?;

            let inscriptions_revealed = get_inscriptions_revealed_in_block(&block)
                .iter()
                .map(|d| d.inscription_number.to_string())
                .collect::<Vec<String>>();

            info!(
                ctx.expect_logger(),
                "Processing block #{} through {} predicate (inscriptions revealed: [{}])",
                cursor,
                predicate_spec.uuid,
                inscriptions_revealed.join(", ")
            );
        }

        match process_block_with_predicates(
            block,
            &vec![&predicate_spec],
            &event_observer_config,
            ctx,
        )
        .await
        {
            Ok(actions) => actions_triggered += actions,
            Err(_) => err_count += 1,
        }

        if err_count >= 3 {
            return Err(format!("Scan aborted (consecutive action errors >= 3)"));
        }

        if let PredicatesApi::On(ref api_config) = config.http_api {
            if blocks_scanned % 50 == 0 {
                let status = PredicateStatus::Scanning(ScanningData {
                    start_block,
                    end_block,
                    cursor,
                    occurrences_found,
                });
                let mut predicates_db_conn =
                    open_readwrite_predicates_db_conn_or_panic(api_config, &ctx);
                update_predicate_status(
                    &predicate_spec.key(),
                    status,
                    &mut predicates_db_conn,
                    &ctx,
                )
            }
        }

        if cursor == end_block && floating_end_block {
            end_block = match bitcoin_rpc.get_blockchain_info() {
                Ok(result) => result.blocks - 1,
                Err(_e) => {
                    continue;
                }
            };
        }
    }
    info!(
        ctx.expect_logger(),
        "{blocks_scanned} blocks scanned, {actions_triggered} actions triggered"
    );

    if let PredicatesApi::On(ref api_config) = config.http_api {
        let status = PredicateStatus::Scanning(ScanningData {
            start_block,
            end_block,
            cursor,
            occurrences_found,
        });
        let mut predicates_db_conn = open_readwrite_predicates_db_conn_or_panic(api_config, &ctx);
        update_predicate_status(&predicate_spec.key(), status, &mut predicates_db_conn, &ctx)
    }

    Ok(())
}

pub async fn process_block_with_predicates(
    block: BitcoinBlockData,
    predicates: &Vec<&BitcoinChainhookSpecification>,
    event_observer_config: &EventObserverConfig,
    ctx: &Context,
) -> Result<u32, ()> {
    let chain_event =
        BitcoinChainEvent::ChainUpdatedWithBlocks(BitcoinChainUpdatedWithBlocksData {
            new_blocks: vec![block],
            confirmed_blocks: vec![],
        });

    let (predicates_triggered, _predicates_evaluated) =
        evaluate_bitcoin_chainhooks_on_chain_event(&chain_event, predicates, ctx);

    execute_predicates_action(predicates_triggered, &event_observer_config, &ctx).await
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
                        send_request(request, 3, 1, &ctx).await?
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
