use crate::config::Config;
use crate::core::protocol::inscription_parsing::{
    get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
    parse_inscriptions_and_standardize_block,
};
use crate::core::protocol::inscription_sequencing::consolidate_block_with_pre_computed_ordinals_data;
use crate::db::{get_any_entry_in_ordinal_activities, open_readonly_ordhook_db_conn};
use crate::download::download_ordinals_dataset_if_required;
use crate::service::observers::{
    open_readwrite_observers_db_conn_or_panic, update_observer_progress,
};
use chainhook_sdk::bitcoincore_rpc::RpcApi;
use chainhook_sdk::bitcoincore_rpc::{Auth, Client};
use chainhook_sdk::chainhooks::bitcoin::{
    evaluate_bitcoin_chainhooks_on_chain_event, handle_bitcoin_hook_action,
    BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_sdk::chainhooks::types::BitcoinChainhookSpecification;
use chainhook_sdk::indexer::bitcoin::{
    build_http_client, download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_sdk::observer::{gather_proofs, DataHandlerEvent, EventObserverConfig};
use chainhook_sdk::types::{
    BitcoinBlockData, BitcoinChainEvent, BitcoinChainUpdatedWithBlocksData,
};
use chainhook_sdk::utils::{file_append, send_request, BlockHeights, Context};
use std::collections::HashMap;

pub async fn scan_bitcoin_chainstate_via_rpc_using_predicate(
    predicate_spec: &BitcoinChainhookSpecification,
    config: &Config,
    event_observer_config_override: Option<&EventObserverConfig>,
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
    let mut floating_end_block = false;

    let mut block_heights_to_scan = if let Some(ref blocks) = predicate_spec.blocks {
        BlockHeights::Blocks(blocks.clone()).get_sorted_entries()
    } else {
        let start_block = match predicate_spec.start_block {
            Some(start_block) => start_block,
            None => {
                return Err(
                    "Bitcoin chainhook specification must include a field start_block in replay mode"
                        .into(),
                );
            }
        };
        let (end_block, update_end_block) = match predicate_spec.end_block {
            Some(end_block) => (end_block, false),
            None => match bitcoin_rpc.get_blockchain_info() {
                Ok(result) => (result.blocks, true),
                Err(e) => {
                    return Err(format!(
                        "unable to retrieve Bitcoin chain tip ({})",
                        e.to_string()
                    ));
                }
            },
        };
        floating_end_block = update_end_block;
        BlockHeights::BlockRange(start_block, end_block).get_sorted_entries()
    };

    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on {} Bitcoin blocks",
        block_heights_to_scan.len()
    );
    let mut actions_triggered = 0;
    let mut err_count = 0;

    let event_observer_config = match event_observer_config_override {
        Some(config_override) => config_override.clone(),
        None => config.get_event_observer_config(),
    };
    let bitcoin_config = event_observer_config.get_bitcoin_config();
    let mut number_of_blocks_scanned = 0;
    let http_client = build_http_client();

    while let Some(current_block_height) = block_heights_to_scan.pop_front() {
        let mut inscriptions_db_conn =
            open_readonly_ordhook_db_conn(&config.expected_cache_path(), ctx)?;

        number_of_blocks_scanned += 1;

        if !get_any_entry_in_ordinal_activities(&current_block_height, &inscriptions_db_conn, &ctx)
        {
            continue;
        }

        let block_hash = retrieve_block_hash_with_retry(
            &http_client,
            &current_block_height,
            &bitcoin_config,
            ctx,
        )
        .await?;
        let block_breakdown =
            download_and_parse_block_with_retry(&http_client, &block_hash, &bitcoin_config, ctx)
                .await?;
        let mut block = match parse_inscriptions_and_standardize_block(
            block_breakdown,
            &event_observer_config.bitcoin_network,
            ctx,
        ) {
            Ok(data) => data,
            Err((e, _)) => {
                warn!(
                    ctx.expect_logger(),
                    "Unable to standardize block#{} {}: {}", current_block_height, block_hash, e
                );
                continue;
            }
        };

        {
            let inscriptions_db_tx = inscriptions_db_conn.transaction().unwrap();
            consolidate_block_with_pre_computed_ordinals_data(
                &mut block,
                &inscriptions_db_tx,
                true,
                &Context::empty(),
            );
        }

        let inscriptions_revealed = get_inscriptions_revealed_in_block(&block)
            .iter()
            .map(|d| d.get_inscription_number().to_string())
            .collect::<Vec<String>>();

        let inscriptions_transferred = get_inscriptions_transferred_in_block(&block).len();

        info!(
            ctx.expect_logger(),
            "Processing block #{current_block_height} through {} predicate revealed {} new inscriptions [{}] and {inscriptions_transferred} transfers",
            predicate_spec.uuid,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        );

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
        {
            let observers_db_conn =
                open_readwrite_observers_db_conn_or_panic(&config.expected_cache_path(), &ctx);
            update_observer_progress(
                &predicate_spec.uuid,
                current_block_height,
                &observers_db_conn,
                &ctx,
            )
        }
        if block_heights_to_scan.is_empty() && floating_end_block {
            match bitcoin_rpc.get_blockchain_info() {
                Ok(result) => {
                    for entry in (current_block_height + 1)..=result.blocks {
                        block_heights_to_scan.push_back(entry);
                    }
                }
                Err(_e) => {
                    continue;
                }
            };
        }
    }
    info!(
        ctx.expect_logger(),
        "{number_of_blocks_scanned} blocks scanned, {actions_triggered} actions triggered"
    );

    Ok(())
}

pub async fn process_block_with_predicates(
    block: BitcoinBlockData,
    predicates: &Vec<&BitcoinChainhookSpecification>,
    event_observer_config: &EventObserverConfig,
    ctx: &Context,
) -> Result<u32, String> {
    let chain_event =
        BitcoinChainEvent::ChainUpdatedWithBlocks(BitcoinChainUpdatedWithBlocksData {
            new_blocks: vec![block],
            confirmed_blocks: vec![],
        });

    let (predicates_triggered, _predicates_evaluated, _) =
        evaluate_bitcoin_chainhooks_on_chain_event(&chain_event, predicates, ctx);

    execute_predicates_action(predicates_triggered, &event_observer_config, &ctx).await
}

pub async fn execute_predicates_action<'a>(
    hits: Vec<BitcoinTriggerChainhook<'a>>,
    config: &EventObserverConfig,
    ctx: &Context,
) -> Result<u32, String> {
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
                    BitcoinChainhookOccurrence::Http(request, _data) => {
                        send_request(request, 60, 3, &ctx).await?
                    }
                    BitcoinChainhookOccurrence::File(path, bytes) => {
                        file_append(path, bytes, &ctx)?
                    }
                    BitcoinChainhookOccurrence::Data(payload) => {
                        if let Some(ref tx) = config.data_handler_tx {
                            let _ = tx.send(DataHandlerEvent::Process(payload));
                        }
                    }
                };
            }
        }
    }

    Ok(actions_triggered)
}
