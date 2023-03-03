use crate::config::Config;
use crate::node::ordinals::inscription_id::InscriptionId;
use chainhook_event_observer::bitcoincore_rpc::bitcoin::BlockHash;
use chainhook_event_observer::bitcoincore_rpc::jsonrpc;
use chainhook_event_observer::chainhooks::bitcoin::{
    handle_bitcoin_hook_action, BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_event_observer::chainhooks::types::{
    BitcoinPredicateType, ChainhookConfig, OrdinalOperations, Protocols,
};
use chainhook_event_observer::indexer::ordinals::indexing::entry::Entry;
use chainhook_event_observer::indexer::ordinals::indexing::{
    HEIGHT_TO_BLOCK_HASH, INSCRIPTION_NUMBER_TO_INSCRIPTION_ID,
};
use chainhook_event_observer::indexer::ordinals::{self, initialize_ordinal_index};
use chainhook_event_observer::indexer::{self, BitcoinChainContext};
use chainhook_event_observer::observer::{
    start_event_observer, EventObserverConfig, ObserverEvent,
};
use chainhook_event_observer::redb::ReadableTable;
use chainhook_event_observer::utils::{file_append, send_request, Context};
use chainhook_event_observer::{
    chainhooks::stacks::{
        evaluate_stacks_predicate_on_transaction, handle_stacks_hook_action,
        StacksChainhookOccurrence, StacksTriggerChainhook,
    },
    chainhooks::types::ChainhookSpecification,
};
use chainhook_types::{
    BlockIdentifier, StacksBlockData, StacksBlockMetadata, StacksChainEvent, StacksTransactionData,
};
use redis::{Commands, Connection};
use reqwest::Client as HttpClient;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::mpsc::channel;
use std::time::Duration;

pub const DEFAULT_INGESTION_PORT: u16 = 20455;
pub const DEFAULT_CONTROL_PORT: u16 = 20456;

pub struct Node {
    config: Config,
    ctx: Context,
}

impl Node {
    pub fn new(config: Config, ctx: Context) -> Self {
        Self { config, ctx }
    }

    pub async fn run(&mut self) -> Result<(), String> {
        let mut chainhook_config = ChainhookConfig::new();

        {
            let redis_config = self.config.expected_redis_config();
            let client = redis::Client::open(redis_config.uri.clone()).unwrap();
            let mut redis_con = match client.get_connection() {
                Ok(con) => con,
                Err(message) => {
                    error!(self.ctx.expect_logger(), "Redis: {}", message.to_string());
                    panic!();
                }
            };

            let chainhooks_to_load: Vec<String> = redis_con
                .scan_match("chainhook:*:*:*")
                .expect("unable to retrieve prunable entries")
                .into_iter()
                .collect();

            for key in chainhooks_to_load.iter() {
                let chainhook = match redis_con.hget::<_, _, String>(key, "specification") {
                    Ok(spec) => {
                        ChainhookSpecification::deserialize_specification(&spec, key).unwrap()
                        // todo
                    }
                    Err(e) => {
                        error!(
                            self.ctx.expect_logger(),
                            "unable to load chainhook associated with key {}: {}",
                            key,
                            e.to_string()
                        );
                        continue;
                    }
                };
                chainhook_config.register_hook(chainhook);
            }
        }

        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        // let (ordinal_indexer_command_tx, ordinal_indexer_command_rx) = channel();

        let event_observer_config = EventObserverConfig {
            normalization_enabled: true,
            grpc_server_enabled: false,
            hooks_enabled: true,
            bitcoin_rpc_proxy_enabled: true,
            event_handlers: vec![],
            chainhook_config: Some(chainhook_config),
            ingestion_port: DEFAULT_INGESTION_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            bitcoin_node_username: self.config.network.bitcoin_node_rpc_username.clone(),
            bitcoin_node_password: self.config.network.bitcoin_node_rpc_password.clone(),
            bitcoin_node_rpc_url: self.config.network.bitcoin_node_rpc_url.clone(),
            stacks_node_rpc_url: self.config.network.stacks_node_rpc_url.clone(),
            operators: HashSet::new(),
            display_logs: false,
            cache_path: self.config.storage.cache_path.clone(),
            bitcoin_network: self.config.network.bitcoin_network.clone(),
        };
        info!(
            self.ctx.expect_logger(),
            "Listening for new blockchain events on port {}", DEFAULT_INGESTION_PORT
        );
        info!(
            self.ctx.expect_logger(),
            "Listening for chainhook predicate registrations on port {}", DEFAULT_CONTROL_PORT
        );

        let ordinal_index = match initialize_ordinal_index(&event_observer_config, &self.ctx) {
            Ok(index) => index,
            Err(e) => {
                panic!()
            }
        };
        let mut bitcoin_context = BitcoinChainContext::new(Some(ordinal_index));

        let context_cloned = self.ctx.clone();
        let _ = std::thread::spawn(move || {
            let future = start_event_observer(
                event_observer_config,
                observer_command_tx,
                observer_command_rx,
                Some(observer_event_tx),
                context_cloned,
            );
            let _ = hiro_system_kit::nestable_block_on(future);
        });

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
            let redis_config = self.config.expected_redis_config();

            let client = redis::Client::open(redis_config.uri.clone()).unwrap();
            let mut redis_con = match client.get_connection() {
                Ok(con) => con,
                Err(message) => {
                    error!(self.ctx.expect_logger(), "Redis: {}", message.to_string());
                    panic!();
                }
            };
            match event {
                ObserverEvent::HookRegistered(chainhook) => {
                    // If start block specified, use it.
                    // I no start block specified, depending on the nature the hook, we'd like to retrieve:
                    // - contract-id

                    let chainhook_key = chainhook.key();
                    let history: Vec<u64> = vec![];
                    let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
                        &chainhook_key,
                        &[
                            ("specification", json!(chainhook).to_string()),
                            ("history", json!(history).to_string()),
                            ("scan_progress", json!(0).to_string()),
                        ],
                    );
                    if let Err(e) = res {
                        error!(
                            self.ctx.expect_logger(),
                            "unable to store chainhook {chainhook_key}: {}",
                            e.to_string()
                        );
                    }
                    match chainhook {
                        ChainhookSpecification::Stacks(stacks_hook) => {
                            // Retrieve highest block height stored
                            let tip_height: u64 = redis_con.get(&format!("stx:tip")).unwrap_or(1);

                            let start_block = stacks_hook.start_block.unwrap_or(1); // TODO(lgalabru): handle STX hooks and genesis block :s
                            let end_block = stacks_hook.end_block.unwrap_or(tip_height); // TODO(lgalabru): handle STX hooks and genesis block :s

                            info!(
                                self.ctx.expect_logger(),
                                "Processing Stacks chainhook {}, will scan blocks [{}; {}]",
                                stacks_hook.uuid,
                                start_block,
                                end_block
                            );
                            let mut total_hits = 0;
                            for cursor in start_block..=end_block {
                                debug!(
                                    self.ctx.expect_logger(),
                                    "Evaluating predicate #{} on block #{}",
                                    stacks_hook.uuid,
                                    cursor
                                );
                                let (
                                    block_identifier,
                                    parent_block_identifier,
                                    timestamp,
                                    transactions,
                                    metadata,
                                ) = {
                                    let payload: Vec<String> = redis_con
                                        .hget(
                                            &format!("stx:{}", cursor),
                                            &[
                                                "block_identifier",
                                                "parent_block_identifier",
                                                "timestamp",
                                                "transactions",
                                                "metadata",
                                            ],
                                        )
                                        .expect("unable to retrieve tip height");
                                    if payload.len() != 5 {
                                        warn!(self.ctx.expect_logger(), "Chain still being processed, please retry in a few minutes");
                                        continue;
                                    }
                                    (
                                        serde_json::from_str::<BlockIdentifier>(&payload[0])
                                            .unwrap(),
                                        serde_json::from_str::<BlockIdentifier>(&payload[1])
                                            .unwrap(),
                                        serde_json::from_str::<i64>(&payload[2]).unwrap(),
                                        serde_json::from_str::<Vec<StacksTransactionData>>(
                                            &payload[3],
                                        )
                                        .unwrap(),
                                        serde_json::from_str::<StacksBlockMetadata>(&payload[4])
                                            .unwrap(),
                                    )
                                };
                                let mut hits = vec![];
                                for tx in transactions.iter() {
                                    if evaluate_stacks_predicate_on_transaction(
                                        &tx,
                                        &stacks_hook,
                                        &self.ctx,
                                    ) {
                                        debug!(
                                            self.ctx.expect_logger(),
                                            "Action #{} triggered by transaction {} (block #{})",
                                            stacks_hook.uuid,
                                            tx.transaction_identifier.hash,
                                            cursor
                                        );
                                        hits.push(tx);
                                        total_hits += 1;
                                    }
                                }

                                if hits.len() > 0 {
                                    let block = StacksBlockData {
                                        block_identifier,
                                        parent_block_identifier,
                                        timestamp,
                                        transactions: vec![],
                                        metadata,
                                    };
                                    let trigger = StacksTriggerChainhook {
                                        chainhook: &stacks_hook,
                                        apply: vec![(hits, &block)],
                                        rollback: vec![],
                                    };

                                    let proofs = HashMap::new();
                                    match handle_stacks_hook_action(trigger, &proofs, &self.ctx) {
                                        Err(e) => {
                                            info!(
                                                self.ctx.expect_logger(),
                                                "unable to handle action {}", e
                                            );
                                        }
                                        Ok(StacksChainhookOccurrence::Http(request)) => {
                                            if let Err(e) =
                                                hiro_system_kit::nestable_block_on(request.send())
                                            {
                                                error!(
                                                    self.ctx.expect_logger(),
                                                    "unable to perform action {}", e
                                                );
                                            }
                                        }
                                        Ok(_) => {
                                            error!(
                                                self.ctx.expect_logger(),
                                                "action not supported"
                                            );
                                        }
                                    }
                                }
                            }
                            info!(self.ctx.expect_logger(), "Stacks chainhook {} scan completed: action triggered by {} transactions", stacks_hook.uuid, total_hits);
                        }
                        ChainhookSpecification::Bitcoin(predicate_spec) => {
                            let mut inscriptions_hints = BTreeMap::new();
                            let mut use_hinting = false;
                            if let BitcoinPredicateType::Protocol(Protocols::Ordinal(
                                OrdinalOperations::InscriptionRevealed,
                            )) = &predicate_spec.predicate
                            {
                                if let Some(ref ordinal_index) = bitcoin_context.ordinal_index {
                                    for (inscription_number, inscription_id) in ordinal_index
                                        .database
                                        .begin_read()
                                        .unwrap()
                                        .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)
                                        .unwrap()
                                        .iter()
                                        .unwrap()
                                    {
                                        let inscription =
                                            InscriptionId::load(*inscription_id.value());
                                        println!(
                                            "{} -> {}",
                                            inscription_number.value(),
                                            inscription
                                        );

                                        let entry = ordinal_index
                                            .get_inscription_entry(inscription)
                                            .unwrap()
                                            .unwrap();
                                        println!("{:?}", entry);

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
                                        use_hinting = true;
                                    }
                                }
                            }

                            let start_block = match predicate_spec.start_block {
                                Some(n) => n,
                                None => 0,
                            };

                            let end_block = match predicate_spec.end_block {
                                Some(n) => n,
                                None => {
                                    let body = json!({
                                        "jsonrpc": "1.0",
                                        "id": "chainhook-cli",
                                        "method": "getblockcount",
                                        "params": json!([])
                                    });
                                    let http_client = HttpClient::builder()
                                        .timeout(Duration::from_secs(20))
                                        .build()
                                        .expect("Unable to build http client");
                                    http_client
                                        .post(&self.config.network.bitcoin_node_rpc_url)
                                        .basic_auth(
                                            &self.config.network.bitcoin_node_rpc_username,
                                            Some(&self.config.network.bitcoin_node_rpc_password),
                                        )
                                        .header("Content-Type", "application/json")
                                        .header(
                                            "Host",
                                            &self.config.network.bitcoin_node_rpc_url[7..],
                                        )
                                        .json(&body)
                                        .send()
                                        .await
                                        .map_err(|e| format!("unable to send request ({})", e))?
                                        .json::<jsonrpc::Response>()
                                        .await
                                        .map_err(|e| format!("unable to parse response ({})", e))?
                                        .result::<u64>()
                                        .map_err(|e| format!("unable to parse response ({})", e))?
                                }
                            };

                            let mut total_hits = vec![];
                            for cursor in start_block..=end_block {
                                let block_hash = if use_hinting {
                                    match inscriptions_hints.remove(&cursor) {
                                        Some(block_hash) => block_hash.to_string(),
                                        None => continue,
                                    }
                                } else {
                                    let body = json!({
                                        "jsonrpc": "1.0",
                                        "id": "chainhook-cli",
                                        "method": "getblockhash",
                                        "params": [cursor]
                                    });
                                    let http_client = HttpClient::builder()
                                        .timeout(Duration::from_secs(20))
                                        .build()
                                        .expect("Unable to build http client");
                                    http_client
                                        .post(&self.config.network.bitcoin_node_rpc_url)
                                        .basic_auth(
                                            &self.config.network.bitcoin_node_rpc_username,
                                            Some(&self.config.network.bitcoin_node_rpc_password),
                                        )
                                        .header("Content-Type", "application/json")
                                        .header(
                                            "Host",
                                            &self.config.network.bitcoin_node_rpc_url[7..],
                                        )
                                        .json(&body)
                                        .send()
                                        .await
                                        .map_err(|e| format!("unable to send request ({})", e))?
                                        .json::<jsonrpc::Response>()
                                        .await
                                        .map_err(|e| format!("unable to parse response ({})", e))?
                                        .result::<String>()
                                        .map_err(|e| format!("unable to parse response ({})", e))?
                                };

                                let body = json!({
                                    "jsonrpc": "1.0",
                                    "id": "chainhook-cli",
                                    "method": "getblock",
                                    "params": [block_hash, 2]
                                });
                                let http_client = HttpClient::builder()
                                    .timeout(Duration::from_secs(20))
                                    .build()
                                    .expect("Unable to build http client");
                                let raw_block = http_client
                                    .post(&self.config.network.bitcoin_node_rpc_url)
                                    .basic_auth(
                                        &self.config.network.bitcoin_node_rpc_username,
                                        Some(&self.config.network.bitcoin_node_rpc_password),
                                    )
                                    .header("Content-Type", "application/json")
                                    .header("Host", &self.config.network.bitcoin_node_rpc_url[7..])
                                    .json(&body)
                                    .send()
                                    .await
                                    .map_err(|e| format!("unable to send request ({})", e))?
                                    .json::<jsonrpc::Response>()
                                    .await
                                    .map_err(|e| format!("unable to parse response ({})", e))?
                                    .result::<indexer::bitcoin::Block>()
                                    .map_err(|e| format!("unable to parse response ({})", e))?;

                                let block = indexer::bitcoin::standardize_bitcoin_block(
                                    &self.config.network,
                                    cursor,
                                    raw_block,
                                    &mut bitcoin_context,
                                    &self.ctx,
                                )?;

                                let mut hits = vec![];
                                for tx in block.transactions.iter() {
                                    if predicate_spec.predicate.evaluate_transaction_predicate(&tx)
                                    {
                                        info!(
                                            self.ctx.expect_logger(),
                                            "Action #{} triggered by transaction {} (block #{})",
                                            predicate_spec.uuid,
                                            tx.transaction_identifier.hash,
                                            cursor
                                        );
                                        hits.push(tx);
                                        total_hits.push(tx.transaction_identifier.hash.to_string());
                                    }
                                }

                                if hits.len() > 0 {
                                    let trigger = BitcoinTriggerChainhook {
                                        chainhook: &predicate_spec,
                                        apply: vec![(hits, &block)],
                                        rollback: vec![],
                                    };

                                    let proofs = HashMap::new();
                                    match handle_bitcoin_hook_action(trigger, &proofs) {
                                        Err(e) => {
                                            error!(
                                                self.ctx.expect_logger(),
                                                "unable to handle action {}", e
                                            );
                                        }
                                        Ok(BitcoinChainhookOccurrence::Http(request)) => {
                                            send_request(request, &self.ctx).await;
                                        }
                                        Ok(BitcoinChainhookOccurrence::File(path, bytes)) => {
                                            file_append(path, bytes, &self.ctx)
                                        }
                                        Ok(BitcoinChainhookOccurrence::Data(_payload)) => {
                                            unreachable!()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                ObserverEvent::HookDeregistered(chainhook) => {
                    let chainhook_key = chainhook.key();
                    let _: Result<(), redis::RedisError> = redis_con.del(chainhook_key);
                }
                ObserverEvent::BitcoinChainEvent(_chain_update) => {
                    debug!(self.ctx.expect_logger(), "Bitcoin update not stored");
                }
                ObserverEvent::StacksChainEvent(chain_event) => {
                    match &chain_event {
                        StacksChainEvent::ChainUpdatedWithBlocks(data) => {
                            update_storage_with_confirmed_stacks_blocks(
                                &mut redis_con,
                                &data.confirmed_blocks,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithReorg(data) => {
                            update_storage_with_confirmed_stacks_blocks(
                                &mut redis_con,
                                &data.confirmed_blocks,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithMicroblocks(_)
                        | StacksChainEvent::ChainUpdatedWithMicroblocksReorg(_) => {}
                    };
                }
                ObserverEvent::Terminate => {
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn update_storage_with_confirmed_stacks_blocks(
    redis_con: &mut Connection,
    blocks: &Vec<StacksBlockData>,
    ctx: &Context,
) {
    let current_tip_height: u64 = redis_con.get(&format!("stx:tip")).unwrap_or(0);

    let mut new_tip = None;

    for block in blocks.iter() {
        let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
            &format!("stx:{}", block.block_identifier.index),
            &[
                (
                    "block_identifier",
                    json!(block.block_identifier).to_string(),
                ),
                (
                    "parent_block_identifier",
                    json!(block.parent_block_identifier).to_string(),
                ),
                ("transactions", json!(block.transactions).to_string()),
                ("metadata", json!(block.metadata).to_string()),
            ],
        );
        if let Err(error) = res {
            crit!(
                ctx.expect_logger(),
                "unable to archive block {}: {}",
                block.block_identifier,
                error.to_string()
            );
        }
        if block.block_identifier.index >= current_tip_height {
            new_tip = Some(block);
        }
    }

    if let Some(block) = new_tip {
        info!(
            ctx.expect_logger(),
            "Archiving confirmed Stacks chain block {}", block.block_identifier
        );
        let _: Result<(), redis::RedisError> =
            redis_con.set(&format!("stx:tip"), block.block_identifier.index);
    }
}

// pub fn start_node(mut config: Config, ctx: Context) {
//     let (digestion_tx, digestion_rx) = channel();
//     let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
//     let (observer_command_tx, observer_command_rx) = channel();

//     let terminate_digestion_tx = digestion_tx.clone();
//     let context_cloned = ctx.clone();
//     ctrlc::set_handler(move || {
//         warn!(
//             &context_cloned.expect_logger(),
//             "Manual interruption signal received"
//         );
//         terminate_digestion_tx
//             .send(DigestingCommand::Kill)
//             .expect("Unable to terminate service");
//     })
//     .expect("Error setting Ctrl-C handler");

//     if config.is_initial_ingestion_required() {
//         // Download default tsv.
//         if config.rely_on_remote_tsv() && config.should_download_remote_tsv() {
//             let url = config.expected_remote_tsv_url();
//             let mut destination_path = config.expected_cache_path();
//             destination_path.push("stacks-node-events.tsv");
//             // Download archive if not already present in cache
//             if !destination_path.exists() {
//                 info!(ctx.expect_logger(), "Downloading {}", url);
//                 match hiro_system_kit::nestable_block_on(archive::download_tsv_file(&config)) {
//                     Ok(_) => {}
//                     Err(e) => {
//                         error!(ctx.expect_logger(), "{}", e);
//                         process::exit(1);
//                     }
//                 }
//                 let mut destination_path = config.expected_cache_path();
//                 destination_path.push("stacks-node-events.tsv");
//             }
//             config.add_local_tsv_source(&destination_path);

//             let ingestion_config = config.clone();
//             let seed_digestion_tx = digestion_tx.clone();
//             let context_cloned = ctx.clone();

//             thread::spawn(move || {
//                 let res = block::ingestion::start(
//                     seed_digestion_tx.clone(),
//                     &ingestion_config,
//                     context_cloned.clone(),
//                 );
//                 let (_stacks_chain_tip, _bitcoin_chain_tip) = match res {
//                     Ok(chain_tips) => chain_tips,
//                     Err(e) => {
//                         error!(&context_cloned.expect_logger(), "{}", e);
//                         process::exit(1);
//                     }
//                 };
//             });
//         }
//     } else {
//         info!(
//             ctx.expect_logger(),
//             "Streaming blocks from stacks-node {}",
//             config.expected_stacks_node_event_source()
//         );
//     }

//     let digestion_config = config.clone();
//     let terminate_observer_command_tx = observer_command_tx.clone();
//     let context_cloned = ctx.clone();

//     thread::spawn(move || {
//         let res = block::digestion::start(digestion_rx, &digestion_config, &context_cloned);
//         if let Err(e) = res {
//             error!(&context_cloned.expect_logger(), "{}", e);
//         }
//         let _ = terminate_observer_command_tx.send(ObserverCommand::Terminate);
//     });

//     let mut chainhook_config = ChainhookConfig::new();

//     {
//         let redis_config = config.expected_redis_config();
//         let client = redis::Client::open(redis_config.uri.clone()).unwrap();
//         let mut redis_con = match client.get_connection() {
//             Ok(con) => con,
//             Err(message) => {
//                 error!(ctx.expect_logger(), "Redis: {}", message.to_string());
//                 panic!();
//             }
//         };

//         let chainhooks_to_load: Vec<String> = redis_con
//             .scan_match("chainhook:*:*:*")
//             .expect("unable to retrieve prunable entries")
//             .into_iter()
//             .collect();

//         for key in chainhooks_to_load.iter() {
//             let chainhook = match redis_con.hget::<_, _, String>(key, "specification") {
//                 Ok(spec) => {
//                     ChainhookSpecification::deserialize_specification(&spec, key).unwrap()
//                     // todo
//                 }
//                 Err(e) => {
//                     error!(
//                         ctx.expect_logger(),
//                         "unable to load chainhook associated with key {}: {}",
//                         key,
//                         e.to_string()
//                     );
//                     continue;
//                 }
//             };
//             chainhook_config.register_hook(chainhook);
//         }
//     }

//     let event_observer_config = EventObserverConfig {
//         normalization_enabled: true,
//         grpc_server_enabled: false,
//         hooks_enabled: true,
//         bitcoin_rpc_proxy_enabled: true,
//         event_handlers: vec![],
//         chainhook_config: Some(chainhook_config),
//         ingestion_port: DEFAULT_INGESTION_PORT,
//         control_port: DEFAULT_CONTROL_PORT,
//         bitcoin_node_username: config.network.bitcoin_node_rpc_username.clone(),
//         bitcoin_node_password: config.network.bitcoin_node_rpc_password.clone(),
//         bitcoin_node_rpc_url: config.network.bitcoin_node_rpc_url.clone(),
//         stacks_node_rpc_url: config.network.stacks_node_rpc_url.clone(),
//         operators: HashSet::new(),
//         display_logs: false,
//     };
//     info!(
//         ctx.expect_logger(),
//         "Listening for new blockchain events on port {}", DEFAULT_INGESTION_PORT
//     );
//     info!(
//         ctx.expect_logger(),
//         "Listening for chainhook predicate registrations on port {}", DEFAULT_CONTROL_PORT
//     );
//     let context_cloned = ctx.clone();
//     let _ = std::thread::spawn(move || {
//         let future = start_event_observer(
//             event_observer_config,
//             observer_command_tx,
//             observer_command_rx,
//             Some(observer_event_tx),
//             context_cloned,
//         );
//         let _ = hiro_system_kit::nestable_block_on(future);
//     });

//     loop {
//         let event = match observer_event_rx.recv() {
//             Ok(cmd) => cmd,
//             Err(e) => {
//                 error!(
//                     ctx.expect_logger(),
//                     "Error: broken channel {}",
//                     e.to_string()
//                 );
//                 break;
//             }
//         };
//         let redis_config = config.expected_redis_config();
//         let client = redis::Client::open(redis_config.uri.clone()).unwrap();
//         let mut redis_con = match client.get_connection() {
//             Ok(con) => con,
//             Err(message) => {
//                 error!(ctx.expect_logger(), "Redis: {}", message.to_string());
//                 panic!();
//             }
//         };
//         match event {
//             ObserverEvent::HookRegistered(chainhook) => {
//                 // If start block specified, use it.
//                 // I no start block specified, depending on the nature the hook, we'd like to retrieve:
//                 // - contract-id

//                 let chainhook_key = chainhook.key();
//                 let history: Vec<u64> = vec![];
//                 let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
//                     &chainhook_key,
//                     &[
//                         ("specification", json!(chainhook).to_string()),
//                         ("history", json!(history).to_string()),
//                         ("scan_progress", json!(0).to_string()),
//                     ],
//                 );
//                 if let Err(e) = res {
//                     error!(
//                         ctx.expect_logger(),
//                         "unable to store chainhook {chainhook_key}: {}",
//                         e.to_string()
//                     );
//                 }
//                 match chainhook {
//                     ChainhookSpecification::Stacks(stacks_hook) => {
//                         // Retrieve highest block height stored
//                         let tip_height: u64 = redis_con.get(&format!("stx:tip")).unwrap_or(1);

//                         let start_block = stacks_hook.start_block.unwrap_or(1); // TODO(lgalabru): handle STX hooks and genesis block :s
//                         let end_block = stacks_hook.end_block.unwrap_or(tip_height); // TODO(lgalabru): handle STX hooks and genesis block :s

//                         info!(
//                             ctx.expect_logger(),
//                             "Processing Stacks chainhook {}, will scan blocks [{}; {}]",
//                             stacks_hook.uuid,
//                             start_block,
//                             end_block
//                         );
//                         let mut total_hits = 0;
//                         for cursor in start_block..=end_block {
//                             debug!(
//                                 ctx.expect_logger(),
//                                 "Evaluating predicate #{} on block #{}", stacks_hook.uuid, cursor
//                             );
//                             let (
//                                 block_identifier,
//                                 parent_block_identifier,
//                                 timestamp,
//                                 transactions,
//                                 metadata,
//                             ) = {
//                                 let payload: Vec<String> = redis_con
//                                     .hget(
//                                         &format!("stx:{}", cursor),
//                                         &[
//                                             "block_identifier",
//                                             "parent_block_identifier",
//                                             "timestamp",
//                                             "transactions",
//                                             "metadata",
//                                         ],
//                                     )
//                                     .expect("unable to retrieve tip height");
//                                 if payload.len() != 5 {
//                                     warn!(ctx.expect_logger(), "Chain still being processed, please retry in a few minutes");
//                                     continue;
//                                 }
//                                 (
//                                     serde_json::from_str::<BlockIdentifier>(&payload[0]).unwrap(),
//                                     serde_json::from_str::<BlockIdentifier>(&payload[1]).unwrap(),
//                                     serde_json::from_str::<i64>(&payload[2]).unwrap(),
//                                     serde_json::from_str::<Vec<StacksTransactionData>>(&payload[3])
//                                         .unwrap(),
//                                     serde_json::from_str::<StacksBlockMetadata>(&payload[4])
//                                         .unwrap(),
//                                 )
//                             };
//                             let mut hits = vec![];
//                             for tx in transactions.iter() {
//                                 if evaluate_stacks_predicate_on_transaction(&tx, &stacks_hook, &ctx)
//                                 {
//                                     debug!(
//                                         ctx.expect_logger(),
//                                         "Action #{} triggered by transaction {} (block #{})",
//                                         stacks_hook.uuid,
//                                         tx.transaction_identifier.hash,
//                                         cursor
//                                     );
//                                     hits.push(tx);
//                                     total_hits += 1;
//                                 }
//                             }

//                             if hits.len() > 0 {
//                                 let block = StacksBlockData {
//                                     block_identifier,
//                                     parent_block_identifier,
//                                     timestamp,
//                                     transactions: vec![],
//                                     metadata,
//                                 };
//                                 let trigger = StacksTriggerChainhook {
//                                     chainhook: &stacks_hook,
//                                     apply: vec![(hits, &block)],
//                                     rollback: vec![],
//                                 };

//                                 let proofs = HashMap::new();
//                                 match handle_stacks_hook_action(trigger, &proofs, &ctx) {
//                                     Err(e) => {
//                                         info!(ctx.expect_logger(), "unable to handle action {}", e);
//                                     }
//                                     Ok(StacksChainhookOccurrence::Http(request)) => {
//                                         if let Err(e) =
//                                             hiro_system_kit::nestable_block_on(request.send())
//                                         {
//                                             error!(
//                                                 ctx.expect_logger(),
//                                                 "unable to perform action {}", e
//                                             );
//                                         }
//                                     }
//                                     Ok(_) => {
//                                         error!(ctx.expect_logger(), "action not supported");
//                                     }
//                                 }
//                             }
//                         }
//                         info!(ctx.expect_logger(), "Stacks chainhook {} scan completed: action triggered by {} transactions", stacks_hook.uuid, total_hits);
//                     }
//                     ChainhookSpecification::Bitcoin(_bitcoin_hook) => {
//                         warn!(
//                             ctx.expect_logger(),
//                             "Bitcoin chainhook evaluation unavailable for historical data"
//                         );
//                     }
//                 }
//             }
//             ObserverEvent::HookDeregistered(chainhook) => {
//                 let chainhook_key = chainhook.key();
//                 let _: Result<(), redis::RedisError> = redis_con.del(chainhook_key);
//             }
//             ObserverEvent::BitcoinChainEvent(_chain_update) => {
//                 debug!(ctx.expect_logger(), "Bitcoin update not stored");
//             }
//             ObserverEvent::StacksChainEvent(chain_event) => {
//                 match &chain_event {
//                     StacksChainEvent::ChainUpdatedWithBlocks(data) => {
//                         update_storage_with_confirmed_stacks_blocks(
//                             &mut redis_con,
//                             &data.confirmed_blocks,
//                             &ctx,
//                         );
//                     }
//                     StacksChainEvent::ChainUpdatedWithReorg(data) => {
//                         update_storage_with_confirmed_stacks_blocks(
//                             &mut redis_con,
//                             &data.confirmed_blocks,
//                             &ctx,
//                         );
//                     }
//                     StacksChainEvent::ChainUpdatedWithMicroblocks(_)
//                     | StacksChainEvent::ChainUpdatedWithMicroblocksReorg(_) => {}
//                 };
//             }
//             ObserverEvent::Terminate => {
//                 break;
//             }
//             _ => {}
//         }
//     }
// }
