use super::block;
use crate::archive;
use crate::block::DigestingCommand;
use crate::config::Config;

use chainhook_event_observer::chainhooks::bitcoin::{
    handle_bitcoin_hook_action, BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_event_observer::chainhooks::types::ChainhookConfig;
use chainhook_event_observer::indexer::bitcoin::standardize_bitcoin_block;
use chainhook_event_observer::observer::{
    start_event_observer, EventObserverConfig, ObserverCommand, ObserverEvent,
};
use chainhook_event_observer::utils::Context;
use chainhook_event_observer::{
    chainhooks::stacks::{
        evaluate_stacks_transaction_predicate_on_transaction, handle_stacks_hook_action,
        StacksChainhookOccurrence, StacksTriggerChainhook,
    },
    chainhooks::types::ChainhookSpecification,
};

use bitcoincore_rpc::{Auth, Client, RpcApi};
use chainhook_types::{
    BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BitcoinTransactionData,
    BlockIdentifier, StacksBlockData, StacksBlockMetadata, StacksChainEvent, StacksNetwork,
    StacksTransactionData,
};
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
use redis::{Commands, Connection};
use reqwest::Url;
use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::{collections::HashMap, process, sync::mpsc::channel, thread};

pub const DEFAULT_INGESTION_PORT: u16 = 20455;
pub const DEFAULT_CONTROL_PORT: u16 = 20456;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum Command {
    /// Manage predicates
    #[clap(subcommand)]
    Predicates(PredicatesCommand),
    /// Start chainhook-cli
    #[clap(subcommand)]
    Node(NodeCommand),
    /// Start chainhook-cli in replay mode
    #[clap(name = "replay", bin_name = "replay")]
    Replay(ReplayCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
#[clap(bin_name = "predicate", aliases = &["predicate"])]
enum PredicatesCommand {
    /// Generate new predicate
    #[clap(name = "new", bin_name = "new", aliases = &["generate"])]
    New(NewPredicate),
    /// Scan blocks (one-off) from specified network and apply provided predicate
    #[clap(name = "scan", bin_name = "scan")]
    Scan(ScanPredicate),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct NewPredicate {
    /// Predicate's name
    pub name: String,
    /// Path to Clarinet.toml
    #[clap(long = "manifest-path")]
    pub manifest_path: Option<String>,
    /// Generate a Bitcoin chainhook
    #[clap(long = "bitcoin", conflicts_with = "stacks")]
    pub bitcoin: bool,
    /// Generate a Stacks chainhook
    #[clap(long = "stacks", conflicts_with = "bitcoin")]
    pub stacks: bool,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanPredicate {
    pub devnet: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "devnet",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub config_path: Option<String>,
    /// Load chainhook file path (yaml format)
    #[clap(long = "predicate-path", short = 'p')]
    pub chainhook_spec_path: String,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum NodeCommand {
    /// Start chainhook-cli
    #[clap(name = "start", bin_name = "start")]
    Start(StartCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct StartCommand {
    /// Target Devnet network
    #[clap(
        long = "devnet",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub devnet: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "devnet",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ReplayCommand {
    pub devnet: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "devnet",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "devnet"
    )]
    pub config_path: Option<String>,
    /// Apply chainhook action (false by default)
    #[clap(long = "apply-trigger")]
    pub apply_trigger: bool,
    /// Bitcoind node url override
    #[clap(long = "bitcoind-rpc-url")]
    pub bitcoind_rpc_url: Option<String>,
}

pub fn main() {
    let logger = hiro_system_kit::log::setup_logger();
    let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
    let ctx = Context {
        logger: Some(logger),
        tracer: false,
    };

    let opts: Opts = match Opts::try_parse() {
        Ok(opts) => opts,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    match opts.command {
        Command::Node(subcmd) => match subcmd {
            NodeCommand::Start(cmd) => {
                let config =
                    match Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path) {
                        Ok(config) => config,
                        Err(e) => {
                            println!("{e}");
                            process::exit(1);
                        }
                    };
                start_node(config, ctx);
            }
        },
        Command::Predicates(subcmd) => match subcmd {
            PredicatesCommand::New(cmd) => {
                // Predicates can either be generated manually by letting developers
                // craft their own json payload, or using the interactive approach.
                // A list of contracts is displayed, then list of methods, then list of events detected
                // 3 files are generated:
                // predicates/simnet/name.json
                // predicates/devnet/name.json
                // predicates/testnet/name.json
                // predicates/mainnet/name.json
                let manifest = clarinet_files::get_manifest_location(None);
            }
            PredicatesCommand::Scan(cmd) => {
                let config =
                    match Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path) {
                        Ok(config) => config,
                        Err(e) => {
                            println!("{e}");
                            process::exit(1);
                        }
                    };
                start_scan(config, ctx);
            }
        },
        Command::Replay(cmd) => {
            let mut config =
                match Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path) {
                    Ok(config) => config,
                    Err(e) => {
                        println!("{e}");
                        process::exit(1);
                    }
                };
            if let Some(bitcoind_rpc_url) = cmd.bitcoind_rpc_url {
                let url = match Url::parse(&bitcoind_rpc_url) {
                    Ok(url) => url,
                    Err(e) => {
                        println!("{e}");
                        process::exit(1);
                    }
                };
                let host = url
                    .host()
                    .expect("unable to retrieve host from bitcoind_rpc_url")
                    .to_string();
                let port = url
                    .port()
                    .expect("unable to retrieve port from bitcoind_rpc_url")
                    .to_string();
                let username = url.username().to_string();
                let password = url
                    .password()
                    .expect("unable to retrieve password from bitcoind_rpc_url")
                    .to_string();
                config.network.bitcoin_node_rpc_url = format!("http://{}:{}", host, port);
                config.network.bitcoin_node_rpc_username = username;
                config.network.bitcoin_node_rpc_password = password;
            }
            start_replay(config, cmd.apply_trigger, ctx);
        }
    }
}

pub fn install_ctrlc_handler(terminate_tx: Sender<DigestingCommand>, ctx: Context) {
    ctrlc::set_handler(move || {
        warn!(&ctx.expect_logger(), "Manual interruption signal received");
        terminate_tx
            .send(DigestingCommand::Kill)
            .expect("Unable to terminate service");
    })
    .expect("Error setting Ctrl-C handler");
}

pub fn download_dataset_if_required(config: &mut Config, ctx: &Context) -> bool {
    if config.is_initial_ingestion_required() {
        // Download default tsv.
        if config.rely_on_remote_tsv() && config.should_download_remote_tsv() {
            let url = config.expected_remote_tsv_url();
            let mut destination_path = config.expected_cache_path();
            destination_path.push(archive::default_tsv_file_path(
                &config.network.stacks_network,
            ));
            // Download archive if not already present in cache
            if !destination_path.exists() {
                info!(ctx.expect_logger(), "Downloading {}", url);
                match hiro_system_kit::nestable_block_on(archive::download_tsv_file(&config)) {
                    Ok(_) => {}
                    Err(e) => {
                        error!(ctx.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                }
            }
            config.add_local_tsv_source(&destination_path);
        }
        true
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from stacks-node {}",
            config.expected_stacks_node_event_source()
        );
        false
    }
}

pub fn start_scan(mut config: Config, ctx: Context) {
    let (digestion_tx, digestion_rx) = channel();
    install_ctrlc_handler(digestion_tx.clone(), ctx.clone());
    let data_downloaded = download_dataset_if_required(&mut config, &ctx);
    if !data_downloaded {
        error!(ctx.expect_logger(), "No dataset to scan");
        process::exit(1);
    }

    info!(ctx.expect_logger(), "Scanning...");
}

pub fn start_replay(mut config: Config, apply: bool, ctx: Context) {
    let indexer_config = config.network.clone();

    let (digestion_tx, digestion_rx) = channel();
    let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
    let (observer_command_tx, observer_command_rx) = channel();

    let terminate_digestion_tx = digestion_tx.clone();
    let context_cloned = ctx.clone();
    ctrlc::set_handler(move || {
        warn!(
            &context_cloned.expect_logger(),
            "Manual interruption signal received"
        );
        terminate_digestion_tx
            .send(DigestingCommand::Kill)
            .expect("Unable to terminate service");
    })
    .expect("Error setting Ctrl-C handler");

    if config.is_initial_ingestion_required() {
        // Download default tsv.
        if config.rely_on_remote_tsv() && config.should_download_remote_tsv() {
            let url = config.expected_remote_tsv_url();
            let mut destination_path = config.expected_cache_path();
            destination_path.push("stacks-node-events.tsv");
            // Download archive if not already present in cache
            if !destination_path.exists() {
                info!(ctx.expect_logger(), "Downloading {}", url);
                match hiro_system_kit::nestable_block_on(archive::download_tsv_file(&config)) {
                    Ok(_) => {}
                    Err(e) => {
                        error!(ctx.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                }
                let mut destination_path = config.expected_cache_path();
                destination_path.push("stacks-node-events.tsv");
            }
            config.add_local_tsv_source(&destination_path);

            let ingestion_config = config.clone();
            let seed_digestion_tx = digestion_tx.clone();
            let context_cloned = ctx.clone();

            thread::spawn(move || {
                let res = block::ingestion::start(
                    seed_digestion_tx.clone(),
                    &ingestion_config,
                    context_cloned.clone(),
                );
                let (_stacks_chain_tip, _bitcoin_chain_tip) = match res {
                    Ok(chain_tips) => chain_tips,
                    Err(e) => {
                        error!(&context_cloned.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                };
            });
        }
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from stacks-node {}",
            config.expected_stacks_node_event_source()
        );
    }

    let digestion_config = config.clone();
    let terminate_observer_command_tx = observer_command_tx.clone();
    let context_cloned = ctx.clone();

    thread::spawn(move || {
        let res = block::digestion::start(digestion_rx, &digestion_config, &context_cloned);
        if let Err(e) = res {
            crit!(&context_cloned.expect_logger(), "{}", e);
        }
        let _ = terminate_observer_command_tx.send(ObserverCommand::Terminate);
    });

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
    };
    info!(
        ctx.expect_logger(),
        "Listening for new blockchain events on port {}", DEFAULT_INGESTION_PORT
    );
    info!(
        ctx.expect_logger(),
        "Listening for chainhook predicate registrations on port {}", DEFAULT_CONTROL_PORT
    );
    let context_cloned = ctx.clone();

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

    let redis_config = config.expected_redis_config();
    let client = redis::Client::open(redis_config.uri.clone()).unwrap();
    let mut redis_con = match client.get_connection() {
        Ok(con) => con,
        Err(message) => {
            crit!(ctx.expect_logger(), "Redis: {}", message.to_string());
            panic!();
        }
    };

    let auth = Auth::UserPass(
        config.network.bitcoin_node_rpc_username.clone(),
        config.network.bitcoin_node_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoin_node_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            crit!(ctx.expect_logger(), "Bitcoin RPC: {}", message.to_string());
            panic!();
        }
    };

    loop {
        let event = match observer_event_rx.recv() {
            Ok(cmd) => cmd,
            Err(e) => {
                crit!(
                    ctx.expect_logger(),
                    "Error: broken channel {}",
                    e.to_string()
                );
                break;
            }
        };
        match event {
            ObserverEvent::HookRegistered(chainhook) => {
                // If start block specified, use it.
                // I no start block specified, depending on the nature the hook, we'd like to retrieve:
                // - contract-id

                match chainhook {
                    ChainhookSpecification::Stacks(stacks_hook) => {
                        // Retrieve highest block height stored
                        let tip_height: u64 = redis_con
                            .get(&format!("stx:tip"))
                            .expect("unable to retrieve tip height");

                        let start_block = stacks_hook.start_block.unwrap_or(2); // TODO(lgalabru): handle STX hooks and genesis block :s
                        let end_block = stacks_hook.end_block.unwrap_or(tip_height); // TODO(lgalabru): handle STX hooks and genesis block :s

                        info!(
                            ctx.expect_logger(), "Processing Stacks chainhook {}, will scan blocks [{}; {}]  (apply = {})",
                            stacks_hook.uuid, start_block, end_block, apply
                        );
                        let mut total_hits = vec![];
                        for cursor in start_block..=end_block {
                            debug!(
                                ctx.expect_logger(),
                                "Evaluating predicate #{} on block #{}", stacks_hook.uuid, cursor
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
                                    warn!(
                                        ctx.expect_logger(),
                                        "Unable to retrieve full data for block #{}", cursor
                                    );
                                    continue;
                                }
                                (
                                    serde_json::from_str::<BlockIdentifier>(&payload[0]).unwrap(),
                                    serde_json::from_str::<BlockIdentifier>(&payload[1]).unwrap(),
                                    serde_json::from_str::<i64>(&payload[2]).unwrap(),
                                    serde_json::from_str::<Vec<StacksTransactionData>>(&payload[3])
                                        .unwrap(),
                                    serde_json::from_str::<StacksBlockMetadata>(&payload[4])
                                        .unwrap(),
                                )
                            };
                            let mut hits = vec![];
                            for tx in transactions.iter() {
                                if evaluate_stacks_transaction_predicate_on_transaction(
                                    &tx,
                                    &stacks_hook,
                                    &ctx,
                                ) {
                                    debug!(
                                        ctx.expect_logger(),
                                        "Action #{} triggered by transaction {} (block #{})",
                                        stacks_hook.uuid,
                                        tx.transaction_identifier.hash,
                                        cursor
                                    );
                                    hits.push(tx);
                                    total_hits.push(tx.transaction_identifier.hash.to_string());
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
                                if apply {
                                    if let Some(result) =
                                        handle_stacks_hook_action(trigger, &proofs, &ctx)
                                    {
                                        if let StacksChainhookOccurrence::Http(request) = result {
                                            hiro_system_kit::nestable_block_on(request.send())
                                                .unwrap();
                                        }
                                    }
                                }
                            }
                        }

                        info!(ctx.expect_logger(), "Stacks chainhook {} scan completed and triggered by {} transactions {}", stacks_hook.uuid, total_hits.len(), total_hits.join(","))
                    }
                    ChainhookSpecification::Bitcoin(bitcoin_hook) => {
                        let start_block = match bitcoin_hook.start_block {
                            Some(start_block) => start_block,
                            None => {
                                warn!(ctx.expect_logger(), "Bitcoin chainhook specification must include a field start_block in replay mode");
                                continue;
                            }
                        };
                        let tip_height = match bitcoin_rpc.get_blockchain_info() {
                            Ok(result) => result.blocks,
                            Err(e) => {
                                warn!(
                                    ctx.expect_logger(),
                                    "unable to retrieve Bitcoin chain tip ({})",
                                    e.to_string()
                                );
                                continue;
                            }
                        };
                        let end_block = bitcoin_hook.end_block.unwrap_or(tip_height);

                        info!(
                            ctx.expect_logger(), "Processing Bitcoin chainhook {}, will scan blocks [{}; {}] (apply = {})",
                            bitcoin_hook.uuid, start_block, end_block, apply
                        );

                        let mut total_hits = vec![];
                        for cursor in start_block..=end_block {
                            debug!(
                                ctx.expect_logger(),
                                "Evaluating predicate #{} on block #{}", bitcoin_hook.uuid, cursor
                            );

                            // Try to retrieve block from cache

                            let cached_block = {
                                let payload: Vec<String> = redis_con
                                    .hget(
                                        &format!("btc:{}", cursor),
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
                                    None
                                } else {
                                    let block = BitcoinBlockData {
                                        block_identifier: serde_json::from_str::<BlockIdentifier>(
                                            &payload[0],
                                        )
                                        .unwrap(),
                                        parent_block_identifier: serde_json::from_str::<
                                            BlockIdentifier,
                                        >(
                                            &payload[1]
                                        )
                                        .unwrap(),
                                        timestamp: serde_json::from_str::<u32>(&payload[2])
                                            .unwrap(),
                                        transactions: serde_json::from_str::<
                                            Vec<BitcoinTransactionData>,
                                        >(
                                            &payload[3]
                                        )
                                        .unwrap(),
                                        metadata: serde_json::from_str::<BitcoinBlockMetadata>(
                                            &payload[4],
                                        )
                                        .unwrap(),
                                    };
                                    debug!(
                                        ctx.expect_logger(),
                                        "Bitcoin block #{} retrieved from cache",
                                        block.block_identifier.index
                                    );
                                    Some(block)
                                }
                            };

                            let block = match cached_block {
                                Some(block) => block,
                                None => {
                                    let block_hash = match bitcoin_rpc.get_block_hash(cursor) {
                                        Ok(block_hash) => block_hash,
                                        Err(e) => {
                                            error!(
                                                ctx.expect_logger(),
                                                "unable to retrieve block hash {}: {}",
                                                cursor,
                                                e.to_string()
                                            );
                                            continue;
                                        }
                                    };

                                    let block = match bitcoin_rpc.get_block(&block_hash) {
                                        Ok(block) => {
                                            standardize_bitcoin_block(
                                                &indexer_config,
                                                cursor,
                                                block,
                                                &ctx,
                                            )
                                            .unwrap() // todo
                                        }
                                        Err(e) => {
                                            error!(
                                                ctx.expect_logger(),
                                                "unable to retrieve block {}: {}",
                                                cursor,
                                                e.to_string()
                                            );
                                            continue;
                                        }
                                    };

                                    let key = format!("btc:{}", block.block_identifier.index);
                                    match redis_con.hset_multiple(
                                        &key,
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
                                            ("timestamp", json!(block.timestamp).to_string()),
                                        ],
                                    ) {
                                        Ok(()) => {
                                            debug!(
                                                ctx.expect_logger(),
                                                "Bitcoin block #{} saved to cache",
                                                block.block_identifier.index
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                ctx.expect_logger(),
                                                "unable to keep block {key} in cache: {}",
                                                e.to_string()
                                            );
                                        }
                                    };

                                    block
                                }
                            };

                            let mut hits = vec![];
                            for tx in block.transactions.iter() {
                                if bitcoin_hook.evaluate_transaction_predicate(&tx) {
                                    debug!(
                                        ctx.expect_logger(),
                                        "Action #{} triggered by transaction {} (block #{})",
                                        bitcoin_hook.uuid,
                                        tx.transaction_identifier.hash,
                                        cursor
                                    );
                                    hits.push(tx);
                                    total_hits.push(tx.transaction_identifier.hash.to_string());
                                }
                            }

                            if hits.len() > 0 {
                                let trigger = BitcoinTriggerChainhook {
                                    chainhook: &bitcoin_hook,
                                    apply: vec![(hits, &block)],
                                    rollback: vec![],
                                };

                                let proofs = HashMap::new();
                                if apply {
                                    if let Some(result) =
                                        handle_bitcoin_hook_action(trigger, &proofs)
                                    {
                                        if let BitcoinChainhookOccurrence::Http(request) = result {
                                            hiro_system_kit::nestable_block_on(request.send())
                                                .unwrap();
                                        }
                                    }
                                }
                            }
                        }
                        info!(ctx.expect_logger(), "Bitcoin chainhook {} scan completed and triggered by {} transactions {}", bitcoin_hook.uuid, total_hits.len(), total_hits.join(","))
                    }
                }
            }
            ObserverEvent::BitcoinChainEvent(_chain_update) => {
                debug!(ctx.expect_logger(), "Bitcoin update not stored");
            }
            ObserverEvent::StacksChainEvent(chain_event) => {
                match &chain_event {
                    StacksChainEvent::ChainUpdatedWithBlocks(data) => {
                        update_storage_with_confirmed_stacks_blocks(
                            &mut redis_con,
                            &data.confirmed_blocks,
                            &ctx,
                        );
                    }
                    StacksChainEvent::ChainUpdatedWithReorg(data) => {
                        update_storage_with_confirmed_stacks_blocks(
                            &mut redis_con,
                            &data.confirmed_blocks,
                            &ctx,
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
}

pub fn seed_storage() {}

pub fn start_node(mut config: Config, ctx: Context) {
    let (digestion_tx, digestion_rx) = channel();
    let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
    let (observer_command_tx, observer_command_rx) = channel();

    let terminate_digestion_tx = digestion_tx.clone();
    let context_cloned = ctx.clone();
    ctrlc::set_handler(move || {
        warn!(
            &context_cloned.expect_logger(),
            "Manual interruption signal received"
        );
        terminate_digestion_tx
            .send(DigestingCommand::Kill)
            .expect("Unable to terminate service");
    })
    .expect("Error setting Ctrl-C handler");

    if config.is_initial_ingestion_required() {
        // Download default tsv.
        if config.rely_on_remote_tsv() && config.should_download_remote_tsv() {
            let url = config.expected_remote_tsv_url();
            let mut destination_path = config.expected_cache_path();
            destination_path.push("stacks-node-events.tsv");
            // Download archive if not already present in cache
            if !destination_path.exists() {
                info!(ctx.expect_logger(), "Downloading {}", url);
                match hiro_system_kit::nestable_block_on(archive::download_tsv_file(&config)) {
                    Ok(_) => {}
                    Err(e) => {
                        error!(ctx.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                }
                let mut destination_path = config.expected_cache_path();
                destination_path.push("stacks-node-events.tsv");
            }
            config.add_local_tsv_source(&destination_path);

            let ingestion_config = config.clone();
            let seed_digestion_tx = digestion_tx.clone();
            let context_cloned = ctx.clone();

            thread::spawn(move || {
                let res = block::ingestion::start(
                    seed_digestion_tx.clone(),
                    &ingestion_config,
                    context_cloned.clone(),
                );
                let (_stacks_chain_tip, _bitcoin_chain_tip) = match res {
                    Ok(chain_tips) => chain_tips,
                    Err(e) => {
                        error!(&context_cloned.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                };
            });
        }
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from stacks-node {}",
            config.expected_stacks_node_event_source()
        );
    }

    let digestion_config = config.clone();
    let terminate_observer_command_tx = observer_command_tx.clone();
    let context_cloned = ctx.clone();

    thread::spawn(move || {
        let res = block::digestion::start(digestion_rx, &digestion_config, &context_cloned);
        if let Err(e) = res {
            error!(&context_cloned.expect_logger(), "{}", e);
        }
        let _ = terminate_observer_command_tx.send(ObserverCommand::Terminate);
    });

    let mut chainhook_config = ChainhookConfig::new();

    {
        let redis_config = config.expected_redis_config();
        let client = redis::Client::open(redis_config.uri.clone()).unwrap();
        let mut redis_con = match client.get_connection() {
            Ok(con) => con,
            Err(message) => {
                error!(ctx.expect_logger(), "Redis: {}", message.to_string());
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
                        ctx.expect_logger(),
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

    let event_observer_config = EventObserverConfig {
        normalization_enabled: true,
        grpc_server_enabled: false,
        hooks_enabled: true,
        bitcoin_rpc_proxy_enabled: true,
        event_handlers: vec![],
        chainhook_config: Some(chainhook_config),
        ingestion_port: DEFAULT_INGESTION_PORT,
        control_port: DEFAULT_CONTROL_PORT,
        bitcoin_node_username: config.network.bitcoin_node_rpc_username.clone(),
        bitcoin_node_password: config.network.bitcoin_node_rpc_password.clone(),
        bitcoin_node_rpc_url: config.network.bitcoin_node_rpc_url.clone(),
        stacks_node_rpc_url: config.network.stacks_node_rpc_url.clone(),
        operators: HashSet::new(),
        display_logs: false,
    };
    info!(
        ctx.expect_logger(),
        "Listening for new blockchain events on port {}", DEFAULT_INGESTION_PORT
    );
    info!(
        ctx.expect_logger(),
        "Listening for chainhook predicate registrations on port {}", DEFAULT_CONTROL_PORT
    );
    let context_cloned = ctx.clone();
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
                    ctx.expect_logger(),
                    "Error: broken channel {}",
                    e.to_string()
                );
                break;
            }
        };
        let redis_config = config.expected_redis_config();
        let client = redis::Client::open(redis_config.uri.clone()).unwrap();
        let mut redis_con = match client.get_connection() {
            Ok(con) => con,
            Err(message) => {
                error!(ctx.expect_logger(), "Redis: {}", message.to_string());
                panic!();
            }
        };
        match event {
            ObserverEvent::HookRegistered(chainhook) => {
                // If start block specified, use it.
                // I no start block specified, depending on the nature the hook, we'd like to retrieve:
                // - contract-id

                let chainhook_key = chainhook.key();
                let mut history: Vec<u64> = vec![];
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
                        ctx.expect_logger(),
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
                            ctx.expect_logger(),
                            "Processing Stacks chainhook {}, will scan blocks [{}; {}]",
                            stacks_hook.uuid,
                            start_block,
                            end_block
                        );
                        let mut total_hits = 0;
                        for cursor in start_block..=end_block {
                            debug!(
                                ctx.expect_logger(),
                                "Evaluating predicate #{} on block #{}", stacks_hook.uuid, cursor
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
                                    warn!(ctx.expect_logger(), "Chain still being processed, please retry in a few minutes");
                                    continue;
                                }
                                (
                                    serde_json::from_str::<BlockIdentifier>(&payload[0]).unwrap(),
                                    serde_json::from_str::<BlockIdentifier>(&payload[1]).unwrap(),
                                    serde_json::from_str::<i64>(&payload[2]).unwrap(),
                                    serde_json::from_str::<Vec<StacksTransactionData>>(&payload[3])
                                        .unwrap(),
                                    serde_json::from_str::<StacksBlockMetadata>(&payload[4])
                                        .unwrap(),
                                )
                            };
                            let mut hits = vec![];
                            for tx in transactions.iter() {
                                if evaluate_stacks_transaction_predicate_on_transaction(
                                    &tx,
                                    &stacks_hook,
                                    &ctx,
                                ) {
                                    debug!(
                                        ctx.expect_logger(),
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
                                if let Some(result) =
                                    handle_stacks_hook_action(trigger, &proofs, &ctx)
                                {
                                    if let StacksChainhookOccurrence::Http(request) = result {
                                        hiro_system_kit::nestable_block_on(request.send()).unwrap();
                                    }
                                }
                            }
                        }
                        info!(ctx.expect_logger(), "Stacks chainhook {} scan completed: action triggered by {} transactions", stacks_hook.uuid, total_hits);
                    }
                    ChainhookSpecification::Bitcoin(_bitcoin_hook) => {
                        warn!(
                            ctx.expect_logger(),
                            "Bitcoin chainhook evaluation unavailable for historical data"
                        );
                    }
                }
            }
            ObserverEvent::HookDeregistered(chainhook) => {
                let chainhook_key = chainhook.key();
                let _: Result<(), redis::RedisError> = redis_con.del(chainhook_key);
            }
            ObserverEvent::BitcoinChainEvent(_chain_update) => {
                debug!(ctx.expect_logger(), "Bitcoin update not stored");
            }
            ObserverEvent::StacksChainEvent(chain_event) => {
                match &chain_event {
                    StacksChainEvent::ChainUpdatedWithBlocks(data) => {
                        update_storage_with_confirmed_stacks_blocks(
                            &mut redis_con,
                            &data.confirmed_blocks,
                            &ctx,
                        );
                    }
                    StacksChainEvent::ChainUpdatedWithReorg(data) => {
                        update_storage_with_confirmed_stacks_blocks(
                            &mut redis_con,
                            &data.confirmed_blocks,
                            &ctx,
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
