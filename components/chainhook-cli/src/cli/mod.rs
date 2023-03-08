use crate::block::DigestingCommand;
use crate::config::Config;
use crate::node::Node;
use crate::scan::bitcoin::{
    scan_bitcoin_chain_with_predicate, retrieve_satoshi_point_using_local_storage,
};
use crate::scan::stacks::scan_stacks_chain_with_predicate;

use chainhook_event_observer::bitcoincore_rpc::bitcoin::OutPoint;
use chainhook_event_observer::chainhooks::types::ChainhookFullSpecification;
use chainhook_event_observer::indexer::ordinals::indexing::entry::{Entry, SatRange};
use chainhook_event_observer::indexer::ordinals::indexing::{
    OUTPOINT_TO_SAT_RANGES, SAT_TO_SATPOINT,
};
use chainhook_event_observer::indexer::ordinals::initialize_ordinal_index;
use chainhook_event_observer::indexer::ordinals::sat_point::SatPoint;
use chainhook_event_observer::observer::{
    EventObserverConfig, DEFAULT_CONTROL_PORT, DEFAULT_INGESTION_PORT,
};
use chainhook_event_observer::redb::ReadableTable;
use chainhook_event_observer::utils::Context;
use chainhook_types::{BlockIdentifier, TransactionIdentifier};
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
use std::collections::{HashSet, VecDeque};
use std::io::{BufReader, Read};
use std::process;
use std::sync::mpsc::Sender;

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
    /// Ordinals related commands
    #[clap(subcommand)]
    Ordinals(OrdinalsCommand),
    /// Db related commands (debug purposes)
    #[clap(subcommand)]
    Db(DbCommand),
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
    /// Chainhook spec file to scan (json format)
    pub predicate_path: String,
    /// Target Testnet network
    #[clap(long = "testnet", conflicts_with = "mainnet")]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(long = "mainnet", conflicts_with = "testnet")]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet"
    )]
    pub config_path: Option<String>,
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

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum OrdinalsCommand {
    /// Retrieve Satoshi
    #[clap(name = "sat", bin_name = "sat")]
    Satoshi(GetSatoshiCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum DbCommand {
    /// Dump DB storage
    #[clap(name = "dump", bin_name = "dump")]
    Dump(DumpDbCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct GetSatoshiCommand {
    /// Txid
    pub txid: String,
    /// Output index
    pub block_height: u64,
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
struct DumpDbCommand {
    /// Db path
    pub path: String,
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

    match hiro_system_kit::nestable_block_on(handle_command(opts, ctx)) {
        Err(e) => {
            println!("{e}");
            process::exit(1);
        }
        Ok(_) => {}
    }
}

async fn handle_command(opts: Opts, ctx: Context) -> Result<(), String> {
    match opts.command {
        Command::Node(subcmd) => match subcmd {
            NodeCommand::Start(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                let mut node = Node::new(config, ctx);
                node.run();
            }
        },
        Command::Predicates(subcmd) => match subcmd {
            PredicatesCommand::New(_cmd) => {
                // let manifest = clarinet_files::get_manifest_location(None);
            }
            PredicatesCommand::Scan(cmd) => {
                let mut config =
                    Config::default(false, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                let file = std::fs::File::open(&cmd.predicate_path)
                    .map_err(|e| format!("unable to read file {}\n{:?}", cmd.predicate_path, e))?;
                let mut file_reader = BufReader::new(file);
                let mut file_buffer = vec![];
                file_reader
                    .read_to_end(&mut file_buffer)
                    .map_err(|e| format!("unable to read file {}\n{:?}", cmd.predicate_path, e))?;
                let predicate: ChainhookFullSpecification = serde_json::from_slice(&file_buffer)
                    .map_err(|e| {
                        format!("unable to parse json file {}\n{:?}", cmd.predicate_path, e)
                    })?;
                match predicate {
                    ChainhookFullSpecification::Bitcoin(predicate) => {
                        scan_bitcoin_chain_with_predicate(predicate, true, &config, &ctx).await?;
                    }
                    ChainhookFullSpecification::Stacks(predicate) => {
                        scan_stacks_chain_with_predicate(predicate, true, &mut config, &ctx)
                            .await?;
                    }
                }
            }
        },
        Command::Ordinals(subcmd) => match subcmd {
            OrdinalsCommand::Satoshi(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                let txid = TransactionIdentifier { hash: cmd.txid.clone() };
                let block_id = BlockIdentifier { hash: "".into(), index: cmd.block_height };
                retrieve_satoshi_point_using_local_storage(&config, &block_id, &txid)
                    .await?;
            }
        },
        Command::Db(subcmd) => match subcmd {
            DbCommand::Dump(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

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
                    cache_path: "cache/tmp".to_string(),
                    bitcoin_network: config.network.bitcoin_network.clone(),
                };

                let ordinal_index = match initialize_ordinal_index(
                    &event_observer_config,
                    Some(cmd.path.into()),
                    &ctx,
                ) {
                    Ok(index) => index,
                    Err(e) => {
                        panic!()
                    }
                };

                for (key, value) in ordinal_index
                    .database
                    .begin_read()
                    .unwrap()
                    .open_table(SAT_TO_SATPOINT)
                    .unwrap()
                    .iter()
                    .unwrap()
                {
                    let sat_point = SatPoint::load(*value.value());
                    println!("{} -> {}", key.value(), sat_point);
                }

                println!("=====");

                for (key, raw_ranges) in ordinal_index
                    .database
                    .begin_read()
                    .unwrap()
                    .open_table(OUTPOINT_TO_SAT_RANGES)
                    .unwrap()
                    .iter()
                    .unwrap()
                {
                    let outpoint = OutPoint::load(*key.value());
                    let mut input_sat_ranges = VecDeque::new();
                    for chunk in raw_ranges.value().chunks_exact(11) {
                        input_sat_ranges.push_back(SatRange::load(chunk.try_into().unwrap()));
                    }
                    println!("{} -> {:?}", outpoint, input_sat_ranges);
                }
            }
        },
    }
    Ok(())
}

#[allow(dead_code)]
pub fn install_ctrlc_handler(terminate_tx: Sender<DigestingCommand>, ctx: Context) {
    ctrlc::set_handler(move || {
        warn!(&ctx.expect_logger(), "Manual interruption signal received");
        terminate_tx
            .send(DigestingCommand::Kill)
            .expect("Unable to terminate service");
    })
    .expect("Error setting Ctrl-C handler");
}
