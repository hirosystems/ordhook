use crate::block::DigestingCommand;
use crate::config::generator::generate_config;
use crate::config::Config;
use crate::node::Node;
use crate::scan::bitcoin::scan_bitcoin_chain_with_predicate;
use crate::scan::stacks::scan_stacks_chain_with_predicate;

use chainhook_event_observer::chainhooks::types::ChainhookFullSpecification;
use chainhook_event_observer::indexer::ordinals::db::{
    build_bitcoin_traversal_local_storage, find_inscriptions_at_wached_outpoint,
    initialize_ordinal_state_storage, open_readonly_ordinals_db_conn,
    retrieve_satoshi_point_using_local_storage,
};
use chainhook_event_observer::observer::BitcoinConfig;
use chainhook_event_observer::utils::Context;
use chainhook_types::{BlockIdentifier, TransactionIdentifier};
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
use std::io::{BufReader, Read};
use std::path::PathBuf;
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
    /// Manage config
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Start chainhook-cli
    #[clap(subcommand)]
    Node(NodeCommand),
    /// Protocols specific commands
    #[clap(subcommand)]
    Protocols(ProtocolsCommand),
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

#[derive(Subcommand, PartialEq, Clone, Debug)]
#[clap(bin_name = "config", aliases = &["config"])]
enum ConfigCommand {
    /// Generate new predicate
    #[clap(name = "new", bin_name = "new", aliases = &["generate"])]
    New(NewConfig),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct NewConfig {
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
enum ProtocolsCommand {
    /// Ordinals related commands
    #[clap(subcommand)]
    Ordinals(OrdinalsCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum OrdinalsCommand {
    /// Retrieve Satoshi
    #[clap(name = "sat", bin_name = "sat")]
    Satoshi(GetSatoshiCommand),
    /// Retrieve Satoshi
    #[clap(name = "inscriptions", bin_name = "inscriptions")]
    WatchedOutpoints(GetWatchedOutpointsCommand),
    /// Retrieve Satoshi
    Traversals(BuildOrdinalsTraversalsCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct BuildOrdinalsTraversalsCommand {
    /// Starting block
    pub start_block: u64,
    /// Starting block
    pub end_block: u64,
    /// # of Networking thread
    pub network_threads: usize,
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
struct GetWatchedOutpointsCommand {
    /// Outpoint
    pub outpoint: String,
    /// Load config file path
    #[clap(long = "db-path")]
    pub db_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct GetSatoshiCommand {
    /// Block height
    pub block_height: u64,
    /// Txid
    pub txid: String,
    /// Output index
    pub output_index: usize,
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
                return node.run().await;
            }
        },
        Command::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::fs::File;
                use std::io::Write;
                let config_content = generate_config();
                let mut file_path = PathBuf::new();
                file_path.push("Chainhook.toml");
                let mut file = File::create(&file_path)
                    .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
                file.write_all(config_content.as_bytes())
                    .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
                println!("Created file Chainhook.toml");
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
        Command::Protocols(ProtocolsCommand::Ordinals(subcmd)) => match subcmd {
            OrdinalsCommand::Satoshi(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                let transaction_identifier = TransactionIdentifier {
                    hash: cmd.txid.clone(),
                };
                let block_identifier = BlockIdentifier {
                    index: cmd.block_height,
                    hash: "".into(),
                };

                let storage_conn =
                    open_readonly_ordinals_db_conn(&config.expected_cache_path(), &ctx).unwrap();

                let (block_height, offset, ordinal_number) =
                    retrieve_satoshi_point_using_local_storage(
                        &storage_conn,
                        &block_identifier,
                        &transaction_identifier,
                        &ctx,
                    )?;
                info!(
                    ctx.expect_logger(),
                    "Block: {block_height}, Offset {offset}:, Ordinal number: {ordinal_number}",
                );
            }
            OrdinalsCommand::Traversals(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let bitcoin_config = BitcoinConfig {
                    username: config.network.bitcoin_node_rpc_username.clone(),
                    password: config.network.bitcoin_node_rpc_password.clone(),
                    rpc_url: config.network.bitcoin_node_rpc_url.clone(),
                };

                let storage_conn =
                    initialize_ordinal_state_storage(&config.expected_cache_path(), &ctx);

                let _ = build_bitcoin_traversal_local_storage(
                    &bitcoin_config,
                    &storage_conn,
                    cmd.start_block,
                    cmd.end_block,
                    &ctx,
                    cmd.network_threads,
                )
                .await;
            }
            OrdinalsCommand::WatchedOutpoints(cmd) => {
                let mut db_path = PathBuf::new();
                db_path.push(&cmd.db_path.unwrap());

                let storage_conn = open_readonly_ordinals_db_conn(&db_path, &ctx).unwrap();

                let results = find_inscriptions_at_wached_outpoint(&cmd.outpoint, &storage_conn);
                println!("{:?}", results);
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
