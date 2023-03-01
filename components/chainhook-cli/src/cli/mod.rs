use crate::block::DigestingCommand;
use crate::config::Config;
use crate::node::Node;
use crate::ordinals::retrieve_satoshi_point;
use crate::scan::bitcoin::scan_bitcoin_chain_with_predicate;
use crate::scan::stacks::scan_stacks_chain_with_predicate;

use chainhook_event_observer::chainhooks::types::ChainhookFullSpecification;
use chainhook_event_observer::utils::Context;
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
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

#[derive(Parser, PartialEq, Clone, Debug)]
struct GetSatoshiCommand {
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

                retrieve_satoshi_point(&config, &cmd.txid, cmd.output_index).await?;
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
