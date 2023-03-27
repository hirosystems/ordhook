use crate::block::DigestingCommand;
use crate::config::generator::generate_config;
use crate::config::Config;
use crate::node::Node;
use crate::scan::bitcoin::scan_bitcoin_chain_with_predicate;
use crate::scan::stacks::scan_stacks_chain_with_predicate;

use chainhook_event_observer::bitcoincore_rpc::{Auth, Client, RpcApi};
use chainhook_event_observer::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification, BitcoinPredicateType,
    ChainhookFullSpecification, FileHook, HookAction, OrdinalOperations, Protocols,
    StacksChainhookFullSpecification, StacksChainhookNetworkSpecification, StacksPredicate,
    StacksPrintEventBasedPredicate,
};
use chainhook_event_observer::hord::db::{
    delete_data_in_hord_db, fetch_and_cache_blocks_in_hord_db,
    find_inscriptions_at_wached_outpoint, find_latest_compacted_block_known,
    open_readonly_hord_db_conn, open_readwrite_hord_db_conn,
    retrieve_satoshi_point_using_local_storage,
};
use chainhook_event_observer::observer::BitcoinConfig;
use chainhook_event_observer::utils::Context;
use chainhook_types::{BitcoinNetwork, BlockIdentifier, StacksNetwork, TransactionIdentifier};
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
use std::collections::BTreeMap;
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
    Hord(HordCommand),
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
enum HordCommand {
    /// Db maintenance related commands
    #[clap(subcommand)]
    Db(DbCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Find(FindCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum DbCommand {
    /// Init hord db
    #[clap(name = "init", bin_name = "init")]
    Init(InitHordDbCommand),
    /// Update hord db
    #[clap(name = "update", bin_name = "update")]
    Update(UpdateHordDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropHordDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum FindCommand {
    /// Init hord db
    #[clap(name = "sat_point", bin_name = "sat_point")]
    SatPoint(FindSatPointCommand),
    /// Update hord db
    #[clap(name = "inscription", bin_name = "inscription")]
    Inscription(FindInscriptionCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct FindSatPointCommand {
    /// Block height
    pub block_height: u64,
    /// Txid
    pub txid: String,
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
struct FindInscriptionCommand {
    /// Outpoint
    pub outpoint: String,
    /// Load config file path
    #[clap(long = "db-path")]
    pub db_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct InitHordDbCommand {
    /// # of Networking thread
    pub network_threads: usize,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct UpdateHordDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Starting block
    pub end_block: u64,
    /// # of Networking thread
    pub network_threads: usize,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct DropHordDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Starting block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
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
                let _config = Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &None)?;
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
            PredicatesCommand::New(cmd) => {
                use uuid::Uuid;

                let id = Uuid::new_v4();

                let predicate = match (cmd.stacks, cmd.bitcoin) {
                    (true, false) => {
                        let mut networks = BTreeMap::new();

                        networks.insert(StacksNetwork::Simnet, StacksChainhookNetworkSpecification {
                            start_block: Some(0),
                            end_block: Some(100),
                            predicate: StacksPredicate::PrintEvent(StacksPrintEventBasedPredicate {
                                contract_identifier: "ST1SVA0SST0EDT4MFYGWGP6GNSXMMQJDVP1G8QTTC.arkadiko-freddie-v1-1".into(),
                                contains: "vault".into(),
                            }),
                            expire_after_occurrence: None,
                            capture_all_events: None,
                            decode_clarity_values: None,
                            action:  HookAction::FileAppend(FileHook {
                                path: "arkadiko.txt".into()
                            })
                        });

                        networks.insert(StacksNetwork::Mainnet, StacksChainhookNetworkSpecification {
                            start_block: Some(0),
                            end_block: Some(100),
                            predicate: StacksPredicate::PrintEvent(StacksPrintEventBasedPredicate {
                                contract_identifier: "SP2C2YFP12AJZB4MABJBAJ55XECVS7E4PMMZ89YZR.arkadiko-freddie-v1-1".into(),
                                contains: "vault".into(),
                            }),
                            expire_after_occurrence: None,
                            capture_all_events: None,
                            decode_clarity_values: None,
                            action:  HookAction::FileAppend(FileHook {
                                path: "arkadiko.txt".into()
                            })
                        });

                        ChainhookFullSpecification::Stacks(StacksChainhookFullSpecification {
                            uuid: id.to_string(),
                            owner_uuid: None,
                            name: "Hello world".into(),
                            version: 1,
                            networks,
                        })
                    }
                    (false, true) => {
                        let mut networks = BTreeMap::new();

                        networks.insert(
                            BitcoinNetwork::Mainnet,
                            BitcoinChainhookNetworkSpecification {
                                start_block: Some(0),
                                end_block: Some(100),
                                predicate: BitcoinPredicateType::Protocol(Protocols::Ordinal(
                                    OrdinalOperations::InscriptionRevealed,
                                )),
                                expire_after_occurrence: None,
                                action: HookAction::FileAppend(FileHook {
                                    path: "ordinals.txt".into(),
                                }),
                            },
                        );

                        ChainhookFullSpecification::Bitcoin(BitcoinChainhookFullSpecification {
                            uuid: id.to_string(),
                            owner_uuid: None,
                            name: "Hello world".into(),
                            version: 1,
                            networks,
                        })
                    }
                    _ => {
                        return Err("command `predicates new` should either provide the flag --stacks or --bitcoin".into());
                    }
                };

                let content = serde_json::to_string_pretty(&predicate).unwrap();
                let mut path = PathBuf::new();
                path.push(cmd.name);

                match std::fs::metadata(&path) {
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            // need to create
                            if let Some(dirp) = PathBuf::from(&path).parent() {
                                std::fs::create_dir_all(dirp).unwrap_or_else(|e| {
                                    println!("{}", e.to_string());
                                });
                            }
                            let mut f = std::fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&path)
                                .map_err(|e| format!("{}", e.to_string()))?;
                            use std::io::Write;
                            let _ = f.write_all(content.as_bytes());
                        } else {
                            panic!("FATAL: could not stat {}", path.display());
                        }
                    }
                    Ok(_m) => {
                        let err = format!("File {} already exists", path.display());
                        return Err(err);
                    }
                };
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
                        scan_bitcoin_chain_with_predicate(predicate, &config, &ctx).await?;
                    }
                    ChainhookFullSpecification::Stacks(predicate) => {
                        scan_stacks_chain_with_predicate(predicate, &mut config, &ctx).await?;
                    }
                }
            }
        },
        Command::Hord(HordCommand::Find(subcmd)) => match subcmd {
            FindCommand::SatPoint(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let hord_db_conn =
                    open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx).unwrap();

                let tip_height = find_latest_compacted_block_known(&hord_db_conn) as u64;

                if cmd.block_height > tip_height {
                    perform_hord_db_update(tip_height, cmd.block_height, 8, &config, &ctx).await?;
                }

                let transaction_identifier = TransactionIdentifier {
                    hash: cmd.txid.clone(),
                };
                let block_identifier = BlockIdentifier {
                    index: cmd.block_height,
                    hash: "".into(),
                };

                let (block_height, offset, ordinal_number, hops) =
                    retrieve_satoshi_point_using_local_storage(
                        &hord_db_conn,
                        &block_identifier,
                        &transaction_identifier,
                        &ctx,
                    )?;
                info!(
                    ctx.expect_logger(),
                    "Satoshi #{ordinal_number} was minted in block #{block_height} at offset {offset} and was transferred {hops} times.",
                );
            }
            FindCommand::Inscription(cmd) => {
                let mut db_path = PathBuf::new();
                db_path.push(&cmd.db_path.unwrap());

                let hord_db_conn = open_readonly_hord_db_conn(&db_path, &ctx).unwrap();

                let results = find_inscriptions_at_wached_outpoint(&cmd.outpoint, &hord_db_conn);
                println!("{:?}", results);
            }
        },
        Command::Hord(HordCommand::Db(subcmd)) => match subcmd {
            DbCommand::Init(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
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

                let end_block = match bitcoin_rpc.get_blockchain_info() {
                    Ok(result) => result.blocks,
                    Err(e) => {
                        return Err(format!(
                            "unable to retrieve Bitcoin chain tip ({})",
                            e.to_string()
                        ));
                    }
                };

                perform_hord_db_update(0, end_block, cmd.network_threads, &config, &ctx).await?;
            }
            DbCommand::Update(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                perform_hord_db_update(
                    cmd.start_block,
                    cmd.end_block,
                    cmd.network_threads,
                    &config,
                    &ctx,
                )
                .await?;
            }
            DbCommand::Drop(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let rw_hord_db_conn =
                    open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;
                delete_data_in_hord_db(cmd.start_block, cmd.end_block, &rw_hord_db_conn, &ctx)?;
                info!(
                    ctx.expect_logger(),
                    "Cleaning hord_db: {} blocks dropped",
                    cmd.end_block - cmd.start_block + 1
                );
            }
        },
    }
    Ok(())
}

pub async fn perform_hord_db_update(
    start_block: u64,
    end_block: u64,
    network_threads: usize,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    info!(
        ctx.expect_logger(),
        "Syncing hord_db: {} blocks to download ({start_block}: {end_block}), using {network_threads} network threads", end_block - start_block + 1
    );

    let bitcoin_config = BitcoinConfig {
        username: config.network.bitcoin_node_rpc_username.clone(),
        password: config.network.bitcoin_node_rpc_password.clone(),
        rpc_url: config.network.bitcoin_node_rpc_url.clone(),
        network: config.network.bitcoin_network.clone(),
    };

    let rw_hord_db_conn = open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    let _ = fetch_and_cache_blocks_in_hord_db(
        &bitcoin_config,
        &rw_hord_db_conn,
        start_block,
        end_block,
        &ctx,
        network_threads,
    )
    .await?;

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
