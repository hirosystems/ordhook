use crate::block::DigestingCommand;
use crate::config::generator::generate_config;
use crate::config::Config;
use crate::scan::bitcoin::scan_bitcoin_chain_with_predicate_via_http;
use crate::scan::stacks::scan_stacks_chain_with_predicate;
use crate::service::Service;

use chainhook_event_observer::bitcoincore_rpc::{Auth, Client, RpcApi};
use chainhook_event_observer::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification, BitcoinPredicateType,
    ChainhookFullSpecification, FileHook, HookAction, OrdinalOperations, Protocols,
    StacksChainhookFullSpecification, StacksChainhookNetworkSpecification, StacksPredicate,
    StacksPrintEventBasedPredicate,
};
use chainhook_event_observer::hord::db::{
    delete_blocks_in_block_range_sqlite, delete_data_in_hord_db, fetch_and_cache_blocks_in_hord_db,
    find_block_at_block_height, find_block_at_block_height_sqlite,
    find_inscriptions_at_wached_outpoint, find_last_block_inserted, initialize_hord_db,
    insert_entry_in_blocks, open_readonly_hord_db_conn, open_readonly_hord_db_conn_rocks_db,
    open_readwrite_hord_db_conn, open_readwrite_hord_db_conn_rocks_db,
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
    Service(ServiceCommand),
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
enum ServiceCommand {
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
    /// Specify relative path of the chainhooks (yaml format) to evaluate
    #[clap(long = "predicate-path")]
    pub predicates_paths: Vec<String>,
    /// Start REST API for managing predicates
    #[clap(long = "start-http-api")]
    pub start_http_api: bool,
    /// Disable hord indexing
    #[clap(long = "no-hord")]
    pub hord_disabled: bool,
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
    /// Rewrite hord db
    #[clap(name = "rewrite", bin_name = "rewrite")]
    Rewrite(UpdateHordDbCommand),
    /// Catch-up hord db
    #[clap(name = "sync", bin_name = "sync")]
    Sync(SyncHordDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropHordDbCommand),
    /// Patch DB
    #[clap(name = "patch", bin_name = "patch")]
    Patch(PatchHordDbCommand),
    /// Check integrity
    #[clap(name = "check", bin_name = "check")]
    Check(CheckHordDbCommand),
    /// Legacy command
    #[clap(name = "init", bin_name = "init")]
    Init(InitHordDbCommand),
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
struct SyncHordDbCommand {
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

#[derive(Parser, PartialEq, Clone, Debug)]
struct PatchHordDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct CheckHordDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct InitHordDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
    /// # of Networking thread
    pub network_threads: usize,
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
        Command::Service(subcmd) => match subcmd {
            ServiceCommand::Start(cmd) => {
                let mut config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                // We disable the API if a predicate was passed, and the --enable-
                if cmd.predicates_paths.len() > 0 && !cmd.start_http_api {
                    config.chainhooks.enable_http_api = false;
                }
                let predicates = cmd
                    .predicates_paths
                    .iter()
                    .map(|p| load_predicate_from_path(p))
                    .collect::<Result<Vec<ChainhookFullSpecification>, _>>()?;

                info!(ctx.expect_logger(), "Starting service...",);

                if !cmd.hord_disabled {
                    info!(
                        ctx.expect_logger(),
                        "Ordinal indexing is enabled by default hord, checking index... (use --no-hord to disable ordinals)"
                    );

                    if let Some((start_block, end_block)) = should_sync_hord_db(&config, &ctx)? {
                        if start_block == 0 {
                            info!(
                                ctx.expect_logger(),
                                "Initializing hord indexing from block #{}", start_block
                            );
                        } else {
                            info!(
                                ctx.expect_logger(),
                                "Resuming hord indexing from block #{}", start_block
                            );
                        }
                        perform_hord_db_update(start_block, end_block, 8, &config, &ctx).await?;
                    }
                }

                let mut service = Service::new(config, ctx);
                return service.run(predicates).await;
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
                let predicate = load_predicate_from_path(&cmd.predicate_path)?;
                match predicate {
                    ChainhookFullSpecification::Bitcoin(predicate) => {
                        let predicate_spec = match predicate
                            .into_selected_network_specification(&config.network.bitcoin_network)
                        {
                            Ok(predicate) => predicate,
                            Err(e) => {
                                return Err(format!(
                                    "Specification missing for network {:?}: {e}",
                                    config.network.bitcoin_network
                                ));
                            }
                        };

                        scan_bitcoin_chain_with_predicate_via_http(predicate_spec, &config, &ctx).await?;
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
                    open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)
                        .unwrap();

                let tip_height = find_last_block_inserted(&hord_db_conn) as u64;
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

                let traversal = retrieve_satoshi_point_using_local_storage(
                    &hord_db_conn,
                    &block_identifier,
                    &transaction_identifier,
                    0,
                    &ctx,
                )?;
                info!(
                    ctx.expect_logger(),
                    "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times.",
                    traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
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

                let sqlite_db_conn_rw =
                    open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                // Migrate if required
                if find_block_at_block_height_sqlite(1, &sqlite_db_conn_rw).is_some() {
                    let blocks_db =
                        open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                    for i in 0..=300000 {
                        match find_block_at_block_height_sqlite(i, &sqlite_db_conn_rw) {
                            Some(block) => {
                                insert_entry_in_blocks(i, &block, &blocks_db, &ctx);
                                info!(ctx.expect_logger(), "Block #{} inserted", i);
                            }
                            None => {
                                error!(ctx.expect_logger(), "Block #{} missing", i);
                            }
                        }
                    }
                    let _ = blocks_db.flush();
                    delete_blocks_in_block_range_sqlite(0, 300000, &sqlite_db_conn_rw, &ctx);

                    for i in 300001..=500000 {
                        match find_block_at_block_height_sqlite(i, &sqlite_db_conn_rw) {
                            Some(block) => {
                                insert_entry_in_blocks(i, &block, &blocks_db, &ctx);
                                info!(ctx.expect_logger(), "Block #{} inserted", i);
                            }
                            None => {
                                info!(ctx.expect_logger(), "Block #{} missing", i);
                            }
                        }
                    }
                    let _ = blocks_db.flush();
                    delete_blocks_in_block_range_sqlite(300001, 500000, &sqlite_db_conn_rw, &ctx);

                    for i in 500001..=783986 {
                        match find_block_at_block_height_sqlite(i, &sqlite_db_conn_rw) {
                            Some(block) => {
                                insert_entry_in_blocks(i, &block, &blocks_db, &ctx);
                                info!(ctx.expect_logger(), "Block #{} inserted", i);
                            }
                            None => {
                                info!(ctx.expect_logger(), "Block #{} missing", i);
                            }
                        }
                    }
                    let _ = blocks_db.flush();
                    delete_blocks_in_block_range_sqlite(500001, 783986, &sqlite_db_conn_rw, &ctx);
                }

                // Sync
                for _ in 0..5 {
                    if let Some((start_block, end_block)) = should_sync_hord_db(&config, &ctx)? {
                        if start_block == 0 {
                            info!(
                                ctx.expect_logger(),
                                "Initializing hord indexing from block #{}", start_block
                            );
                        } else {
                            info!(
                                ctx.expect_logger(),
                                "Resuming hord indexing from block #{}", start_block
                            );
                        }
                        perform_hord_db_update(start_block, end_block, 10, &config, &ctx).await?;
                    } else {
                        info!(ctx.expect_logger(), "Database hord up to date");
                    }
                }

                // Start node
                let mut service = Service::new(config, ctx);
                return service.run(vec![]).await;
            }
            DbCommand::Sync(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                if let Some((start_block, end_block)) = should_sync_hord_db(&config, &ctx)? {
                    if start_block == 0 {
                        info!(
                            ctx.expect_logger(),
                            "Initializing hord indexing from block #{}", start_block
                        );
                    } else {
                        info!(
                            ctx.expect_logger(),
                            "Resuming hord indexing from block #{}", start_block
                        );
                    }
                    perform_hord_db_update(
                        start_block,
                        end_block,
                        cmd.network_threads,
                        &config,
                        &ctx,
                    )
                    .await?;
                } else {
                    info!(ctx.expect_logger(), "Database hord up to date");
                }
            }
            DbCommand::Rewrite(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                // Delete data, if any
                {
                    let blocks_db_rw =
                        open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
                    let inscriptions_db_conn_rw =
                        open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                    delete_data_in_hord_db(
                        cmd.start_block,
                        cmd.end_block,
                        &blocks_db_rw,
                        &inscriptions_db_conn_rw,
                        &ctx,
                    )?;
                }
                // Update data
                perform_hord_db_update(
                    cmd.start_block,
                    cmd.end_block,
                    cmd.network_threads,
                    &config,
                    &ctx,
                )
                .await?;
            }
            DbCommand::Check(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                // Delete data, if any
                {
                    let blocks_db_rw =
                        open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                    let mut missing_blocks = vec![];
                    for i in 1..=780000 {
                        if find_block_at_block_height(i, &blocks_db_rw).is_none() {
                            println!("Missing block {i}");
                            missing_blocks.push(i);
                        }
                    }
                    println!("{:?}", missing_blocks);
                }
            }
            DbCommand::Drop(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let blocks_db =
                    open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
                let inscriptions_db_conn_rw =
                    open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                delete_data_in_hord_db(
                    cmd.start_block,
                    cmd.end_block,
                    &blocks_db,
                    &inscriptions_db_conn_rw,
                    &ctx,
                )?;
                info!(
                    ctx.expect_logger(),
                    "Cleaning hord_db: {} blocks dropped",
                    cmd.end_block - cmd.start_block + 1
                );
            }
            DbCommand::Patch(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let sqlite_db_conn =
                    open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                let blocks_db =
                    open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                for i in 0..774940 {
                    match find_block_at_block_height_sqlite(i, &sqlite_db_conn) {
                        Some(block) => {
                            insert_entry_in_blocks(i, &block, &blocks_db, &ctx);
                            println!("Block #{} inserted", i);
                        }
                        None => {
                            println!("Block #{} missing", i)
                        }
                    }
                }
            }
        },
    }
    Ok(())
}

pub fn should_sync_hord_db(config: &Config, ctx: &Context) -> Result<Option<(u64, u64)>, String> {
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

    let start_block = match open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)
    {
        Ok(blocks_db) => find_last_block_inserted(&blocks_db) as u64,
        Err(err) => {
            warn!(ctx.expect_logger(), "{}", err);
            0
        }
    };

    if start_block == 0 {
        let _ = initialize_hord_db(&config.expected_cache_path(), &ctx);
    }

    let end_block = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks,
        Err(e) => {
            return Err(format!(
                "unable to retrieve Bitcoin chain tip ({})",
                e.to_string()
            ));
        }
    };

    if start_block < end_block {
        Ok(Some((start_block, end_block)))
    } else {
        Ok(None)
    }
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
        username: config.network.bitcoind_rpc_username.clone(),
        password: config.network.bitcoind_rpc_password.clone(),
        rpc_url: config.network.bitcoind_rpc_url.clone(),
        network: config.network.bitcoin_network.clone(),
        bitcoin_block_signaling: config.network.bitcoin_block_signaling.clone(),
    };

    let blocks_db = open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
    let inscriptions_db_conn_rw = open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx)?;

    let _ = fetch_and_cache_blocks_in_hord_db(
        &bitcoin_config,
        &blocks_db,
        &inscriptions_db_conn_rw,
        start_block,
        end_block,
        network_threads,
        &config.expected_cache_path(),
        &ctx,
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

pub fn load_predicate_from_path(
    predicate_path: &str,
) -> Result<ChainhookFullSpecification, String> {
    let file = std::fs::File::open(&predicate_path)
        .map_err(|e| format!("unable to read file {}\n{:?}", predicate_path, e))?;
    let mut file_reader = BufReader::new(file);
    let mut file_buffer = vec![];
    file_reader
        .read_to_end(&mut file_buffer)
        .map_err(|e| format!("unable to read file {}\n{:?}", predicate_path, e))?;
    let predicate: ChainhookFullSpecification = serde_json::from_slice(&file_buffer)
        .map_err(|e| format!("unable to parse json file {}\n{:?}", predicate_path, e))?;
    Ok(predicate)
}
