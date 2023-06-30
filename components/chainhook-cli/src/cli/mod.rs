use crate::archive::download_stacks_dataset_if_required;
use crate::block::DigestingCommand;
use crate::config::generator::generate_config;
use crate::config::{Config, PredicatesApi};
use crate::scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate;
use crate::scan::stacks::scan_stacks_chainstate_via_csv_using_predicate;
use crate::service::Service;
use crate::storage::{
    get_last_block_height_inserted, get_stacks_block_at_block_height, is_stacks_block_present,
    open_readonly_stacks_db_conn,
};

use chainhook_sdk::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification, BitcoinPredicateType,
    ChainhookFullSpecification, FileHook, HookAction, OrdinalOperations,
    StacksChainhookFullSpecification, StacksChainhookNetworkSpecification, StacksPredicate,
    StacksPrintEventBasedPredicate,
};
use chainhook_sdk::hord::db::{
    delete_data_in_hord_db, find_last_block_inserted, find_lazy_block_at_block_height,
    find_watched_satpoint_for_inscription, initialize_hord_db, open_readonly_hord_db_conn,
    open_readonly_hord_db_conn_rocks_db, open_readwrite_hord_db_conn,
    open_readwrite_hord_db_conn_rocks_db, retrieve_satoshi_point_using_lazy_storage,
};
use chainhook_sdk::hord::{
    new_traversals_lazy_cache, retrieve_inscribed_satoshi_points_from_block,
    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data, Storage,
};
use chainhook_sdk::indexer;
use chainhook_sdk::indexer::bitcoin::{
    download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_sdk::observer::BitcoinConfig;
use chainhook_sdk::utils::Context;
use chainhook_types::{
    BitcoinBlockData, BitcoinNetwork, BlockIdentifier, StacksNetwork, TransactionIdentifier,
};
use clap::{Parser, Subcommand};
use ctrlc;
use hiro_system_kit;
use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process;
use std::sync::mpsc::Sender;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum Command {
    /// Generate and test predicates
    #[clap(subcommand)]
    Predicates(PredicatesCommand),
    /// Generate configuration files
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Run a service streaming blocks and evaluating registered predicates
    #[clap(subcommand)]
    Service(ServiceCommand),
    /// Ordinals related subcommands  
    #[clap(subcommand)]
    Hord(HordCommand),
    /// Stacks related subcommands  
    #[clap(subcommand)]
    Stacks(StacksCommand),
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
    /// Check given predicate
    #[clap(name = "check", bin_name = "check")]
    Check(CheckPredicate),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
#[clap(bin_name = "config", aliases = &["config"])]
enum ConfigCommand {
    /// Generate new config
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
    /// Generate a Bitcoin predicate
    #[clap(long = "bitcoin", conflicts_with = "stacks")]
    pub bitcoin: bool,
    /// Generate a Stacks predicate
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

#[derive(Parser, PartialEq, Clone, Debug)]
struct CheckPredicate {
    /// Chainhook spec file to check (json format)
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
    Db(HordDbCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Scan(ScanCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum HordDbCommand {
    /// Rewrite hord db
    #[clap(name = "rewrite", bin_name = "rewrite")]
    Rewrite(UpdateHordDbCommand),
    /// Catch-up hord db
    #[clap(name = "sync", bin_name = "sync")]
    Sync(SyncHordDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropHordDbCommand),
    /// Check integrity
    #[clap(name = "check", bin_name = "check")]
    Check(CheckDbCommand),
    /// Patch DB
    #[clap(name = "patch", bin_name = "patch")]
    Patch(PatchHordDbCommand),
    /// Migrate
    #[clap(name = "migrate", bin_name = "migrate")]
    Migrate(MigrateHordDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum StacksCommand {
    /// Db maintenance related commands
    #[clap(subcommand)]
    Db(StacksDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum StacksDbCommand {
    /// Check integrity
    #[clap(name = "check", bin_name = "check")]
    Check(CheckDbCommand),
    /// Update database using latest Stacks archive file
    #[clap(name = "update", bin_name = "update")]
    Update(UpdateDbCommand),
    /// Retrieve a block from the Stacks db
    #[clap(name = "get", bin_name = "get")]
    GetBlock(GetBlockDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum ScanCommand {
    /// Compute ordinal number of the 1st satoshi of the 1st input of a given transaction
    #[clap(name = "inscriptions", bin_name = "inscriptions")]
    Inscriptions(ScanInscriptionsCommand),
    /// Retrieve all the transfers for a given inscription
    #[clap(name = "transfers", bin_name = "transfers")]
    Transfers(ScanTransfersCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanInscriptionsCommand {
    /// Block height
    pub block_height: u64,
    /// Txid
    pub txid: Option<String>,
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
struct ScanTransfersCommand {
    /// Inscription ID
    pub inscription_id: String,
    /// Block height
    pub block_height: Option<u64>,
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
struct UpdateHordDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Starting block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct SyncHordDbCommand {
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
struct MigrateHordDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct CheckDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct UpdateDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct GetBlockDbCommand {
    /// Block index to retrieve
    #[clap(long = "block-height")]
    pub block_height: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct InitHordDbCommand {
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
        Command::Service(subcmd) => match subcmd {
            ServiceCommand::Start(cmd) => {
                let mut config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                // We disable the API if a predicate was passed, and the --enable-
                if cmd.predicates_paths.len() > 0 && !cmd.start_http_api {
                    config.http_api = PredicatesApi::Off;
                }

                let _ = initialize_hord_db(&config.expected_cache_path(), &ctx);

                let predicates = cmd
                    .predicates_paths
                    .iter()
                    .map(|p| load_predicate_from_path(p))
                    .collect::<Result<Vec<ChainhookFullSpecification>, _>>()?;

                info!(ctx.expect_logger(), "Starting service...",);

                let mut service = Service::new(config, ctx);
                return service.run(predicates, cmd.hord_disabled).await;
            }
        },
        Command::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::fs::File;
                use std::io::Write;
                let config = Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &None)?;
                let config_content = generate_config(&config.network.bitcoin_network);
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

                        networks.insert(StacksNetwork::Testnet, StacksChainhookNetworkSpecification {
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
                                predicate: BitcoinPredicateType::OrdinalsProtocol(
                                    OrdinalOperations::InscriptionFeed,
                                ),
                                expire_after_occurrence: None,
                                action: HookAction::FileAppend(FileHook {
                                    path: "ordinals.txt".into(),
                                }),
                                include_inputs: None,
                                include_outputs: None,
                                include_proof: None,
                                include_witness: None,
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

                        scan_bitcoin_chainstate_via_rpc_using_predicate(
                            &predicate_spec,
                            &config,
                            &ctx,
                        )
                        .await?;
                    }
                    ChainhookFullSpecification::Stacks(predicate) => {
                        let predicate_spec = match predicate
                            .into_selected_network_specification(&config.network.stacks_network)
                        {
                            Ok(predicate) => predicate,
                            Err(e) => {
                                return Err(format!(
                                    "Specification missing for network {:?}: {e}",
                                    config.network.bitcoin_network
                                ));
                            }
                        };
                        // TODO: if a stacks.rocksdb is present, use it.
                        // TODO: update Stacks archive file if required.
                        scan_stacks_chainstate_via_csv_using_predicate(
                            &predicate_spec,
                            &mut config,
                            &ctx,
                        )
                        .await?;
                    }
                }
            }
            PredicatesCommand::Check(cmd) => {
                let config = Config::default(false, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
                let predicate: ChainhookFullSpecification =
                    load_predicate_from_path(&cmd.predicate_path)?;

                match predicate {
                    ChainhookFullSpecification::Bitcoin(predicate) => {
                        let _ = match predicate
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
                    }
                    ChainhookFullSpecification::Stacks(predicate) => {
                        let _ = match predicate
                            .into_selected_network_specification(&config.network.stacks_network)
                        {
                            Ok(predicate) => predicate,
                            Err(e) => {
                                return Err(format!(
                                    "Specification missing for network {:?}: {e}",
                                    config.network.bitcoin_network
                                ));
                            }
                        };
                    }
                }
                println!("✔️ Predicate {} successfully checked", cmd.predicate_path);
            }
        },
        Command::Hord(HordCommand::Scan(subcmd)) => match subcmd {
            ScanCommand::Inscriptions(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let tip_height = {
                    let hord_db_conn =
                        open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)
                            .unwrap();
                    find_last_block_inserted(&hord_db_conn) as u64
                };
                if cmd.block_height > tip_height {
                    crate::hord::perform_hord_db_update(
                        tip_height,
                        cmd.block_height,
                        &config.get_hord_config(),
                        &config,
                        None,
                        &ctx,
                    )
                    .await?;
                }

                match cmd.txid {
                    Some(txid) => {
                        // let global_block_cache = HashMap::new();
                        let block_identifier = BlockIdentifier {
                            index: cmd.block_height,
                            hash: "".into(),
                        };

                        let transaction_identifier = TransactionIdentifier { hash: txid.clone() };
                        let traversals_cache = new_traversals_lazy_cache(1024);
                        let traversal = retrieve_satoshi_point_using_lazy_storage(
                            &config.expected_cache_path(),
                            &block_identifier,
                            &transaction_identifier,
                            0,
                            0,
                            Arc::new(traversals_cache),
                            &ctx,
                        )?;
                        info!(
                            ctx.expect_logger(),
                            "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times.",
                            traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                        );
                    }
                    None => {
                        let event_observer_config = config.get_event_observer_config();
                        let bitcoin_config = event_observer_config.get_bitcoin_config();
                        let block =
                            fetch_and_standardize_block(cmd.block_height, &bitcoin_config, &ctx)
                                .await?;
                        let traversals_cache = Arc::new(new_traversals_lazy_cache(1024));

                        let _traversals = retrieve_inscribed_satoshi_points_from_block(
                            &block,
                            None,
                            event_observer_config.hord_config.as_ref().unwrap(),
                            &traversals_cache,
                            &ctx,
                        );
                        // info!(
                        //     ctx.expect_logger(),
                        //     "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times.",
                        //     traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                        // );
                    }
                }
            }
            ScanCommand::Transfers(cmd) => {
                let config =
                    Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let inscriptions_db_conn =
                    open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                let blocks_db_conn =
                    open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                let tip_height = find_last_block_inserted(&blocks_db_conn) as u64;
                let _end_at = match cmd.block_height {
                    Some(block_height) if block_height > tip_height => {
                        crate::hord::perform_hord_db_update(
                            tip_height,
                            block_height,
                            &config.get_hord_config(),
                            &config,
                            None,
                            &ctx,
                        )
                        .await?;
                        block_height
                    }
                    _ => tip_height,
                };

                let (_start_at_height, watched_satpoint) = find_watched_satpoint_for_inscription(
                    &cmd.inscription_id,
                    &inscriptions_db_conn,
                )?;
                let mut cache = BTreeMap::new();
                cache.insert(
                    watched_satpoint.get_genesis_satpoint(),
                    vec![watched_satpoint],
                );
                let mut storage = Storage::Memory(cache);

                let mut seq = vec![
                    784787, 781409, 781069, 781000, 780978, 780060, 777543, 777542,
                ];

                // rm -rf /hirosystems/chainhook-nodedata/hord.*
                // cd /hirosystems/chainhook-node/ && rm data/hord.rocksdb/LOCK && chainhook hord db drop 767430 787985 --config-path ./config/config.toml

                seq.reverse();

                let bitcoin_config = config.get_event_observer_config().get_bitcoin_config();

                for cursor in seq {
                    //start_at_height..=end_at {
                    match storage {
                        Storage::Memory(ref c) => {
                            info!(ctx.expect_logger(), "#{} -> {}", cursor, c.len());
                        }
                        _ => unreachable!(),
                    }

                    let mut block =
                        fetch_and_standardize_block(cursor, &bitcoin_config, &ctx).await?;

                    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
                        &mut block,
                        &mut storage,
                        &ctx,
                    )?;
                }
            }
        },
        Command::Hord(HordCommand::Db(subcmd)) => match subcmd {
            HordDbCommand::Sync(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                if let Some((start_block, end_block)) =
                    crate::hord::should_sync_hord_db(&config, &ctx)?
                {
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
                    crate::hord::perform_hord_db_update(
                        start_block,
                        end_block,
                        &config.get_hord_config(),
                        &config,
                        None,
                        &ctx,
                    )
                    .await?;
                } else {
                    info!(ctx.expect_logger(), "Database hord up to date");
                }
            }
            HordDbCommand::Rewrite(cmd) => {
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
                crate::hord::perform_hord_db_update(
                    cmd.start_block,
                    cmd.end_block,
                    &config.get_hord_config(),
                    &config,
                    None,
                    &ctx,
                )
                .await?;
            }
            HordDbCommand::Check(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                // Delete data, if any
                {
                    let blocks_db_rw =
                        open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                    let mut missing_blocks = vec![];
                    for i in 1..=790000 {
                        if find_lazy_block_at_block_height(i, 3, false, &blocks_db_rw, &ctx)
                            .is_none()
                        {
                            println!("Missing block {i}");
                            missing_blocks.push(i);
                        }
                    }
                    println!("{:?}", missing_blocks);
                }
            }
            HordDbCommand::Drop(cmd) => {
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
            HordDbCommand::Patch(_cmd) => {
                unimplemented!()
            }
            HordDbCommand::Migrate(_cmd) => {
                unimplemented!()
            }
        },
        Command::Stacks(subcmd) => match subcmd {
            StacksCommand::Db(StacksDbCommand::GetBlock(cmd)) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let stacks_db = open_readonly_stacks_db_conn(&config.expected_cache_path(), &ctx)
                    .expect("unable to read stacks_db");
                match get_stacks_block_at_block_height(cmd.block_height, true, 3, &stacks_db) {
                    Ok(Some(block)) => {
                        info!(ctx.expect_logger(), "{}", json!(block));
                    }
                    Ok(None) => {
                        warn!(
                            ctx.expect_logger(),
                            "Block {} not present in database", cmd.block_height
                        );
                    }
                    Err(e) => {
                        error!(ctx.expect_logger(), "{e}",);
                    }
                }
            }
            StacksCommand::Db(StacksDbCommand::Update(cmd)) => {
                let mut config = Config::default(false, false, false, &cmd.config_path)?;
                download_stacks_dataset_if_required(&mut config, &ctx).await;
            }
            StacksCommand::Db(StacksDbCommand::Check(cmd)) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                // Delete data, if any
                {
                    let stacks_db =
                        open_readonly_stacks_db_conn(&config.expected_cache_path(), &ctx)?;
                    let mut missing_blocks = vec![];
                    let mut min = 0;
                    let mut max = 0;
                    if let Some(tip) = get_last_block_height_inserted(&stacks_db, &ctx) {
                        min = 1;
                        max = tip;
                        for index in 1..=tip {
                            let block_identifier = BlockIdentifier {
                                index,
                                hash: "".into(),
                            };
                            if !is_stacks_block_present(&block_identifier, 3, &stacks_db) {
                                missing_blocks.push(index);
                            }
                        }
                    }
                    if missing_blocks.is_empty() {
                        info!(
                            ctx.expect_logger(),
                            "Stacks db successfully checked ({min}, {max})"
                        );
                    } else {
                        warn!(
                            ctx.expect_logger(),
                            "Stacks db includes {} missing entries ({min}, {max}): {:?}",
                            missing_blocks.len(),
                            missing_blocks
                        );
                    }
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

pub async fn fetch_and_standardize_block(
    block_height: u64,
    bitcoin_config: &BitcoinConfig,
    ctx: &Context,
) -> Result<BitcoinBlockData, String> {
    let block_hash = retrieve_block_hash_with_retry(&block_height, &bitcoin_config, &ctx).await?;
    let block_breakdown =
        download_and_parse_block_with_retry(&block_hash, &bitcoin_config, &ctx).await?;

    indexer::bitcoin::standardize_bitcoin_block(block_breakdown, &bitcoin_config.network, &ctx)
        .map_err(|(e, _)| e)
}
