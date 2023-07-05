use crate::config::generator::generate_config;
use crate::config::{Config, PredicatesApi};
use crate::service::Service;

use crate::db::{
    delete_data_in_hord_db, find_last_block_inserted, find_lazy_block_at_block_height,
    find_watched_satpoint_for_inscription, initialize_hord_db, open_readonly_hord_db_conn,
    open_readonly_hord_db_conn_rocks_db, open_readwrite_hord_db_conn,
    open_readwrite_hord_db_conn_rocks_db, retrieve_satoshi_point_using_lazy_storage,
};
use crate::hord::{
    new_traversals_lazy_cache, retrieve_inscribed_satoshi_points_from_block,
    update_storage_and_augment_bitcoin_block_with_inscription_transfer_data, Storage,
};
use chainhook_sdk::chainhooks::types::ChainhookFullSpecification;
use chainhook_sdk::indexer;
use chainhook_sdk::indexer::bitcoin::{
    download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_sdk::observer::BitcoinConfig;
use chainhook_sdk::utils::Context;
use chainhook_types::{BitcoinBlockData, BlockIdentifier, TransactionIdentifier};
use clap::{Parser, Subcommand};
use hiro_system_kit;
use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum Command {
    /// Generate configuration files
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Run a service streaming blocks and evaluating registered predicates
    #[clap(subcommand)]
    Service(ServiceCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Db(HordDbCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Scan(ScanCommand),
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
        Command::Scan(ScanCommand::Inscriptions(cmd)) => {
            let config = Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

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
                    let hord_config = config.get_hord_config();
                    let bitcoin_config = event_observer_config.get_bitcoin_config();
                    let block =
                        fetch_and_standardize_block(cmd.block_height, &bitcoin_config, &ctx)
                            .await?;
                    let traversals_cache = Arc::new(new_traversals_lazy_cache(1024));

                    let _traversals = retrieve_inscribed_satoshi_points_from_block(
                        &block,
                        None,
                        &hord_config,
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
        Command::Scan(ScanCommand::Transfers(cmd)) => {
            let config = Config::default(cmd.devnet, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

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

            let (_start_at_height, watched_satpoint) =
                find_watched_satpoint_for_inscription(&cmd.inscription_id, &inscriptions_db_conn)?;
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

                let mut block = fetch_and_standardize_block(cursor, &bitcoin_config, &ctx).await?;

                update_storage_and_augment_bitcoin_block_with_inscription_transfer_data(
                    &mut block,
                    &mut storage,
                    &ctx,
                )?;
            }
        }
        Command::Db(HordDbCommand::Sync(cmd)) => {
            let config = Config::default(false, false, false, &cmd.config_path)?;
            if let Some((start_block, end_block)) = crate::hord::should_sync_hord_db(&config, &ctx)?
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
        Command::Db(HordDbCommand::Rewrite(cmd)) => {
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
        Command::Db(HordDbCommand::Check(cmd)) => {
            let config = Config::default(false, false, false, &cmd.config_path)?;
            // Delete data, if any
            {
                let blocks_db_rw =
                    open_readwrite_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;

                let mut missing_blocks = vec![];
                for i in 1..=790000 {
                    if find_lazy_block_at_block_height(i, 3, false, &blocks_db_rw, &ctx).is_none() {
                        println!("Missing block {i}");
                        missing_blocks.push(i);
                    }
                }
                println!("{:?}", missing_blocks);
            }
        }
        Command::Db(HordDbCommand::Drop(cmd)) => {
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
        } // HordDbCommand::Patch(_cmd) => {
          //     unimplemented!()
          // }
          // HordDbCommand::Migrate(_cmd) => {
          //     unimplemented!()
          // }
    }
    Ok(())
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
