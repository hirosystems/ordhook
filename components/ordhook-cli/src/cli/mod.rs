use crate::config::file::ConfigFile;
use crate::config::generator::generate_config;
use clap::{Parser, Subcommand};
use hiro_system_kit;
use ordhook::chainhook_sdk::bitcoincore_rpc::{Auth, Client, RpcApi};
use ordhook::chainhook_sdk::chainhooks::types::{BitcoinChainhookSpecification, HttpHook};
use ordhook::chainhook_sdk::chainhooks::types::{
    BitcoinPredicateType, ChainhookFullSpecification, HookAction, OrdinalOperations,
};
use ordhook::chainhook_sdk::indexer::bitcoin::{
    build_http_client, download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use ordhook::chainhook_sdk::observer::BitcoinConfig;
use ordhook::chainhook_sdk::types::{BitcoinBlockData, TransactionIdentifier};
use ordhook::chainhook_sdk::utils::BlockHeights;
use ordhook::chainhook_sdk::utils::Context;
use ordhook::config::Config;
use ordhook::core::new_traversals_lazy_cache;
use ordhook::core::pipeline::download_and_pipeline_blocks;
use ordhook::core::pipeline::processors::block_archiving::start_block_archiving_processor;
use ordhook::core::pipeline::processors::start_inscription_indexing_processor;
use ordhook::core::protocol::inscription_parsing::parse_inscriptions_and_standardize_block;
use ordhook::core::protocol::satoshi_numbering::compute_satoshi_number;
use ordhook::db::{
    delete_data_in_ordhook_db, find_all_inscription_transfers, find_all_inscriptions_in_block,
    find_all_transfers_in_block, find_block_bytes_at_block_height, find_inscription_with_id,
    find_last_block_inserted, find_latest_inscription_block_height, find_missing_blocks,
    get_default_ordhook_db_file_path, initialize_ordhook_db, open_ordhook_db_conn_rocks_db_loop,
    open_readonly_ordhook_db_conn, open_readonly_ordhook_db_conn_rocks_db,
    open_readwrite_ordhook_db_conn, BlockBytesCursor,
};
use ordhook::download::download_ordinals_dataset_if_required;
use ordhook::hex;
use ordhook::scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate;
use ordhook::service::observers::initialize_observers_db;
use ordhook::service::{start_observer_forwarding, Service};
use reqwest::Client as HttpClient;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum Command {
    /// Generate a new configuration file
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Scan the Bitcoin chain for inscriptions
    #[clap(subcommand)]
    Scan(ScanCommand),
    /// Stream Bitcoin blocks and index ordinals inscriptions and transfers
    #[clap(subcommand)]
    Service(ServiceCommand),
    /// Perform maintenance operations on local databases
    #[clap(subcommand)]
    Db(OrdhookDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum ScanCommand {
    /// Scans blocks for Ordinals activities
    #[clap(name = "blocks", bin_name = "blocks")]
    Blocks(ScanBlocksCommand),
    /// Retrieve activities for a given inscription
    #[clap(name = "inscription", bin_name = "inscription")]
    Inscription(ScanInscriptionCommand),
    /// Retrieve activities for a given inscription
    #[clap(name = "transaction", bin_name = "transaction")]
    Transaction(ScanTransactionCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanBlocksCommand {
    /// Interval of blocks (--interval 767430:800000)
    #[clap(long = "interval", conflicts_with = "blocks")]
    pub blocks_interval: Option<String>,
    /// List of blocks (--blocks 767430,767431,767433,800000)
    #[clap(long = "blocks", conflicts_with = "interval")]
    pub blocks: Option<String>,
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
    /// HTTP Post activity to a URL
    #[clap(long = "post-to")]
    pub post_to: Option<String>,
    /// HTTP Auth token
    #[clap(long = "auth-token")]
    pub auth_token: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanInscriptionCommand {
    /// Inscription Id
    pub inscription_id: String,
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanTransactionCommand {
    /// Block Hash
    pub block_height: u64,
    /// Inscription Id
    pub transaction_id: String,
    /// Input index
    pub input_index: usize,
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum RepairCommand {
    /// Rewrite blocks data in hord.rocksdb
    #[clap(name = "blocks", bin_name = "blocks")]
    Blocks(RepairStorageCommand),
    /// Rewrite inscriptions data in hord.sqlite
    #[clap(name = "inscriptions", bin_name = "inscriptions")]
    Inscriptions(RepairStorageCommand),
    /// Rewrite transfers data in hord.sqlite
    #[clap(name = "transfers", bin_name = "transfers")]
    Transfers(RepairStorageCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct RepairStorageCommand {
    /// Interval of blocks (--interval 767430:800000)
    #[clap(long = "interval", conflicts_with = "blocks")]
    pub blocks_interval: Option<String>,
    /// List of blocks (--blocks 767430,767431,767433,800000)
    #[clap(long = "blocks", conflicts_with = "interval")]
    pub blocks: Option<String>,
    /// Network threads
    #[clap(long = "network-threads")]
    pub network_threads: Option<usize>,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
    /// Cascade to observers
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub repair_observers: Option<bool>,
    /// Display debug logs
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub debug: Option<bool>,
}

impl RepairStorageCommand {
    pub fn get_blocks(&self) -> Vec<u64> {
        let blocks = match (&self.blocks_interval, &self.blocks) {
            (Some(interval), None) => {
                let blocks = interval.split(':').collect::<Vec<_>>();
                let start_block: u64 = blocks
                    .first()
                    .expect("unable to get start_block")
                    .parse::<u64>()
                    .expect("unable to parse start_block");
                let end_block: u64 = blocks
                    .get(1)
                    .expect("unable to get end_block")
                    .parse::<u64>()
                    .expect("unable to parse end_block");
                BlockHeights::BlockRange(start_block, end_block).get_sorted_entries()
            }
            (None, Some(blocks)) => {
                let blocks = blocks
                    .split(',')
                    .map(|b| b.parse::<u64>().expect("unable to parse block"))
                    .collect::<Vec<_>>();
                BlockHeights::Blocks(blocks).get_sorted_entries()
            }
            _ => unreachable!(),
        };
        blocks.into()
    }
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
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
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
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
    /// Specify relative path of the chainhooks (yaml format) to evaluate
    #[clap(long = "post-to")]
    pub post_to: Vec<String>,
    /// Block height where ordhook will start posting Ordinals activities
    #[clap(long = "start-at-block")]
    pub start_at_block: Option<u64>,
    /// HTTP Auth token
    #[clap(long = "auth-token")]
    pub auth_token: Option<String>,
    /// Check blocks integrity
    #[clap(long = "check-blocks-integrity")]
    pub block_integrity_check: bool,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum OrdhookDbCommand {
    /// Initialize a new ordhook db
    #[clap(name = "new", bin_name = "new")]
    New(SyncOrdhookDbCommand),
    /// Catch-up ordhook db
    #[clap(name = "sync", bin_name = "sync")]
    Sync(SyncOrdhookDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropOrdhookDbCommand),
    /// Check integrity
    #[clap(name = "check", bin_name = "check")]
    Check(CheckDbCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Repair(RepairCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum TestCommand {
    /// Compute ordinal number of the 1st satoshi of the 1st input of a given transaction
    #[clap(name = "inscriptions", bin_name = "inscriptions")]
    Inscriptions(ScanInscriptionsCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanInscriptionsCommand {
    /// Block height
    pub block_height: u64,
    /// Txid
    pub txid: Option<String>,
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanTransfersCommand {
    /// Inscription ID
    pub inscription_id: String,
    /// Block height
    pub block_height: Option<u64>,
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct UpdateOrdhookDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
    /// Transfers only
    pub transfers_only: Option<bool>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct SyncOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct DropOrdhookDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct PatchOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct MigrateOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct CheckDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
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

    if let Err(e) = hiro_system_kit::nestable_block_on(handle_command(opts, &ctx)) {
        error!(ctx.expect_logger(), "{e}");
        std::thread::sleep(std::time::Duration::from_millis(500));
        process::exit(1);
    }
}

async fn handle_command(opts: Opts, ctx: &Context) -> Result<(), String> {
    match opts.command {
        Command::Scan(ScanCommand::Blocks(cmd)) => {
            let config: Config =
                ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
            // Download dataset if required
            // If console:
            // - Replay based on SQLite queries
            // If post-to:
            // - Replay that requires connection to bitcoind
            let block_heights = parse_blocks_heights_spec(&cmd.blocks_interval, &cmd.blocks);
            let mut block_range = block_heights.get_sorted_entries();
            if let Some(ref post_to) = cmd.post_to {
                info!(ctx.expect_logger(), "A fully synchronized bitcoind node is required for retrieving inscriptions content.");
                info!(
                    ctx.expect_logger(),
                    "Checking {}...", config.network.bitcoind_rpc_url
                );
                let tip = check_bitcoind_connection(&config).await?;
                if let Some(highest_desired) = block_range.pop_back() {
                    if tip < highest_desired {
                        error!(ctx.expect_logger(), "Unable to scan desired block range: underlying bitcoind synchronized until block #{} ", tip);
                    } else {
                        info!(ctx.expect_logger(), "Starting scan");
                    }
                    block_range.push_back(highest_desired);
                }

                let predicate_spec = build_predicate_from_cli(
                    &config,
                    post_to,
                    Some(&block_heights),
                    None,
                    cmd.auth_token,
                )?;

                let _ = initialize_observers_db(&config.expected_cache_path(), ctx);

                scan_bitcoin_chainstate_via_rpc_using_predicate(
                    &predicate_spec,
                    &config,
                    None,
                    ctx,
                )
                .await?;
            } else {
                let _ = download_ordinals_dataset_if_required(&config, ctx).await;
                let mut total_inscriptions = 0;
                let mut total_transfers = 0;

                let inscriptions_db_conn =
                    initialize_ordhook_db(&config.expected_cache_path(), ctx);
                while let Some(block_height) = block_range.pop_front() {
                    let inscriptions =
                        find_all_inscriptions_in_block(&block_height, &inscriptions_db_conn, ctx);
                    let locations =
                        find_all_transfers_in_block(&block_height, &inscriptions_db_conn, ctx);

                    let mut total_transfers_in_block = 0;

                    for (_, inscription) in inscriptions.iter() {
                        println!("Inscription {} revealed at block #{} (inscription_number {}, ordinal_number {})", inscription.get_inscription_id(), block_height, inscription.inscription_number.jubilee, inscription.ordinal_number);
                        if let Some(transfers) = locations.get(&inscription.ordinal_number) {
                            for t in transfers.iter().skip(1) {
                                total_transfers_in_block += 1;
                                println!(
                                    "\t→ Transferred in transaction {}",
                                    t.transaction_identifier_location.hash
                                );
                            }
                        }
                    }
                    for (inscription_id, transfers) in locations.iter() {
                        println!("Inscription {}", inscription_id);
                        for t in transfers.iter() {
                            total_transfers_in_block += 1;
                            println!(
                                "\t→ Transferred in transaction {}",
                                t.transaction_identifier_location.hash
                            );
                        }
                    }
                    if total_transfers_in_block > 0 && !inscriptions.is_empty() {
                        println!(
                            "Inscriptions revealed: {}, inscriptions transferred: {total_transfers_in_block}",
                            inscriptions.len()
                        );
                        println!("-----");
                    }

                    total_inscriptions += inscriptions.len();
                    total_transfers += total_transfers_in_block;
                }
                if total_transfers == 0 && total_inscriptions == 0 {
                    let db_file_path =
                        get_default_ordhook_db_file_path(&config.expected_cache_path());
                    warn!(ctx.expect_logger(), "No data available. Check the validity of the range being scanned and the validity of your local database {}", db_file_path.display());
                }
            }
        }
        Command::Scan(ScanCommand::Inscription(cmd)) => {
            let config: Config =
                ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

            let _ = download_ordinals_dataset_if_required(&config, ctx).await;

            let inscriptions_db_conn =
                open_readonly_ordhook_db_conn(&config.expected_cache_path(), ctx)?;
            let (inscription, block_height) =
                match find_inscription_with_id(&cmd.inscription_id, &inscriptions_db_conn, ctx)? {
                    Some(entry) => entry,
                    _ => {
                        return Err(format!(
                            "unable to retrieve inscription {}",
                            cmd.inscription_id
                        ));
                    }
                };
            println!(
                "Inscription {} revealed at block #{} (inscription_number {}, ordinal_number {})",
                inscription.get_inscription_id(),
                block_height,
                inscription.inscription_number.jubilee,
                inscription.ordinal_number
            );
            let transfers = find_all_inscription_transfers(
                &inscription.get_inscription_id(),
                &inscriptions_db_conn,
                ctx,
            );
            for (transfer, block_height) in transfers.iter().skip(1) {
                println!(
                    "\t→ Transferred in transaction {} (block #{block_height})",
                    transfer.transaction_identifier_location.hash
                );
            }
            println!("Number of transfers: {}", transfers.len() - 1);
        }
        Command::Scan(ScanCommand::Transaction(cmd)) => {
            let config: Config =
                ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
            let http_client = build_http_client();
            let block = fetch_and_standardize_block(
                &http_client,
                cmd.block_height,
                &config.get_event_observer_config().get_bitcoin_config(),
                ctx,
            )
            .await?;
            let transaction_identifier = TransactionIdentifier::new(&cmd.transaction_id);
            let cache = new_traversals_lazy_cache(100);
            let (res, _, mut back_trace) = compute_satoshi_number(
                &config.get_ordhook_config().db_path,
                &block.block_identifier,
                &transaction_identifier,
                cmd.input_index,
                0,
                &Arc::new(cache),
                config.resources.ulimit,
                config.resources.memory_available,
                true,
                ctx,
            )?;
            back_trace.reverse();
            for (block_height, tx, index) in back_trace.iter() {
                println!("{}\t{}:{}", block_height, hex::encode(tx), index);
            }
            println!("{:?}", res);
        }
        Command::Service(subcmd) => match subcmd {
            ServiceCommand::Start(cmd) => {
                let maintenance_enabled =
                    std::env::var("ORDHOOK_MAINTENANCE").unwrap_or("0".into());
                if maintenance_enabled.eq("1") {
                    info!(ctx.expect_logger(), "Entering maintenance mode (default duration = 7 days). Unset ORDHOOK_MAINTENANCE and reboot to resume operations");
                    sleep(Duration::from_secs(3600 * 24 * 7))
                }

                let config =
                    ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let _ = initialize_ordhook_db(&config.expected_cache_path(), ctx);

                let inscriptions_db_conn =
                    open_readonly_ordhook_db_conn(&config.expected_cache_path(), ctx)?;

                let last_known_block =
                    find_latest_inscription_block_height(&inscriptions_db_conn, ctx)?;
                if last_known_block.is_none() {
                    open_ordhook_db_conn_rocks_db_loop(
                        true,
                        &config.expected_cache_path(),
                        config.resources.ulimit,
                        config.resources.memory_available,
                        ctx,
                    );
                }

                let ordhook_config = config.get_ordhook_config();

                info!(ctx.expect_logger(), "Starting service...",);

                let start_block = match cmd.start_at_block {
                    Some(entry) => entry,
                    None => match last_known_block {
                        Some(entry) => entry,
                        None => {
                            warn!(
                                ctx.expect_logger(),
                                "Inscription ingestion will start at block #{}",
                                ordhook_config.first_inscription_height
                            );
                            ordhook_config.first_inscription_height
                        }
                    },
                };

                let mut predicates = vec![];

                for post_to in cmd.post_to.iter() {
                    let predicate = build_predicate_from_cli(
                        &config,
                        post_to,
                        None,
                        Some(start_block),
                        cmd.auth_token.clone(),
                    )?;
                    predicates.push(predicate);
                }

                let mut service = Service::new(config, ctx.clone());
                return service
                    .run(predicates, None, cmd.block_integrity_check)
                    .await;
            }
        },
        Command::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::fs::File;
                use std::io::Write;
                let config = ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &None)?;
                let config_content = generate_config(&config.network.bitcoin_network);
                let mut file_path = PathBuf::new();
                file_path.push("Ordhook.toml");
                let mut file = File::create(&file_path)
                    .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
                file.write_all(config_content.as_bytes())
                    .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
                println!("Created file Ordhook.toml");
            }
        },
        Command::Db(OrdhookDbCommand::New(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
            initialize_ordhook_db(&config.expected_cache_path(), ctx);
            open_ordhook_db_conn_rocks_db_loop(
                true,
                &config.expected_cache_path(),
                config.resources.ulimit,
                config.resources.memory_available,
                ctx,
            );
        }
        Command::Db(OrdhookDbCommand::Sync(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
            initialize_ordhook_db(&config.expected_cache_path(), ctx);
            let service = Service::new(config, ctx.clone());
            service.update_state(None).await?;
        }
        Command::Db(OrdhookDbCommand::Repair(subcmd)) => match subcmd {
            RepairCommand::Blocks(cmd) => {
                let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
                let mut ordhook_config = config.get_ordhook_config();
                if let Some(network_threads) = cmd.network_threads {
                    ordhook_config.resources.bitcoind_rpc_threads = network_threads;
                }
                let blocks = cmd.get_blocks();
                let block_ingestion_processor =
                    start_block_archiving_processor(&config, ctx, false, None);
                download_and_pipeline_blocks(
                    &config,
                    blocks,
                    ordhook_config.first_inscription_height,
                    Some(&block_ingestion_processor),
                    10_000,
                    ctx,
                )
                .await?;
                if let Some(true) = cmd.debug {
                    let blocks_db = open_ordhook_db_conn_rocks_db_loop(
                        false,
                        &config.get_ordhook_config().db_path,
                        config.resources.ulimit,
                        config.resources.memory_available,
                        ctx,
                    );
                    for i in cmd.get_blocks().into_iter() {
                        let block_bytes =
                            find_block_bytes_at_block_height(i as u32, 10, &blocks_db, ctx)
                                .expect("unable to retrieve block {i}");
                        let block = BlockBytesCursor::new(&block_bytes);
                        info!(ctx.expect_logger(), "--------------------");
                        info!(ctx.expect_logger(), "Block: {i}");
                        for tx in block.iter_tx() {
                            info!(ctx.expect_logger(), "Tx: {}", ordhook::hex::encode(tx.txid));
                        }
                    }
                }
            }
            RepairCommand::Inscriptions(cmd) => {
                let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
                let mut ordhook_config = config.get_ordhook_config();
                if let Some(network_threads) = cmd.network_threads {
                    ordhook_config.resources.bitcoind_rpc_threads = network_threads;
                }
                let block_post_processor = match cmd.repair_observers {
                    Some(true) => {
                        let tx_replayer =
                            start_observer_forwarding(&config.get_event_observer_config(), ctx);
                        Some(tx_replayer)
                    }
                    _ => None,
                };
                let blocks = cmd.get_blocks();
                let inscription_indexing_processor =
                    start_inscription_indexing_processor(&config, ctx, block_post_processor);

                download_and_pipeline_blocks(
                    &config,
                    blocks,
                    ordhook_config.first_inscription_height,
                    Some(&inscription_indexing_processor),
                    10_000,
                    ctx,
                )
                .await?;
            }
            RepairCommand::Transfers(cmd) => {
                let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
                let block_post_processor = match cmd.repair_observers {
                    Some(true) => {
                        let tx_replayer =
                            start_observer_forwarding(&config.get_event_observer_config(), ctx);
                        Some(tx_replayer)
                    }
                    _ => None,
                };
                let service = Service::new(config, ctx.clone());
                let blocks = cmd.get_blocks();
                info!(
                    ctx.expect_logger(),
                    "Re-indexing transfers for {} blocks",
                    blocks.len()
                );
                for block in blocks.into_iter() {
                    service
                        .replay_transfers(vec![block], block_post_processor.clone())
                        .await?;
                }
            }
        },
        Command::Db(OrdhookDbCommand::Check(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
            {
                let blocks_db = open_readonly_ordhook_db_conn_rocks_db(
                    &config.expected_cache_path(),
                    config.resources.ulimit,
                    config.resources.memory_available,
                    ctx,
                )?;
                let tip = find_last_block_inserted(&blocks_db);
                println!("Tip: {}", tip);
                let missing_blocks = find_missing_blocks(&blocks_db, 1, tip, ctx);
                println!("{:?}", missing_blocks);
            }
        }
        Command::Db(OrdhookDbCommand::Drop(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path)?;
            let blocks_db = open_ordhook_db_conn_rocks_db_loop(
                true,
                &config.expected_cache_path(),
                config.resources.ulimit,
                config.resources.memory_available,
                ctx,
            );
            let inscriptions_db_conn_rw =
                open_readwrite_ordhook_db_conn(&config.expected_cache_path(), ctx)?;

            delete_data_in_ordhook_db(
                cmd.start_block,
                cmd.end_block,
                &blocks_db,
                &inscriptions_db_conn_rw,
                ctx,
            )?;
            info!(
                ctx.expect_logger(),
                "Cleaning ordhook_db: {} blocks dropped",
                cmd.end_block - cmd.start_block + 1
            );
        }
    }
    Ok(())
}

pub fn load_predicate_from_path(
    predicate_path: &str,
) -> Result<ChainhookFullSpecification, String> {
    let file = std::fs::File::open(predicate_path)
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
    http_client: &HttpClient,
    block_height: u64,
    bitcoin_config: &BitcoinConfig,
    ctx: &Context,
) -> Result<BitcoinBlockData, String> {
    let block_hash =
        retrieve_block_hash_with_retry(http_client, &block_height, bitcoin_config, ctx).await?;
    let block_breakdown =
        download_and_parse_block_with_retry(http_client, &block_hash, bitcoin_config, ctx).await?;

    parse_inscriptions_and_standardize_block(block_breakdown, &bitcoin_config.network, ctx)
        .map_err(|(e, _)| e)
}

pub fn build_predicate_from_cli(
    config: &Config,
    post_to: &str,
    block_heights: Option<&BlockHeights>,
    start_block: Option<u64>,
    auth_token: Option<String>,
) -> Result<BitcoinChainhookSpecification, String> {
    // Retrieve last block height known, and display it
    let (start_block, end_block, blocks) = match (start_block, block_heights) {
        (None, Some(BlockHeights::BlockRange(start, end))) => (Some(*start), Some(*end), None),
        (None, Some(BlockHeights::Blocks(blocks))) => (None, None, Some(blocks.clone())),
        (Some(start), None) => (Some(start), None, None),
        _ => unreachable!(),
    };
    let predicate = BitcoinChainhookSpecification {
        network: config.network.bitcoin_network.clone(),
        uuid: post_to.to_string(),
        owner_uuid: None,
        name: post_to.to_string(),
        version: 1,
        start_block,
        end_block,
        blocks,
        expire_after_occurrence: None,
        include_proof: false,
        include_inputs: false,
        include_outputs: false,
        include_witness: false,
        expired_at: None,
        enabled: true,
        predicate: BitcoinPredicateType::OrdinalsProtocol(OrdinalOperations::InscriptionFeed),
        action: HookAction::HttpPost(HttpHook {
            url: post_to.to_string(),
            authorization_header: format!("Bearer {}", auth_token.unwrap_or("".to_string())),
        }),
    };

    Ok(predicate)
}

pub async fn check_bitcoind_connection(config: &Config) -> Result<u64, String> {
    let auth = Auth::UserPass(
        config.network.bitcoind_rpc_username.clone(),
        config.network.bitcoind_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoind_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("unable to connect to bitcoind: {}", message));
        }
    };

    let end_block = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks,
        Err(e) => {
            return Err(format!("unable to connect to bitcoind: {}", e));
        }
    };

    Ok(end_block)
}

fn parse_blocks_heights_spec(
    blocks_interval: &Option<String>,
    blocks: &Option<String>,
) -> BlockHeights {
    let blocks = match (blocks_interval, blocks) {
        (Some(interval), None) => {
            let blocks = interval.split(':').collect::<Vec<_>>();
            let start_block: u64 = blocks
                .first()
                .expect("unable to get start_block")
                .parse::<u64>()
                .expect("unable to parse start_block");
            let end_block: u64 = blocks
                .get(1)
                .expect("unable to get end_block")
                .parse::<u64>()
                .expect("unable to parse end_block");
            BlockHeights::BlockRange(start_block, end_block)
        }
        (None, Some(blocks)) => {
            let blocks = blocks
                .split(',')
                .map(|b| b.parse::<u64>().expect("unable to parse block"))
                .collect::<Vec<_>>();
            BlockHeights::Blocks(blocks)
        }
        _ => unreachable!(),
    };
    blocks
}
