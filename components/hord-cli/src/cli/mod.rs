use crate::archive::download_ordinals_dataset_if_required;
use crate::config::generator::generate_config;
use crate::config::Config;
use crate::core::pipeline::download_and_pipeline_blocks;
use crate::core::pipeline::processors::block_ingestion::start_block_ingestion_processor;
use crate::core::pipeline::processors::start_inscription_indexing_processor;
use crate::core::{self};
use crate::scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate;
use crate::service::Service;

use crate::db::{
    delete_data_in_hord_db, find_all_inscription_transfers, find_all_inscriptions_in_block,
    find_all_transfers_in_block, find_inscription_with_id, find_last_block_inserted,
    find_latest_inscription_block_height, find_lazy_block_at_block_height, initialize_hord_db,
    open_readonly_hord_db_conn, open_readonly_hord_db_conn_rocks_db, open_readwrite_hord_db_conn,
    open_readwrite_hord_db_conn_rocks_db,
};
use chainhook_sdk::bitcoincore_rpc::{Auth, Client, RpcApi};
use chainhook_sdk::chainhooks::types::HttpHook;
use chainhook_sdk::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification, BitcoinPredicateType,
    ChainhookFullSpecification, HookAction, OrdinalOperations,
};
use chainhook_sdk::indexer::bitcoin::{
    download_and_parse_block_with_retry, retrieve_block_hash_with_retry,
};
use chainhook_sdk::observer::BitcoinConfig;
use chainhook_sdk::types::BitcoinBlockData;
use chainhook_sdk::utils::Context;
use clap::{Parser, Subcommand};
use hiro_system_kit;
use reqwest::Client as HttpClient;
use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::process;

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
    Db(HordDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum ScanCommand {
    /// Scans blocks for Ordinals activities
    #[clap(name = "blocks", bin_name = "blocks")]
    Blocks(ScanBlocksCommand),
    /// Retrieve activities for a given inscription
    #[clap(name = "inscription", bin_name = "inscription")]
    Inscription(ScanInscriptionCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct ScanBlocksCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
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
    Transfers(RepairTransfersCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct RepairStorageCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Network threads
    pub network_threads: usize,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct RepairTransfersCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
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
    /// Block height where hord will start posting Ordinals activities
    #[clap(long = "start-at-block")]
    pub start_at_block: Option<u64>,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum HordDbCommand {
    /// Catch-up hord db
    #[clap(name = "sync", bin_name = "sync")]
    Sync(SyncHordDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropHordDbCommand),
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
struct UpdateHordDbCommand {
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
struct SyncHordDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct DropHordDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
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

    match hiro_system_kit::nestable_block_on(handle_command(opts, &ctx)) {
        Err(e) => {
            error!(ctx.expect_logger(), "{e}");
            std::thread::sleep(std::time::Duration::from_millis(500));
            process::exit(1);
        }
        Ok(_) => {}
    }
}

async fn handle_command(opts: Opts, ctx: &Context) -> Result<(), String> {
    match opts.command {
        Command::Scan(ScanCommand::Blocks(cmd)) => {
            let config: Config =
                Config::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;
            // Download dataset if required
            // If console:
            // - Replay based on SQLite queries
            // If post-to:
            // - Replay that requires connection to bitcoind
            let mut block_range =
                chainhook_sdk::utils::BlockHeights::BlockRange(cmd.start_block, cmd.end_block)
                    .get_sorted_entries();

            if let Some(ref post_to) = cmd.post_to {
                info!(ctx.expect_logger(), "A fully synchronized bitcoind node is required for retrieving inscriptions content.");
                info!(
                    ctx.expect_logger(),
                    "Checking {}...", config.network.bitcoind_rpc_url
                );
                let tip = check_bitcoind_connection(&config).await?;
                if tip < cmd.end_block {
                    error!(ctx.expect_logger(), "Unable to scan block range [{}, {}]: underlying bitcoind synchronized until block {} ", cmd.start_block, cmd.end_block, tip);
                } else {
                    info!(ctx.expect_logger(), "Starting scan");
                }

                let predicate_spec = build_predicate_from_cli(
                    &config,
                    &post_to,
                    cmd.start_block,
                    Some(cmd.end_block),
                )?
                .into_selected_network_specification(&config.network.bitcoin_network)?;
                scan_bitcoin_chainstate_via_rpc_using_predicate(&predicate_spec, &config, &ctx)
                    .await?;
            } else {
                let _ = download_ordinals_dataset_if_required(&config, ctx).await;

                let inscriptions_db_conn =
                    open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;
                while let Some(block_height) = block_range.pop_front() {
                    let mut total_transfers = 0;
                    let inscriptions =
                        find_all_inscriptions_in_block(&block_height, &inscriptions_db_conn, &ctx);
                    let mut locations =
                        find_all_transfers_in_block(&block_height, &inscriptions_db_conn, &ctx);
                    for (_, inscription) in inscriptions.iter() {
                        println!("Inscription {} revealed at block #{} (inscription_number {}, ordinal_number {})", inscription.get_inscription_id(), block_height, inscription.inscription_number, inscription.ordinal_number);
                        if let Some(transfers) = locations.remove(&inscription.get_inscription_id())
                        {
                            for t in transfers.iter().skip(1) {
                                total_transfers += 1;
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
                            total_transfers += 1;
                            println!(
                                "\t→ Transferred in transaction {}",
                                t.transaction_identifier_location.hash
                            );
                        }
                    }
                    if total_transfers > 0 && inscriptions.len() > 0 {
                        println!(
                            "Inscriptions revealed: {}, inscriptions transferred: {total_transfers}",
                            inscriptions.len()
                        );
                        println!("-----");
                    }
                }
            }
        }
        Command::Scan(ScanCommand::Inscription(cmd)) => {
            let config: Config =
                Config::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

            let _ = download_ordinals_dataset_if_required(&config, ctx).await;

            let inscriptions_db_conn =
                open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;
            let (inscription, block_height) =
                match find_inscription_with_id(&cmd.inscription_id, &inscriptions_db_conn, &ctx)? {
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
                inscription.inscription_number,
                inscription.ordinal_number
            );
            let transfers = find_all_inscription_transfers(
                &inscription.get_inscription_id(),
                &inscriptions_db_conn,
                &ctx,
            );
            for (transfer, block_height) in transfers.iter().skip(1) {
                println!(
                    "\t→ Transferred in transaction {} (block {block_height})",
                    transfer.transaction_identifier_location.hash
                );
            }
            println!("Number of transfers: {}", transfers.len() - 1);
        }
        Command::Service(subcmd) => match subcmd {
            ServiceCommand::Start(cmd) => {
                let config =
                    Config::default(cmd.regtest, cmd.testnet, cmd.mainnet, &cmd.config_path)?;

                let _ = initialize_hord_db(&config.expected_cache_path(), &ctx);

                let inscriptions_db_conn =
                    open_readonly_hord_db_conn(&config.expected_cache_path(), &ctx)?;

                let last_known_block =
                    find_latest_inscription_block_height(&inscriptions_db_conn, &ctx)?;

                let hord_config = config.get_hord_config();

                info!(ctx.expect_logger(), "Starting service...",);

                let start_block = match cmd.start_at_block {
                    Some(entry) => entry,
                    None => match last_known_block {
                        Some(entry) => entry,
                        None => {
                            warn!(ctx.expect_logger(), "Inscription ingestion will start at block {} once hord internal indexes are built", hord_config.first_inscription_height);
                            hord_config.first_inscription_height
                        }
                    },
                };

                let mut predicates = vec![];

                for post_to in cmd.post_to.iter() {
                    let predicate = build_predicate_from_cli(&config, post_to, start_block, None)?;
                    predicates.push(ChainhookFullSpecification::Bitcoin(predicate));
                }

                // let predicates = cmd
                //     .predicates_paths
                //     .iter()
                //     .map(|p| load_predicate_from_path(p))
                //     .collect::<Result<Vec<ChainhookFullSpecification>, _>>()?;

                let mut service = Service::new(config, ctx.clone());
                return service.run(predicates).await;
            }
        },
        Command::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::fs::File;
                use std::io::Write;
                let config = Config::default(cmd.regtest, cmd.testnet, cmd.mainnet, &None)?;
                let config_content = generate_config(&config.network.bitcoin_network);
                let mut file_path = PathBuf::new();
                file_path.push("Hord.toml");
                let mut file = File::create(&file_path)
                    .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
                file.write_all(config_content.as_bytes())
                    .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
                println!("Created file Hord.toml");
            }
        },
        Command::Db(HordDbCommand::Sync(_cmd)) => unimplemented!(),
        Command::Db(HordDbCommand::Repair(subcmd)) => match subcmd {
            RepairCommand::Blocks(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let mut hord_config = config.get_hord_config();
                hord_config.network_thread_max = cmd.network_threads;

                let blocks_post_processor = start_block_ingestion_processor(&config, ctx, None);

                download_and_pipeline_blocks(
                    &config,
                    cmd.start_block,
                    cmd.end_block,
                    hord_config.first_inscription_height,
                    Some(&blocks_post_processor),
                    &ctx,
                )
                .await?
            }
            RepairCommand::Inscriptions(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let mut hord_config = config.get_hord_config();
                hord_config.network_thread_max = cmd.network_threads;

                let blocks_post_processor =
                    start_inscription_indexing_processor(&config, ctx, None);

                download_and_pipeline_blocks(
                    &config,
                    cmd.start_block,
                    cmd.end_block,
                    hord_config.first_inscription_height,
                    Some(&blocks_post_processor),
                    &ctx,
                )
                .await?;
            }
            RepairCommand::Transfers(cmd) => {
                let config = Config::default(false, false, false, &cmd.config_path)?;
                let service = Service::new(config, ctx.clone());
                service.replay_transfers(cmd.start_block, cmd.end_block, None)?;
            }
        },
        Command::Db(HordDbCommand::Check(cmd)) => {
            let config = Config::default(false, false, false, &cmd.config_path)?;
            {
                let blocks_db =
                    open_readonly_hord_db_conn_rocks_db(&config.expected_cache_path(), &ctx)?;
                let tip = find_last_block_inserted(&blocks_db) as u64;
                println!("Tip: {}", tip);

                let mut missing_blocks = vec![];
                for i in cmd.start_block..=cmd.end_block {
                    if find_lazy_block_at_block_height(i as u32, 0, false, &blocks_db, &ctx)
                        .is_none()
                    {
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
        }
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
    http_client: &HttpClient,
    block_height: u64,
    bitcoin_config: &BitcoinConfig,
    ctx: &Context,
) -> Result<BitcoinBlockData, String> {
    let block_hash =
        retrieve_block_hash_with_retry(http_client, &block_height, &bitcoin_config, &ctx).await?;
    let block_breakdown =
        download_and_parse_block_with_retry(http_client, &block_hash, &bitcoin_config, &ctx)
            .await?;

    core::parse_ordinals_and_standardize_block(block_breakdown, &bitcoin_config.network, &ctx)
        .map_err(|(e, _)| e)
}

pub fn build_predicate_from_cli(
    config: &Config,
    post_to: &str,
    start_block: u64,
    end_block: Option<u64>,
) -> Result<BitcoinChainhookFullSpecification, String> {
    let mut networks = BTreeMap::new();
    // Retrieve last block height known, and display it
    networks.insert(
        config.network.bitcoin_network.clone(),
        BitcoinChainhookNetworkSpecification {
            start_block: Some(start_block),
            end_block,
            blocks: None,
            expire_after_occurrence: None,
            include_proof: None,
            include_inputs: None,
            include_outputs: None,
            include_witness: None,
            predicate: BitcoinPredicateType::OrdinalsProtocol(OrdinalOperations::InscriptionFeed),
            action: HookAction::HttpPost(HttpHook {
                url: post_to.to_string(),
                authorization_header: "".to_string(),
            }),
        },
    );
    let predicate = BitcoinChainhookFullSpecification {
        uuid: post_to.to_string(),
        owner_uuid: None,
        name: post_to.to_string(),
        version: 1,
        networks,
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
            return Err(format!(
                "unable to connect to bitcoind: {}",
                message.to_string()
            ));
        }
    };

    let end_block = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks.saturating_sub(1),
        Err(e) => {
            return Err(format!("unable to connect to bitcoind: {}", e.to_string()));
        }
    };

    Ok(end_block)
}
