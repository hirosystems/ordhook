pub mod file;
pub mod generator;

use chainhook_sdk::hord::HordConfig;
pub use chainhook_sdk::indexer::IndexerConfig;
use chainhook_sdk::observer::EventObserverConfig;
use chainhook_types::{BitcoinBlockSignaling, BitcoinNetwork, StacksNetwork, StacksNodeConfig};
pub use file::ConfigFile;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

const DEFAULT_MAINNET_STACKS_TSV_ARCHIVE: &str =
    "https://archive.hiro.so/mainnet/stacks-blockchain-api/mainnet-stacks-blockchain-api-latest";
const DEFAULT_TESTNET_STACKS_TSV_ARCHIVE: &str =
    "https://archive.hiro.so/testnet/stacks-blockchain-api/testnet-stacks-blockchain-api-latest";
const DEFAULT_MAINNET_ORDINALS_SQLITE_ARCHIVE: &str =
    "https://archive.hiro.so/mainnet/chainhooks/hord-latest.sqlite";
const DEFAULT_REDIS_URI: &str = "redis://localhost:6379/";

pub const DEFAULT_INGESTION_PORT: u16 = 20455;
pub const DEFAULT_CONTROL_PORT: u16 = 20456;
pub const STACKS_SCAN_THREAD_POOL_SIZE: usize = 10;
pub const BITCOIN_SCAN_THREAD_POOL_SIZE: usize = 10;
pub const STACKS_MAX_PREDICATE_REGISTRATION: usize = 50;
pub const BITCOIN_MAX_PREDICATE_REGISTRATION: usize = 50;

#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub http_api: PredicatesApi,
    pub event_sources: Vec<EventSourceConfig>,
    pub limits: LimitsConfig,
    pub network: IndexerConfig,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub working_dir: String,
}

#[derive(Clone, Debug)]
pub enum PredicatesApi {
    Off,
    On(PredicatesApiConfig),
}

#[derive(Clone, Debug)]
pub struct PredicatesApiConfig {
    pub http_port: u16,
    pub database_uri: String,
    pub display_logs: bool,
}

#[derive(Clone, Debug)]
pub enum EventSourceConfig {
    StacksTsvPath(PathConfig),
    StacksTsvUrl(UrlConfig),
    OrdinalsSqlitePath(PathConfig),
    OrdinalsSqliteUrl(UrlConfig),
}

#[derive(Clone, Debug)]
pub struct PathConfig {
    pub file_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct UrlConfig {
    pub file_url: String,
}

#[derive(Clone, Debug)]
pub struct LimitsConfig {
    pub max_number_of_bitcoin_predicates: usize,
    pub max_number_of_concurrent_bitcoin_scans: usize,
    pub max_number_of_stacks_predicates: usize,
    pub max_number_of_concurrent_stacks_scans: usize,
    pub max_number_of_processing_threads: usize,
    pub max_number_of_networking_threads: usize,
    pub max_caching_memory_size_mb: usize,
}

impl Config {
    pub fn from_file_path(file_path: &str) -> Result<Config, String> {
        let file = File::open(file_path)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;
        let mut file_reader = BufReader::new(file);
        let mut file_buffer = vec![];
        file_reader
            .read_to_end(&mut file_buffer)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;

        let config_file: ConfigFile = match toml::from_slice(&file_buffer) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Config file malformatted {}", e.to_string()));
            }
        };
        Config::from_config_file(config_file)
    }

    pub fn is_http_api_enabled(&self) -> bool {
        match self.http_api {
            PredicatesApi::Off => false,
            PredicatesApi::On(_) => true,
        }
    }

    pub fn get_hord_config(&self) -> HordConfig {
        HordConfig {
            network_thread_max: self.limits.max_number_of_networking_threads,
            ingestion_thread_max: self.limits.max_number_of_processing_threads,
            cache_size: self.limits.max_caching_memory_size_mb,
            db_path: self.expected_cache_path(),
            first_inscription_height: match self.network.bitcoin_network {
                BitcoinNetwork::Mainnet => 767430,
                BitcoinNetwork::Regtest => 1,
                BitcoinNetwork::Testnet => 2413343,
                // BitcoinNetwork::Signet => 112402,
            },
        }
    }

    pub fn get_event_observer_config(&self) -> EventObserverConfig {
        EventObserverConfig {
            bitcoin_rpc_proxy_enabled: true,
            event_handlers: vec![],
            chainhook_config: None,
            ingestion_port: DEFAULT_INGESTION_PORT,
            bitcoind_rpc_username: self.network.bitcoind_rpc_username.clone(),
            bitcoind_rpc_password: self.network.bitcoind_rpc_password.clone(),
            bitcoind_rpc_url: self.network.bitcoind_rpc_url.clone(),
            bitcoin_block_signaling: self.network.bitcoin_block_signaling.clone(),
            display_logs: false,
            cache_path: self.storage.working_dir.clone(),
            bitcoin_network: self.network.bitcoin_network.clone(),
            stacks_network: self.network.stacks_network.clone(),
            hord_config: Some(self.get_hord_config()),
        }
    }

    pub fn from_config_file(config_file: ConfigFile) -> Result<Config, String> {
        let (stacks_network, bitcoin_network) = match config_file.network.mode.as_str() {
            "devnet" => (StacksNetwork::Devnet, BitcoinNetwork::Regtest),
            "testnet" => (StacksNetwork::Testnet, BitcoinNetwork::Testnet),
            "mainnet" => (StacksNetwork::Mainnet, BitcoinNetwork::Mainnet),
            _ => return Err("network.mode not supported".to_string()),
        };

        let mut event_sources = vec![];
        for source in config_file.event_source.unwrap_or(vec![]).iter_mut() {
            if let Some(dst) = source.tsv_file_path.take() {
                let mut file_path = PathBuf::new();
                file_path.push(dst);
                event_sources.push(EventSourceConfig::StacksTsvPath(PathConfig { file_path }));
                continue;
            }
            if let Some(file_url) = source.tsv_file_url.take() {
                event_sources.push(EventSourceConfig::StacksTsvUrl(UrlConfig { file_url }));
                continue;
            }
        }

        let config = Config {
            storage: StorageConfig {
                working_dir: config_file.storage.working_dir.unwrap_or("cache".into()),
            },
            http_api: match config_file.http_api {
                None => PredicatesApi::Off,
                Some(http_api) => match http_api.disabled {
                    Some(false) => PredicatesApi::Off,
                    _ => PredicatesApi::On(PredicatesApiConfig {
                        http_port: http_api.http_port.unwrap_or(DEFAULT_CONTROL_PORT),
                        display_logs: http_api.display_logs.unwrap_or(true),
                        database_uri: http_api
                            .database_uri
                            .unwrap_or(DEFAULT_REDIS_URI.to_string()),
                    }),
                },
            },
            event_sources,
            limits: LimitsConfig {
                max_number_of_stacks_predicates: config_file
                    .limits
                    .max_number_of_stacks_predicates
                    .unwrap_or(STACKS_MAX_PREDICATE_REGISTRATION),
                max_number_of_bitcoin_predicates: config_file
                    .limits
                    .max_number_of_bitcoin_predicates
                    .unwrap_or(BITCOIN_MAX_PREDICATE_REGISTRATION),
                max_number_of_concurrent_stacks_scans: config_file
                    .limits
                    .max_number_of_concurrent_stacks_scans
                    .unwrap_or(STACKS_SCAN_THREAD_POOL_SIZE),
                max_number_of_concurrent_bitcoin_scans: config_file
                    .limits
                    .max_number_of_concurrent_bitcoin_scans
                    .unwrap_or(BITCOIN_SCAN_THREAD_POOL_SIZE),
                max_number_of_processing_threads: config_file
                    .limits
                    .max_number_of_processing_threads
                    .unwrap_or(1.max(num_cpus::get().saturating_sub(1))),
                max_number_of_networking_threads: config_file
                    .limits
                    .max_number_of_networking_threads
                    .unwrap_or(1.max(num_cpus::get().saturating_sub(1))),
                max_caching_memory_size_mb: config_file
                    .limits
                    .max_caching_memory_size_mb
                    .unwrap_or(2048),
            },
            network: IndexerConfig {
                bitcoind_rpc_url: config_file.network.bitcoind_rpc_url.to_string(),
                bitcoind_rpc_username: config_file.network.bitcoind_rpc_username.to_string(),
                bitcoind_rpc_password: config_file.network.bitcoind_rpc_password.to_string(),
                bitcoin_block_signaling: match config_file.network.bitcoind_zmq_url {
                    Some(ref zmq_url) => BitcoinBlockSignaling::ZeroMQ(zmq_url.clone()),
                    None => BitcoinBlockSignaling::Stacks(StacksNodeConfig::default_localhost(
                        config_file
                            .network
                            .stacks_events_ingestion_port
                            .unwrap_or(DEFAULT_INGESTION_PORT),
                    )),
                },
                stacks_network,
                bitcoin_network,
            },
        };
        Ok(config)
    }

    pub fn is_initial_ingestion_required(&self) -> bool {
        for source in self.event_sources.iter() {
            match source {
                EventSourceConfig::StacksTsvUrl(_) | EventSourceConfig::StacksTsvPath(_) => {
                    return true
                }
                _ => {}
            }
        }
        return false;
    }

    pub fn add_local_stacks_tsv_source(&mut self, file_path: &PathBuf) {
        self.event_sources
            .push(EventSourceConfig::StacksTsvPath(PathConfig {
                file_path: file_path.clone(),
            }));
    }

    pub fn add_ordinals_sqlite_remote_source_url(&mut self, file_url: &str) {
        self.event_sources
            .push(EventSourceConfig::OrdinalsSqliteUrl(UrlConfig {
                file_url: file_url.to_string(),
            }));
    }

    pub fn add_local_ordinals_sqlite_source(&mut self, file_path: &PathBuf) {
        self.event_sources
            .push(EventSourceConfig::OrdinalsSqlitePath(PathConfig {
                file_path: file_path.clone(),
            }));
    }

    pub fn expected_api_database_uri(&self) -> &str {
        &self.expected_api_config().database_uri
    }

    pub fn expected_api_config(&self) -> &PredicatesApiConfig {
        match self.http_api {
            PredicatesApi::On(ref config) => config,
            _ => unreachable!(),
        }
    }

    pub fn expected_local_stacks_tsv_file(&self) -> &PathBuf {
        for source in self.event_sources.iter() {
            if let EventSourceConfig::StacksTsvPath(config) = source {
                return &config.file_path;
            }
        }
        panic!("expected local-tsv source")
    }

    pub fn expected_cache_path(&self) -> PathBuf {
        let mut destination_path = PathBuf::new();
        destination_path.push(&self.storage.working_dir);
        destination_path
    }

    fn expected_remote_ordinals_sqlite_base_url(&self) -> &String {
        for source in self.event_sources.iter() {
            if let EventSourceConfig::OrdinalsSqliteUrl(config) = source {
                return &config.file_url;
            }
        }
        panic!("expected remote-tsv source")
    }

    fn expected_remote_stacks_tsv_base_url(&self) -> &String {
        for source in self.event_sources.iter() {
            if let EventSourceConfig::StacksTsvUrl(config) = source {
                return &config.file_url;
            }
        }
        panic!("expected remote-tsv source")
    }

    pub fn expected_remote_stacks_tsv_sha256(&self) -> String {
        format!("{}.sha256", self.expected_remote_stacks_tsv_base_url())
    }

    pub fn expected_remote_stacks_tsv_url(&self) -> String {
        format!("{}.gz", self.expected_remote_stacks_tsv_base_url())
    }

    pub fn expected_remote_ordinals_sqlite_sha256(&self) -> String {
        format!("{}.sha256", self.expected_remote_ordinals_sqlite_base_url())
    }

    pub fn expected_remote_ordinals_sqlite_url(&self) -> String {
        format!("{}.gz", self.expected_remote_ordinals_sqlite_base_url())
    }

    pub fn rely_on_remote_stacks_tsv(&self) -> bool {
        for source in self.event_sources.iter() {
            if let EventSourceConfig::StacksTsvUrl(_config) = source {
                return true;
            }
        }
        false
    }

    pub fn rely_on_remote_ordinals_sqlite(&self) -> bool {
        for source in self.event_sources.iter() {
            if let EventSourceConfig::OrdinalsSqliteUrl(_config) = source {
                return true;
            }
        }
        false
    }

    pub fn should_download_remote_stacks_tsv(&self) -> bool {
        let mut rely_on_remote_tsv = false;
        let mut remote_tsv_present_locally = false;
        for source in self.event_sources.iter() {
            if let EventSourceConfig::StacksTsvUrl(_config) = source {
                rely_on_remote_tsv = true;
            }
            if let EventSourceConfig::StacksTsvPath(_config) = source {
                remote_tsv_present_locally = true;
            }
        }
        rely_on_remote_tsv == true && remote_tsv_present_locally == false
    }

    pub fn should_download_remote_ordinals_sqlite(&self) -> bool {
        let mut rely_on_remote_tsv = false;
        let mut remote_tsv_present_locally = false;
        for source in self.event_sources.iter() {
            if let EventSourceConfig::OrdinalsSqliteUrl(_config) = source {
                rely_on_remote_tsv = true;
            }
            if let EventSourceConfig::OrdinalsSqlitePath(_config) = source {
                remote_tsv_present_locally = true;
            }
        }
        rely_on_remote_tsv == true && remote_tsv_present_locally == false
    }

    pub fn default(
        devnet: bool,
        testnet: bool,
        mainnet: bool,
        config_path: &Option<String>,
    ) -> Result<Config, String> {
        let config = match (devnet, testnet, mainnet, config_path) {
            (true, false, false, _) => Config::devnet_default(),
            (false, true, false, _) => Config::testnet_default(),
            (false, false, true, _) => Config::mainnet_default(),
            (false, false, false, Some(config_path)) => Config::from_file_path(&config_path)?,
            _ => Err("Invalid combination of arguments".to_string())?,
        };
        Ok(config)
    }

    pub fn devnet_default() -> Config {
        Config {
            storage: StorageConfig {
                working_dir: default_cache_path(),
            },
            http_api: PredicatesApi::Off,
            event_sources: vec![],
            limits: LimitsConfig {
                max_number_of_bitcoin_predicates: BITCOIN_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_bitcoin_scans: BITCOIN_SCAN_THREAD_POOL_SIZE,
                max_number_of_stacks_predicates: STACKS_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_stacks_scans: STACKS_SCAN_THREAD_POOL_SIZE,
                max_number_of_processing_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_number_of_networking_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_caching_memory_size_mb: 2048,
            },
            network: IndexerConfig {
                bitcoind_rpc_url: "http://0.0.0.0:18443".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    StacksNodeConfig::default_localhost(DEFAULT_INGESTION_PORT),
                ),
                stacks_network: StacksNetwork::Devnet,
                bitcoin_network: BitcoinNetwork::Regtest,
            },
        }
    }

    pub fn testnet_default() -> Config {
        Config {
            storage: StorageConfig {
                working_dir: default_cache_path(),
            },
            http_api: PredicatesApi::Off,
            event_sources: vec![EventSourceConfig::StacksTsvUrl(UrlConfig {
                file_url: DEFAULT_TESTNET_STACKS_TSV_ARCHIVE.into(),
            })],
            limits: LimitsConfig {
                max_number_of_bitcoin_predicates: BITCOIN_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_bitcoin_scans: BITCOIN_SCAN_THREAD_POOL_SIZE,
                max_number_of_stacks_predicates: STACKS_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_stacks_scans: STACKS_SCAN_THREAD_POOL_SIZE,
                max_number_of_processing_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_number_of_networking_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_caching_memory_size_mb: 2048,
            },
            network: IndexerConfig {
                bitcoind_rpc_url: "http://0.0.0.0:18332".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    StacksNodeConfig::default_localhost(DEFAULT_INGESTION_PORT),
                ),
                stacks_network: StacksNetwork::Testnet,
                bitcoin_network: BitcoinNetwork::Testnet,
            },
        }
    }

    pub fn mainnet_default() -> Config {
        Config {
            storage: StorageConfig {
                working_dir: default_cache_path(),
            },
            http_api: PredicatesApi::Off,
            event_sources: vec![
                EventSourceConfig::StacksTsvUrl(UrlConfig {
                    file_url: DEFAULT_MAINNET_STACKS_TSV_ARCHIVE.into(),
                }),
                EventSourceConfig::OrdinalsSqliteUrl(UrlConfig {
                    file_url: DEFAULT_MAINNET_ORDINALS_SQLITE_ARCHIVE.into(),
                }),
            ],
            limits: LimitsConfig {
                max_number_of_bitcoin_predicates: BITCOIN_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_bitcoin_scans: BITCOIN_SCAN_THREAD_POOL_SIZE,
                max_number_of_stacks_predicates: STACKS_MAX_PREDICATE_REGISTRATION,
                max_number_of_concurrent_stacks_scans: STACKS_SCAN_THREAD_POOL_SIZE,
                max_number_of_processing_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_number_of_networking_threads: 1.max(num_cpus::get().saturating_sub(1)),
                max_caching_memory_size_mb: 2048,
            },
            network: IndexerConfig {
                bitcoind_rpc_url: "http://0.0.0.0:8332".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    StacksNodeConfig::default_localhost(DEFAULT_INGESTION_PORT),
                ),
                stacks_network: StacksNetwork::Mainnet,
                bitcoin_network: BitcoinNetwork::Mainnet,
            },
        }
    }
}

pub fn default_cache_path() -> String {
    let mut cache_path = std::env::current_dir().expect("unable to get current dir");
    cache_path.push("cache");
    format!("{}", cache_path.display())
}
