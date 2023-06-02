pub mod file;
pub mod generator;

pub use chainhook_event_observer::indexer::IndexerConfig;
use chainhook_event_observer::observer::EventObserverConfig;
use chainhook_types::{BitcoinBlockSignaling, BitcoinNetwork, StacksNetwork};
pub use file::ConfigFile;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use crate::service::{DEFAULT_CONTROL_PORT, DEFAULT_INGESTION_PORT};

const DEFAULT_MAINNET_STACKS_TSV_ARCHIVE: &str =
    "https://archive.hiro.so/mainnet/stacks-blockchain-api/mainnet-stacks-blockchain-api-latest";
const DEFAULT_TESTNET_STACKS_TSV_ARCHIVE: &str =
    "https://archive.hiro.so/testnet/stacks-blockchain-api/testnet-stacks-blockchain-api-latest";
const DEFAULT_MAINNET_ORDINALS_SQLITE_ARCHIVE: &str =
    "https://archive.hiro.so/mainnet/chainhooks/hord-latest.sqlite";

#[derive(Clone, Debug)]
pub struct Config {
    pub storage: StorageConfig,
    pub event_sources: Vec<EventSourceConfig>,
    pub chainhooks: ChainhooksConfig,
    pub network: IndexerConfig,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub driver: StorageDriver,
    pub cache_path: String,
}

#[derive(Clone, Debug)]
pub enum StorageDriver {
    Redis(RedisConfig),
    Tikv(TikvConfig),
    Memory,
}

#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub uri: String,
}

#[derive(Clone, Debug)]
pub struct TikvConfig {
    pub uri: String,
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
pub struct ChainhooksConfig {
    pub max_stacks_registrations: u16,
    pub max_bitcoin_registrations: u16,
    pub enable_http_api: bool,
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

    pub fn get_event_observer_config(&self) -> EventObserverConfig {
        EventObserverConfig {
            hooks_enabled: true,
            bitcoin_rpc_proxy_enabled: true,
            event_handlers: vec![],
            chainhook_config: None,
            ingestion_port: DEFAULT_INGESTION_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            control_api_enabled: self.chainhooks.enable_http_api,
            bitcoind_rpc_username: self.network.bitcoind_rpc_username.clone(),
            bitcoind_rpc_password: self.network.bitcoind_rpc_password.clone(),
            bitcoind_rpc_url: self.network.bitcoind_rpc_url.clone(),
            stacks_node_rpc_url: self.network.stacks_node_rpc_url.clone(),
            bitcoin_block_signaling: self.network.bitcoin_block_signaling.clone(),
            operators: HashSet::new(),
            display_logs: false,
            cache_path: self.storage.cache_path.clone(),
            bitcoin_network: self.network.bitcoin_network.clone(),
            stacks_network: self.network.stacks_network.clone(),
            ordinals_enabled: true,
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
                driver: StorageDriver::Redis(RedisConfig {
                    uri: config_file.storage.redis_uri.to_string(),
                }),
                cache_path: config_file.storage.cache_path.unwrap_or("cache".into()),
            },
            event_sources,
            chainhooks: ChainhooksConfig {
                max_stacks_registrations: config_file
                    .chainhooks
                    .max_stacks_registrations
                    .unwrap_or(100),
                max_bitcoin_registrations: config_file
                    .chainhooks
                    .max_bitcoin_registrations
                    .unwrap_or(100),
                enable_http_api: true,
            },
            network: IndexerConfig {
                stacks_node_rpc_url: config_file.network.stacks_node_rpc_url.to_string(),
                bitcoind_rpc_url: config_file.network.bitcoind_rpc_url.to_string(),
                bitcoind_rpc_username: config_file.network.bitcoind_rpc_username.to_string(),
                bitcoind_rpc_password: config_file.network.bitcoind_rpc_password.to_string(),
                bitcoin_block_signaling: match config_file.network.bitcoind_zmq_url {
                    Some(ref zmq_url) => BitcoinBlockSignaling::ZeroMQ(zmq_url.clone()),
                    None => BitcoinBlockSignaling::Stacks(
                        config_file.network.stacks_node_rpc_url.clone(),
                    ),
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

    pub fn expected_tikv_config(&self) -> &TikvConfig {
        match self.storage.driver {
            StorageDriver::Tikv(ref conf) => conf,
            _ => unreachable!(),
        }
    }

    pub fn expected_redis_config(&self) -> &RedisConfig {
        match self.storage.driver {
            StorageDriver::Redis(ref conf) => conf,
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
        destination_path.push(&self.storage.cache_path);
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
                driver: StorageDriver::Redis(RedisConfig {
                    uri: "redis://localhost:6379/".into(),
                }),
                cache_path: default_cache_path(),
            },
            event_sources: vec![],
            chainhooks: ChainhooksConfig {
                max_stacks_registrations: 50,
                max_bitcoin_registrations: 50,
                enable_http_api: true,
            },
            network: IndexerConfig {
                stacks_node_rpc_url: "http://0.0.0.0:20443".into(),
                bitcoind_rpc_url: "http://0.0.0.0:18443".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    "http://0.0.0.0:20443".into(),
                ),
                stacks_network: StacksNetwork::Devnet,
                bitcoin_network: BitcoinNetwork::Regtest,
            },
        }
    }

    pub fn testnet_default() -> Config {
        Config {
            storage: StorageConfig {
                driver: StorageDriver::Redis(RedisConfig {
                    uri: "redis://localhost:6379/".into(),
                }),
                cache_path: default_cache_path(),
            },
            event_sources: vec![EventSourceConfig::StacksTsvUrl(UrlConfig {
                file_url: DEFAULT_TESTNET_STACKS_TSV_ARCHIVE.into(),
            })],
            chainhooks: ChainhooksConfig {
                max_stacks_registrations: 10,
                max_bitcoin_registrations: 10,
                enable_http_api: true,
            },
            network: IndexerConfig {
                stacks_node_rpc_url: "http://0.0.0.0:20443".into(),
                bitcoind_rpc_url: "http://0.0.0.0:18332".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    "http://0.0.0.0:20443".into(),
                ),
                stacks_network: StacksNetwork::Testnet,
                bitcoin_network: BitcoinNetwork::Testnet,
            },
        }
    }

    pub fn mainnet_default() -> Config {
        Config {
            storage: StorageConfig {
                driver: StorageDriver::Redis(RedisConfig {
                    uri: "redis://localhost:6379/".into(),
                }),
                cache_path: default_cache_path(),
            },
            event_sources: vec![
                EventSourceConfig::StacksTsvUrl(UrlConfig {
                    file_url: DEFAULT_MAINNET_STACKS_TSV_ARCHIVE.into(),
                }),
                EventSourceConfig::OrdinalsSqliteUrl(UrlConfig {
                    file_url: DEFAULT_MAINNET_ORDINALS_SQLITE_ARCHIVE.into(),
                }),
            ],
            chainhooks: ChainhooksConfig {
                max_stacks_registrations: 10,
                max_bitcoin_registrations: 10,
                enable_http_api: true,
            },
            network: IndexerConfig {
                stacks_node_rpc_url: "http://0.0.0.0:20443".into(),
                bitcoind_rpc_url: "http://0.0.0.0:8332".into(),
                bitcoind_rpc_username: "devnet".into(),
                bitcoind_rpc_password: "devnet".into(),
                bitcoin_block_signaling: BitcoinBlockSignaling::Stacks(
                    "http://0.0.0.0:20443".into(),
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
