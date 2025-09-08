use std::path::PathBuf;

use bitcoin::Network;

use crate::toml::ConfigToml;

pub const DEFAULT_WORKING_DIR: &str = "data";
pub const DEFAULT_ULIMIT: usize = 2048;
pub const DEFAULT_MEMORY_AVAILABLE: usize = 8;
pub const DEFAULT_BITCOIND_RPC_THREADS: usize = 4;
pub const DEFAULT_BITCOIND_RPC_TIMEOUT: u32 = 15;
pub const DEFAULT_LRU_CACHE_SIZE: usize = 50_000;
pub const DEFAULT_INDEXER_CHANNEL_CAPACITY: usize = 10;

#[derive(Clone, Debug)]
pub struct Config {
    pub bitcoind: BitcoindConfig,
    pub ordinals: Option<OrdinalsConfig>,
    pub runes: Option<RunesConfig>,
    pub resources: ResourcesConfig,
    pub storage: StorageConfig,
    pub metrics: Option<MetricsConfig>,
    pub redis: Option<RedisConfig>,
}

#[derive(Clone, Debug)]
pub struct OrdinalsConfig {
    pub db: PgDatabaseConfig,
    pub meta_protocols: Option<OrdinalsMetaProtocolsConfig>,
}

#[derive(Clone, Debug)]
pub struct OrdinalsMetaProtocolsConfig {
    pub brc20: Option<OrdinalsBrc20Config>,
}

#[derive(Clone, Debug)]
pub struct OrdinalsBrc20Config {
    pub enabled: bool,
    pub lru_cache_size: usize,
    pub db: PgDatabaseConfig,
}

#[derive(Clone, Debug)]
pub struct RunesConfig {
    pub lru_cache_size: usize,
    pub db: PgDatabaseConfig,
}

#[derive(Clone, Debug)]
pub struct BitcoindConfig {
    pub network: Network,
    pub rpc_url: String,
    pub rpc_username: String,
    pub rpc_password: String,
    pub zmq_url: String,
}

/// A Postgres configuration for a single database.
#[derive(Clone, Debug)]
pub struct PgDatabaseConfig {
    pub dbname: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub search_path: Option<String>,
    pub pool_max_size: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub working_dir: String,
}

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub prometheus_port: u16,
}

#[derive(Clone, Debug, Default)]
pub struct RedisConfig {
    pub enabled: bool,
    pub url: String,
    pub queue: String,
    pub database: Option<u8>,                // Redis database number (0-15)
    pub cluster_nodes: Option<Vec<String>>,  // for Redis Cluster
    pub sentinel_nodes: Option<Vec<String>>, // for Redis Sentinel
    pub sentinel_master_name: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub retry_attempts: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub connection_timeout_ms: Option<u64>,
    pub command_timeout_ms: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResourcesConfig {
    pub ulimit: usize,
    pub cpu_core_available: usize,
    pub memory_available: usize,
    pub bitcoind_rpc_threads: usize,
    pub bitcoind_rpc_timeout: u32,
    pub indexer_channel_capacity: usize,
}

impl ResourcesConfig {
    pub fn get_optimal_thread_pool_capacity(&self) -> usize {
        // Generally speaking when dealing a pool, we need one thread for
        // feeding the thread pool and eventually another thread for
        // handling the "reduce" step.
        self.cpu_core_available.saturating_sub(2).max(1)
    }
}

impl Config {
    pub fn from_file_path(file_path: &str) -> Result<Config, String> {
        ConfigToml::config_from_file_path(file_path)
    }

    pub fn expected_cache_path(&self) -> PathBuf {
        let mut destination_path = PathBuf::new();
        destination_path.push(&self.storage.working_dir);
        destination_path
    }

    pub fn devnet_default() -> Config {
        Config {
            storage: StorageConfig {
                working_dir: default_cache_path(),
            },
            resources: ResourcesConfig {
                cpu_core_available: num_cpus::get(),
                memory_available: DEFAULT_MEMORY_AVAILABLE,
                ulimit: DEFAULT_ULIMIT,
                bitcoind_rpc_threads: DEFAULT_BITCOIND_RPC_THREADS,
                bitcoind_rpc_timeout: DEFAULT_BITCOIND_RPC_TIMEOUT,
                indexer_channel_capacity: DEFAULT_INDEXER_CHANNEL_CAPACITY,
            },
            bitcoind: BitcoindConfig {
                rpc_url: "http://0.0.0.0:18443".into(),
                rpc_username: "devnet".into(),
                rpc_password: "devnet".into(),
                network: Network::Regtest,
                zmq_url: "http://0.0.0.0:18543".into(),
            },
            ordinals: Some(OrdinalsConfig {
                db: PgDatabaseConfig {
                    dbname: "ordinals".to_string(),
                    host: "localhost".to_string(),
                    port: 5432,
                    user: "postgres".to_string(),
                    password: Some("postgres".to_string()),
                    search_path: None,
                    pool_max_size: None,
                },
                meta_protocols: None,
            }),
            runes: Some(RunesConfig {
                lru_cache_size: DEFAULT_LRU_CACHE_SIZE,
                db: PgDatabaseConfig {
                    dbname: "runes".to_string(),
                    host: "localhost".to_string(),
                    port: 5432,
                    user: "postgres".to_string(),
                    password: Some("postgres".to_string()),
                    search_path: None,
                    pool_max_size: None,
                },
            }),
            metrics: Some(MetricsConfig {
                enabled: true,
                prometheus_port: 9153,
            }),
            redis: Some(RedisConfig {
                enabled: true,
                url: "redis://127.0.0.1:6379/0".into(),
                queue: "bitcoin:index-progress".into(),
                database: None, // defaults to 0
                cluster_nodes: None,
                sentinel_nodes: None,
                sentinel_master_name: None,
                username: None,
                password: None,
                retry_attempts: None,
                retry_backoff_ms: None,
                connection_timeout_ms: None,
                command_timeout_ms: None,
            }),
        }
    }

    pub fn testnet_default() -> Config {
        let mut default = Config::devnet_default();
        default.bitcoind.network = Network::Testnet;
        default
    }

    pub fn mainnet_default() -> Config {
        let mut default = Config::devnet_default();
        default.bitcoind.rpc_url = "http://localhost:8332".into();
        default.bitcoind.network = Network::Bitcoin;
        default
    }

    // TODO: Move this to a shared test utils component
    pub fn test_default() -> Config {
        let mut config = Self::mainnet_default();
        config.storage.working_dir = "tmp".to_string();
        config.resources.bitcoind_rpc_threads = 1;
        config.resources.cpu_core_available = 1;
        config
    }

    pub fn ordinals_brc20_config(&self) -> Option<&OrdinalsBrc20Config> {
        if let Some(OrdinalsConfig {
            meta_protocols:
                Some(OrdinalsMetaProtocolsConfig {
                    brc20: Some(brc20), ..
                }),
            ..
        }) = &self.ordinals
        {
            if brc20.enabled {
                return Some(brc20);
            }
        }
        None
    }

    pub fn assert_ordinals_config(&self) -> Result<(), String> {
        if self.ordinals.is_none() {
            return Err("Config entry for `ordinals` not found in config file.".to_string());
        }
        Ok(())
    }

    pub fn assert_runes_config(&self) -> Result<(), String> {
        if self.runes.is_none() {
            return Err("Config entry for `runes` not found in config file.".to_string());
        }
        Ok(())
    }
}

pub fn default_cache_path() -> String {
    let mut cache_path = std::env::current_dir().expect("unable to get current dir");
    cache_path.push("data");
    format!("{}", cache_path.display())
}
