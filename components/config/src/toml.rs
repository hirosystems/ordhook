use std::{
    fs::File,
    io::{BufReader, Read},
};

use bitcoin::Network;

use crate::{
    BitcoindConfig, Config, MetricsConfig, OrdinalsBrc20Config, OrdinalsConfig,
    OrdinalsMetaProtocolsConfig, PgDatabaseConfig, ResourcesConfig, RunesConfig, StorageConfig,
    DEFAULT_BITCOIND_RPC_THREADS, DEFAULT_BITCOIND_RPC_TIMEOUT, DEFAULT_INDEXER_CHANNEL_CAPACITY,
    DEFAULT_LRU_CACHE_SIZE, DEFAULT_MEMORY_AVAILABLE, DEFAULT_ULIMIT, DEFAULT_WORKING_DIR,
};

#[derive(Deserialize, Clone, Debug)]
pub struct PgDatabaseConfigToml {
    pub database: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub search_path: Option<String>,
    pub pool_max_size: Option<usize>,
}

impl PgDatabaseConfigToml {
    fn to_config(&self) -> PgDatabaseConfig {
        PgDatabaseConfig {
            dbname: self.database.clone(),
            host: self.host.clone(),
            port: self.port,
            user: self.username.clone(),
            password: self.password.clone(),
            search_path: self.search_path.clone(),
            pool_max_size: self.pool_max_size,
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct OrdinalsConfigToml {
    pub db: PgDatabaseConfigToml,
    pub meta_protocols: Option<OrdinalsMetaProtocolsConfigToml>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct OrdinalsMetaProtocolsConfigToml {
    pub brc20: Option<OrdinalsBrc20ConfigToml>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct OrdinalsBrc20ConfigToml {
    pub enabled: bool,
    pub lru_cache_size: Option<usize>,
    pub db: PgDatabaseConfigToml,
}

#[derive(Deserialize, Clone, Debug)]
pub struct RunesConfigToml {
    pub lru_cache_size: Option<usize>,
    pub db: PgDatabaseConfigToml,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfigToml {
    pub working_dir: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResourcesConfigToml {
    pub ulimit: Option<usize>,
    pub cpu_core_available: Option<usize>,
    pub memory_available: Option<usize>,
    pub bitcoind_rpc_threads: Option<usize>,
    pub bitcoind_rpc_timeout: Option<u32>,
    pub indexer_channel_capacity: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BitcoindConfigToml {
    pub network: String,
    pub rpc_url: String,
    pub rpc_username: String,
    pub rpc_password: String,
    pub zmq_url: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MetricsConfigToml {
    pub enabled: bool,
    pub prometheus_port: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigToml {
    pub storage: StorageConfigToml,
    pub ordinals: Option<OrdinalsConfigToml>,
    pub runes: Option<RunesConfigToml>,
    pub bitcoind: BitcoindConfigToml,
    pub resources: ResourcesConfigToml,
    pub metrics: Option<MetricsConfigToml>,
}

impl ConfigToml {
    pub fn config_from_file_path(file_path: &str) -> Result<Config, String> {
        let file = File::open(file_path)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;
        let mut file_reader = BufReader::new(file);
        let mut file_buffer = vec![];
        file_reader
            .read_to_end(&mut file_buffer)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;

        let config_file: ConfigToml = match toml::from_slice(&file_buffer) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Config file malformatted {}", e));
            }
        };
        ConfigToml::config_from_toml(config_file)
    }

    fn config_from_toml(toml: ConfigToml) -> Result<Config, String> {
        let bitcoin_network = match toml.bitcoind.network.as_str() {
            "devnet" => Network::Regtest,
            "testnet" => Network::Testnet,
            "mainnet" => Network::Bitcoin,
            "signet" => Network::Signet,
            _ => return Err("bitcoind.network not supported".to_string()),
        };
        let ordinals = match toml.ordinals {
            Some(ordinals) => Some(OrdinalsConfig {
                db: ordinals.db.to_config(),
                meta_protocols: match ordinals.meta_protocols {
                    Some(meta_protocols) => Some(OrdinalsMetaProtocolsConfig {
                        brc20: match meta_protocols.brc20 {
                            Some(brc20) => Some(OrdinalsBrc20Config {
                                enabled: brc20.enabled,
                                lru_cache_size: brc20
                                    .lru_cache_size
                                    .unwrap_or(DEFAULT_LRU_CACHE_SIZE),
                                db: brc20.db.to_config(),
                            }),
                            None => None,
                        },
                    }),
                    None => None,
                },
            }),
            None => None,
        };
        let runes = match toml.runes {
            Some(runes) => Some(RunesConfig {
                lru_cache_size: runes.lru_cache_size.unwrap_or(DEFAULT_LRU_CACHE_SIZE),
                db: runes.db.to_config(),
            }),
            None => None,
        };
        let metrics = toml.metrics.map(|metrics| MetricsConfig {
            enabled: metrics.enabled,
            prometheus_port: metrics.prometheus_port,
        });

        let config = Config {
            storage: StorageConfig {
                working_dir: toml
                    .storage
                    .working_dir
                    .unwrap_or(DEFAULT_WORKING_DIR.into()),
            },
            ordinals,
            runes,
            resources: ResourcesConfig {
                ulimit: toml.resources.ulimit.unwrap_or(DEFAULT_ULIMIT),
                cpu_core_available: toml.resources.cpu_core_available.unwrap_or(num_cpus::get()),
                memory_available: toml
                    .resources
                    .memory_available
                    .unwrap_or(DEFAULT_MEMORY_AVAILABLE),
                bitcoind_rpc_threads: toml
                    .resources
                    .bitcoind_rpc_threads
                    .unwrap_or(DEFAULT_BITCOIND_RPC_THREADS),
                bitcoind_rpc_timeout: toml
                    .resources
                    .bitcoind_rpc_timeout
                    .unwrap_or(DEFAULT_BITCOIND_RPC_TIMEOUT),
                indexer_channel_capacity: toml
                    .resources
                    .indexer_channel_capacity
                    .unwrap_or(DEFAULT_INDEXER_CHANNEL_CAPACITY),
            },
            bitcoind: BitcoindConfig {
                rpc_url: toml.bitcoind.rpc_url.to_string(),
                rpc_username: toml.bitcoind.rpc_username.to_string(),
                rpc_password: toml.bitcoind.rpc_password.to_string(),
                network: bitcoin_network,
                zmq_url: toml.bitcoind.zmq_url,
            },
            metrics,
        };
        Ok(config)
    }
}
