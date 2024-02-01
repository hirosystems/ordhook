use ordhook::chainhook_sdk::indexer::IndexerConfig;
use ordhook::chainhook_sdk::observer::DEFAULT_INGESTION_PORT;
use ordhook::chainhook_sdk::types::{
    BitcoinBlockSignaling, BitcoinNetwork, StacksNetwork, StacksNodeConfig,
};
use ordhook::config::{
    Config, LogConfig, PredicatesApi, PredicatesApiConfig, ResourcesConfig, SnapshotConfig,
    StorageConfig, DEFAULT_BITCOIND_RPC_THREADS, DEFAULT_BITCOIND_RPC_TIMEOUT,
    DEFAULT_CONTROL_PORT, DEFAULT_MEMORY_AVAILABLE, DEFAULT_ULIMIT,
};
use std::fs::File;
use std::io::{BufReader, Read};

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub storage: StorageConfigFile,
    pub http_api: Option<PredicatesApiConfigFile>,
    pub resources: ResourcesConfigFile,
    pub network: NetworkConfigFile,
    pub logs: Option<LogConfigFile>,
    pub snapshot: Option<SnapshotConfigFile>,
}

impl ConfigFile {
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
                return Err(format!("Config file malformatted {}", e));
            }
        };
        ConfigFile::from_config_file(config_file)
    }

    pub fn from_config_file(config_file: ConfigFile) -> Result<Config, String> {
        let (stacks_network, bitcoin_network) = match config_file.network.mode.as_str() {
            "devnet" => (StacksNetwork::Devnet, BitcoinNetwork::Regtest),
            "testnet" => (StacksNetwork::Testnet, BitcoinNetwork::Testnet),
            "mainnet" => (StacksNetwork::Mainnet, BitcoinNetwork::Mainnet),
            "signet" => (StacksNetwork::Testnet, BitcoinNetwork::Signet),
            _ => return Err("network.mode not supported".to_string()),
        };

        let snapshot = match config_file.snapshot {
            Some(bootstrap) => match bootstrap.download_url {
                Some(ref url) => SnapshotConfig::Download(url.to_string()),
                None => SnapshotConfig::Build,
            },
            None => SnapshotConfig::Build,
        };

        let config = Config {
            storage: StorageConfig {
                working_dir: config_file.storage.working_dir.unwrap_or("ordhook".into()),
            },
            http_api: match config_file.http_api {
                None => PredicatesApi::Off,
                Some(http_api) => match http_api.disabled {
                    Some(false) => PredicatesApi::Off,
                    _ => PredicatesApi::On(PredicatesApiConfig {
                        http_port: http_api.http_port.unwrap_or(DEFAULT_CONTROL_PORT),
                        display_logs: http_api.display_logs.unwrap_or(true),
                    }),
                },
            },
            snapshot,
            resources: ResourcesConfig {
                ulimit: config_file.resources.ulimit.unwrap_or(DEFAULT_ULIMIT),
                cpu_core_available: config_file
                    .resources
                    .cpu_core_available
                    .unwrap_or(num_cpus::get()),
                memory_available: config_file
                    .resources
                    .memory_available
                    .unwrap_or(DEFAULT_MEMORY_AVAILABLE),
                bitcoind_rpc_threads: config_file
                    .resources
                    .bitcoind_rpc_threads
                    .unwrap_or(DEFAULT_BITCOIND_RPC_THREADS),
                bitcoind_rpc_timeout: config_file
                    .resources
                    .bitcoind_rpc_timeout
                    .unwrap_or(DEFAULT_BITCOIND_RPC_TIMEOUT),
                expected_observers_count: config_file
                    .resources
                    .expected_observers_count
                    .unwrap_or(1),
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
            logs: LogConfig {
                ordinals_internals: config_file
                    .logs
                    .as_ref()
                    .and_then(|l| l.ordinals_internals)
                    .unwrap_or(true),
                chainhook_internals: config_file
                    .logs
                    .as_ref()
                    .and_then(|l| l.chainhook_internals)
                    .unwrap_or(true),
            },
        };
        Ok(config)
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
            (false, false, false, Some(config_path)) => ConfigFile::from_file_path(config_path)?,
            _ => Err("Invalid combination of arguments".to_string())?,
        };
        Ok(config)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct LogConfigFile {
    pub ordinals_internals: Option<bool>,
    pub chainhook_internals: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfigFile {
    pub working_dir: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PredicatesApiConfigFile {
    pub http_port: Option<u16>,
    pub database_uri: Option<String>,
    pub display_logs: Option<bool>,
    pub disabled: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotConfigFile {
    pub download_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResourcesConfigFile {
    pub ulimit: Option<usize>,
    pub cpu_core_available: Option<usize>,
    pub memory_available: Option<usize>,
    pub bitcoind_rpc_threads: Option<usize>,
    pub bitcoind_rpc_timeout: Option<u32>,
    pub expected_observers_count: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NetworkConfigFile {
    pub mode: String,
    pub bitcoind_rpc_url: String,
    pub bitcoind_rpc_username: String,
    pub bitcoind_rpc_password: String,
    pub bitcoind_zmq_url: Option<String>,
    pub stacks_node_rpc_url: Option<String>,
    pub stacks_events_ingestion_port: Option<u16>,
}
