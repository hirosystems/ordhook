#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub storage: StorageConfigFile,
    pub event_source: Option<Vec<EventSourceConfigFile>>,
    pub chainhooks: ChainhooksConfigFile,
    pub network: NetworkConfigFile,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfigFile {
    pub driver: String,
    pub redis_uri: String,
    pub cache_path: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct EventSourceConfigFile {
    pub source_type: Option<String>,
    pub stacks_node_url: Option<String>,
    pub chainhook_node_url: Option<String>,
    pub polling_delay: Option<u32>,
    pub tsv_file_path: Option<String>,
    pub tsv_file_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChainhooksConfigFile {
    pub max_stacks_registrations: Option<usize>,
    pub max_bitcoin_registrations: Option<usize>,
    pub max_stacks_concurrent_scans: Option<usize>,
    pub max_bitcoin_concurrent_scans: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NetworkConfigFile {
    pub mode: String,
    pub bitcoind_rpc_url: String,
    pub bitcoind_rpc_username: String,
    pub bitcoind_rpc_password: String,
    pub bitcoind_zmq_url: Option<String>,
    pub stacks_node_rpc_url: String,
}
