#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub storage: StorageConfigFile,
    pub http_api: Option<PredicatesApiConfigFile>,
    pub event_source: Option<Vec<EventSourceConfigFile>>,
    pub limits: LimitsConfigFile,
    pub network: NetworkConfigFile,
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
pub struct EventSourceConfigFile {
    pub source_type: Option<String>,
    pub stacks_node_url: Option<String>,
    pub chainhook_node_url: Option<String>,
    pub polling_delay: Option<u32>,
    pub tsv_file_path: Option<String>,
    pub tsv_file_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LimitsConfigFile {
    pub max_number_of_bitcoin_predicates: Option<usize>,
    pub max_number_of_concurrent_bitcoin_scans: Option<usize>,
    pub max_number_of_stacks_predicates: Option<usize>,
    pub max_number_of_concurrent_stacks_scans: Option<usize>,
    pub max_number_of_processing_threads: Option<usize>,
    pub max_number_of_networking_threads: Option<usize>,
    pub max_caching_memory_size_mb: Option<usize>,
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
