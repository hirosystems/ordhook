pub fn generate_config() -> String {
    let conf = format!(
        r#"[storage]
driver = "redis"
redis_uri = "redis://localhost:6379/"
cache_path = "cache"

[limits]
max_number_of_bitcoin_predicates = 100
max_number_of_concurrent_bitcoin_scans = 100
max_number_of_stacks_predicates = 10
max_number_of_concurrent_stacks_scans = 10
max_number_of_processing_threads = 16
max_number_of_networking_threads = 16
max_caching_memory_size_mb = 32000

[network]
mode = "mainnet"
bitcoind_rpc_url = "http://localhost:8332"
bitcoind_rpc_username = "devnet"
bitcoind_rpc_password = "devnet"
stacks_node_rpc_url = "http://localhost:20443"

[[event_source]]
tsv_file_url = "https://archive.hiro.so/mainnet/stacks-blockchain-api/mainnet-stacks-blockchain-api-latest"
"#
    );
    return conf;
}
