pub fn generate_config() -> String {
    let conf = format!(
        r#"[storage]
driver = "redis"
redis_uri = "redis://localhost:6379/"
cache_path = "cache"

[chainhooks]
max_stacks_registrations = 100
max_bitcoin_registrations = 100
max_stacks_concurrent_scans = 10
max_bitcoin_concurrent_scans = 10

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
