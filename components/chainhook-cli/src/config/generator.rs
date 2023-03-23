pub fn generate_config() -> String {
    let conf = format!(
        r#"[storage]
driver = "redis"
redis_uri = "redis://localhost:6379/"
cache_path = "chainhook_cache"

[chainhooks]
max_stacks_registrations = 500
max_bitcoin_registrations = 500

[network]
mode = "mainnet"
bitcoin_node_rpc_url = "http://localhost:8332"
bitcoin_node_rpc_username = "devnet"
bitcoin_node_rpc_password = "devnet"
stacks_node_rpc_url = "http://localhost:20443"
"#
    );
    return conf;
}
