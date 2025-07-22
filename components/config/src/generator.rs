pub fn generate_toml_config(network: &str) -> String {
    let conf = format!(
        r#"[storage]
working_dir = "tmp"

[metrics]
enabled = true
prometheus_port = 9153

[ordinals.db]
database = "ordinals"
host = "localhost"
port = 5432
username = "postgres"
password = "postgres"

[ordinals.meta_protocols.brc20]
enabled = true
lru_cache_size = 10000

[ordinals.meta_protocols.brc20.db]
database = "brc20"
host = "localhost"
port = 5432
username = "postgres"
password = "postgres"

[runes]
lru_cache_size = 10000

[runes.db]
database = "runes"
host = "localhost"
port = 5432
username = "postgres"
password = "postgres"

[bitcoind]
network = "{network}"
rpc_url = "http://localhost:8332"
rpc_username = "devnet"
rpc_password = "devnet"
zmq_url = "tcp://0.0.0.0:18543"

[resources]
ulimit = 2048
cpu_core_available = 6
memory_available = 16
bitcoind_rpc_threads = 2
bitcoind_rpc_timeout = 15
indexer_channel_capacity = 10
"#,
        network = network.to_lowercase(),
    );
    conf
}
