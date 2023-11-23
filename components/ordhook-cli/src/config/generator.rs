use ordhook::chainhook_sdk::types::BitcoinNetwork;

pub fn generate_config(network: &BitcoinNetwork) -> String {
    let network = format!("{:?}", network);
    let conf = format!(
        r#"[storage]
working_dir = "ordhook"

# The Http Api allows you to register / deregister
# dynamically predicates.
# Disable by default.
#
# [http_api]
# http_port = 20456
# database_uri = "redis://localhost:6379/"

[network]
mode = "{network}"
bitcoind_rpc_url = "http://0.0.0.0:8332"
bitcoind_rpc_username = "devnet"
bitcoind_rpc_password = "devnet"
# Bitcoin block events can be received by Chainhook
# either through a Bitcoin node's ZeroMQ interface,
# or through the Stacks node. Zmq is being
# used by default:
bitcoind_zmq_url = "tcp://0.0.0.0:18543"
# but stacks can also be used:
# stacks_node_rpc_url = "http://0.0.0.0:20443"

[limits]
max_number_of_bitcoin_predicates = 100
max_number_of_concurrent_bitcoin_scans = 100
max_number_of_processing_threads = 16
bitcoin_concurrent_http_requests_max = 16
max_caching_memory_size_mb = 32000

# Disable the following section if the state
# must be built locally
[bootstrap]
download_url = "https://archive.hiro.so/mainnet/ordhook/mainnet-ordhook-sqlite-latest"

[logs]
ordinals_internals = true
chainhook_internals = true
"#,
        network = network.to_lowercase(),
    );
    conf
}
