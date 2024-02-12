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

[resources]
ulimit = 2048
cpu_core_available = 16
memory_available = 32
bitcoind_rpc_threads = 4
bitcoind_rpc_timeout = 15
expected_observers_count = 1

# Disable the following section if the state
# must be built locally
[snapshot]
download_url = "https://archive.hiro.so/mainnet/ordhook/mainnet-ordhook-sqlite-latest"

[logs]
ordinals_internals = true
chainhook_internals = true
"#,
        network = network.to_lowercase(),
    );
    conf
}
