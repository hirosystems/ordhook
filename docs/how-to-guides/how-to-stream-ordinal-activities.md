---
title: Stream Ordinal Activities to an API
---

Ordhook is a tool that helps you find ordinal activity from the Bitcoin chain and build your own customized database with that data. This guide will show you how to use Ordhook to stream ordinal activity.

### Configure Ordhook

This section walks you through streaming ordinal activities. To post the ordinal activity, you'll need to configure bitcoind. Refer to [Setting up a bitcoin node](./how-to-run-ordhook-as-a-service-using-bitcoind.md#setting-up-a-bitcoin-node) to understand the steps to configure Bitcoind.

> **_NOTE_**
> Ordhook is applicable to the Bitcoin chain only.

Once the Bitcoin node is configured, you can use the following command in your terminal to create a configuration for Ordhook:

`ordhook config new --mainnet`

You will see a success message "Created file Ordhook.toml" in your terminal.

The generated `Ordhook.toml` file looks like this:

```toml
[storage]
working_dir = "ordhook"

# The Http Api allows you to register / deregister
# dynamically predicates.
# Disable by default.
#
# [http_api]
# http_port = 20456
# database_uri = "redis://localhost:6379/"

[network]
mode = "mainnet"
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
```

Observe that the bitcoind configured fields will appear in the `Ordhook.toml` file. Now, ensure that these fields are configured with the right values and URLs, as shown below:

```toml
bitcoind_rpc_url = "http://0.0.0.0:8332"
bitcoind_rpc_username = "devnet"
bitcoind_rpc_password = "devnet"
bitcoind_zmq_url = "tcp://0.0.0.0:18543"
```

### Post Ordinal Activity to an External Endpoint

After adjusting the `Ordhook.toml` settings to make them match the bitcoind configuration, the following command can be run to scan blocks and post events to a local server:

`ordhook scan blocks 767430 767753 --post-to=http://localhost:3000/api/events --config-path=./Ordhook.toml`

The above command uses Ordhook to stream and then post ordinal activities to `http://localhost:3000/api/events` where you can build out your own database or custom views.
