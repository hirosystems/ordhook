---
title: Explore Ordinal activities in your terminal
---

Ordhook can be used to extract ordinal activities from the Bitcoin chain and stream these activities to the indexer. This guide helps you with the steps to explore ordinal activities and post these activities to the indexer.

### Explore ordinal activity

You can use the following command to scan a range of blocks on mainnet or testnet.

`ordhook scan blocks 767430 767753 --mainnet`

The above command generates a `hord.sqlite.gz` file in your directory and displays inscriptions and transfers activities occurring in the range of the specified blocks.

The output of the above command looks like this:

```
Inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 revealed at block #767430 (ordinal_number 1252201400444387, inscription_number 0)
Inscription 26482871f33f1051f450f2da9af275794c0b5f1c61ebf35e4467fb42c2813403i0 revealed at block #767753 (ordinal_number 727624168684699, inscription_number 1)
```

You can now generate an activity for a given inscription by using the following command:

`ordhook scan inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 --mainnet`

The above command generates the ordinal activity for that inscription and also the number of transfers in the transactions.

```
Inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 revealed at block #767430 (inscription_number 0, ordinal_number 1252201400444387)
        → Transferred in transaction 0x2c8a11858825ae2056be90c3e49938d271671ac4245b452cd88b1475cbea8971 (block #785391)
        → Transferred in transaction 0xbc4c30829a9564c0d58e6287195622b53ced54a25711d1b86be7cd3a70ef61ed (block #785396)
Number of transfers: 2
```

### Configure ordhook

This section walks you through streaming ordinal activities to an indexer. To post the ordinal activity, you'll need to configure bitcoind. Refer to [Setting up a bitcoin node](https://docs.hiro.so/chainhook/how-to-guides/how-to-run-chainhook-as-a-service-using-bitcoind#setting-up-a-bitcoin-node) to understand the steps to configure Bitcoind.

> **_NOTE_**
> Ordhook is applicable to the Bitcoin chain only.

Once the Bitcoin node is configured, you can use the following command in your terminal to create a configuration for Ordhook.

`ordhook config new --mainnet`

You will see a success message "Created file Ordhook.toml" in your terminal.

The generated `ordhook.toml` file looks like this:

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

Observe that the bitcoind configured fields will appear in the `ordhook.toml` file. Now, ensure that these fields are configured with the right values and URLs, as shown below:

```toml
bitcoind_rpc_url = "http://0.0.0.0:8332"
bitcoind_rpc_username = "devnet"
bitcoind_rpc_password = "devnet"
bitcoind_zmq_url = "tcp://0.0.0.0:18543"
```

### Post ordinal activity to the indexer

After adjusting the `Ordhook.toml` settings to make them match the bitcoind configuration, the following command can be run to scan blocks and post events to a local server.

`ordhook scan blocks 767430 767753 --post-to=http://localhost:3000/api/events --config-path=./Ordhook.toml`

The above command uses chainhook to create a predicate ordinal theory where one of the inscriptions is created and posts the events to `http://localhost:3000/api/events`.

You can update the above command to scan between block heights and post events to the local server.
