---
title: Run Ordhook as a Service Using Bitcoind
---

## Prerequisites

### Setting Up a Bitcoin Node

The Bitcoin Core daemon (bitcoind) is a program that implements the Bitcoin protocol for remote procedure call (RPC) use. Ordhook can be set up to interact with the Bitcoin chainstate through bitcoind's ZeroMQ interface, its embedded networking library, passing raw blockchain data to be evaluated for relevant events.

This guide is written to work with the latest Bitcoin Core software containing bitcoind, [Bitcoin Core 25.0](https://bitcoincore.org/bin/bitcoin-core-25.0/).

> **_NOTE:_**
>
> While bitcoind can and will start syncing a Bitcoin node, customizing this node to your use cases beyond supporting a Ordhook is out of scope for this guide. See the Bitcoin wiki for ["Running Bitcoin"](https://en.bitcoin.it/wiki/Running_Bitcoin) or bitcoin.org [Running A Full Node guide](https://bitcoin.org/en/full-node).

- Make note of the path of your `bitcoind` executable (located within the `bin` directory of the `bitcoin-25.0` folder you downloaded above appropriate to your operating system)
- Navigate to your project folder where your Ordhook node will reside, create a new file, and rename it to `bitcoin.conf`. Copy the configuration below to this `bitcoin.conf` file.
- Find and copy your Bitcoin data directory and paste to the `datadir` field in the `bitcoin.conf` file below. Either copy the default path (see [list of default directories by operating system](https://en.bitcoin.it/wiki/Data_directory)) or copy the custom path you set for your Bitcoin data
- Set a username of your choice for bitcoind and use it in the `rpcuser` configuration below (`devnet` is a default).
- Set a password of your choice for bitcoind and use it in the `rpcpassword` configuration below (`devnet` is a default).

```conf
# Bitcoin Core Configuration

datadir=/path/to/bitcoin/directory/ # Path to Bitcoin directory
server=1
rpcuser=devnet
rpcpassword=devnet
rpcport=8332
rpcallowip=0.0.0.0/0
rpcallowip=::/0
txindex=1
listen=1
discover=0
dns=0
dnsseed=0
listenonion=0
rpcserialversion=1
disablewallet=0
fallbackfee=0.00001
rpcthreads=8
blocksonly=1
dbcache=4096

# Start zeromq
zmqpubhashblock=tcp://0.0.0.0:18543
```

> **_NOTE:_**
>
> The below command is a startup process that, if this is your first time syncing a node, might take a few hours to a few days to run. Alternatively, if the directory pointed to in the `datadir` field above contains bitcoin blockchain data, syncing will resume.

Now that you have the `bitcoin.conf` file ready with the bitcoind configurations, you can run the bitcoind node. The command takes the form `path/to/bitcoind -conf=path/to/bitcoin.conf`, for example:

```console
/Volumes/SSD/bitcoin-25.0/bin/bitcoind -conf=/Volumes/SSD/project/Ordhook/bitcoin.conf
```

Once the above command runs, you will see `zmq_url` entries in the console's stdout, displaying ZeroMQ logs of your bitcoin node.

### Configure Ordhook

In this section, you will configure Ordhook to match the network configurations with the bitcoin config file. First, [install the latest version of Ordhook](../getting-started.md#installing-ordhook).

Next, you will generate a `Ordhook.toml` file to connect Ordhook with your bitcoind node. Navigate to the directory where you want to generate the `Ordhook.toml` file and use the following command in your terminal:

```console
ordhook config generate --mainnet
```

Several network parameters in the generated `Ordhook.toml` configuration file need to match those in the `bitcoin.conf` file created earlier in the [Setting up a Bitcoin Node](#setting-up-a-bitcoin-node) section. Please update the following parameters accordingly:

1. Update `bitcoind_rpc_username` with the username set for `rpcuser` in `bitcoin.conf`.
2. Update `bitcoind_rpc_password` with the password set for `rpcpassword` in `bitcoin.conf`.
3. Update `bitcoind_rpc_url` with the same host and port used for `rpcport` in `bitcoin.conf`.

Additionally, if you want to receive events from the configured Bitcoin node, substitute `stacks_node_rpc_url` with `bitcoind_zmq_url`, as follows:

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

Here is a table of the relevant parameters this guide changes in our configuration files.

| bitcoin.conf    | Ordhook.toml          |
| --------------- | --------------------- |
| rpcuser         | bitcoind_rpc_username |
| rpcpassword     | bitcoind_rpc_password |
| rpcport         | bitcoind_rpc_url      |
| zmqpubhashblock | bitcoind_zmq_url      |

## Initiate Ordhook Service

In this section, you'll learn how to run Ordhook as a service using [Ordhook SDK](https://github.com/hirosystems/ordhook/tree/develop/components/ordhook-sdk-js) to post events to a server.

Use the following command to start the Ordhook service for streaming and processing new blocks appended to the Bitcoin blockchain:

`ordhook service start --post-to=http://localhost:3000/api/events --config-path=./Ordhook.toml`

When the Ordhook service starts, it is initiated in the background to augment the blocks from Bitcoin. Bitcoind sends ZeroMQ notifications to Ordhook to retrieve the inscriptions.

### Add `http-post` endpoints dynamically

To enable dynamically posting endpoints to the server, you can spin up the Redis server by enabling the following lines of code in the `Ordhook.toml` file:

```toml
[http_api]
http_port = 20456
database_uri = "redis://localhost:6379/"
```

## Run ordhook service

Based on the `Ordhook.toml` file configuration, the ordhook service spins up an HTTP API to manage event destinations. Use the following command to start the ordhook service:

`ordhook service start --config-path=./Ordhook.toml`

A comprehensive OpenAPI specification explaining how to interact with this HTTP REST API can be found [here](https://github.com/hirosystems/ordhook/blob/develop/docs/ordhook-openapi.json).
