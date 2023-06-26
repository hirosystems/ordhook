# ⛓️🪝 Chainhook

## Introduction

Blockchains are pieces of infrastructure that unblock new use cases and introduce a new generation of decentralized applications by relying on a public ledger.
`chainhook` is a fork-aware transaction indexing engine aiming at helping developers focus on the information they need, by helping with the on-chain data extraction. By focusing on the data they care about, developers are working with much lighter datasets. Benefits are plurals: 
- Improved Developer Experience: instead of working with a generic blockchain indexer, taking hours to process every single transaction of every single block, developers can create their own indexes, build, iterate, and refine them in minutes. 
- Cost Optimization: by avoiding full chain indexation, developers avoid massive storage management and unnecessary storage scaling issues. Also, `chainhook` helps developers create elegant event-based architectures. Developers write `if_this` / `then_that` **predicates**, being evaluated on transactions and blocks. When the evaluation of these **predicates** appears to be true, the related transactions are packaged as events and forwarded to the configured destination. By using cloud functions as destinations, developers can also cut costs on processing by only paying for processing when a block that contains some data relevant to the developer's application is being mined.
- Optimized User Experience: lighter indexes implies faster query results, which helps minimize end-user response time. 

---
## Install chainhook

### Install from the source

```bash 
$ git clone https://github.com/hirosystems/chainhook.git
$ cd chainhook
$ cargo chainhook-install
```

---
## Development workflow for Bitcoin chainhooks

### Guide to `if_this` / `then_that` predicate design

To get started with Bitcoin predicates, we can use the `chainhook` to generate a template: 

```bash
$ chainhook predicates new hello-ordinals.json --bitcoin
```

We will focus on the `if_this` and `then_that` parts of the specifications.

The current `bitcoin` predicates support the following `if_this` constructs:

```jsonc
// Get any transaction matching a given txid
// `txid` mandatory argument admits:
//  - 32 bytes hex encoded type. example: 
{
    "if_this": {
        "scope": "txid",
        "equals": "0xfaaac1833dc4883e7ec28f61e35b41f896c395f8d288b1a177155de2abd6052f"
    }
}

// Get any transaction, including an OP_RETURN output starting with a set of characters.
// `starts_with` mandatory argument admits:
//  - ASCII string type. example: `X2[`
//  - hex encoded bytes. example: `0x589403`
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "starts_with": "X2["
        }
    }
}

// Get any transaction, including an OP_RETURN output matching the sequence of bytes specified 
// `equals` mandatory argument admits:
//  - hex encoded bytes. example: `0x589403`
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "equals": "0x69bd04208265aca9424d0337dac7d9e84371a2c91ece1891d67d3554bd9fdbe60afc6924d4b0773d90000006700010000006600012"
        }
    }
}

// Get any transaction including an OP_RETURN output ending with a set of characters 
// `ends_with` mandatory argument admits:
//  - ASCII string type. example: `X2[`
//  - hex encoded bytes. example: `0x589403`
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "ends_with": "0x76a914000000000000000000000000000000000000000088ac"
        }
    }
}

// Get any transaction including a p2pkh output paying a given recipient
// `p2pkh` construct admits:
//  - string type. example: "mr1iPkD9N3RJZZxXRk7xF9d36gffa6exNC"
//  - hex encoded bytes type. example: "0x76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" 
{
    "if_this": {
        "scope": "outputs",
        "p2pkh": "mr1iPkD9N3RJZZxXRk7xF9d36gffa6exNC"
    }
}

// Get any transaction including a p2sh output paying a given recipient
// `p2sh` construct admits:
//  - string type. example: "2MxDJ723HBJtEMa2a9vcsns4qztxBuC8Zb2"
//  - hex encoded bytes type. example: "0x76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac"
{
    "if_this": {
        "scope": "outputs",
        "p2sh": "2MxDJ723HBJtEMa2a9vcsns4qztxBuC8Zb2"
    }
}

// Get any transaction including a p2wpkh output paying a given recipient
// `p2wpkh` construct admits:
//  - string type. example: "bcrt1qnxknq3wqtphv7sfwy07m7e4sr6ut9yt6ed99jg"
{
    "if_this": {
        "scope": "outputs",
        "p2wpkh": "bcrt1qnxknq3wqtphv7sfwy07m7e4sr6ut9yt6ed99jg"
    }
}

// Get any transaction including a p2wsh output paying a given recipient
// `p2wsh` construct admits:
//  - string type. example: "bc1qklpmx03a8qkv263gy8te36w0z9yafxplc5kwzc"
{
    "if_this": {
        "scope": "outputs",
        "p2wsh": "bc1qklpmx03a8qkv263gy8te36w0z9yafxplc5kwzc"
    }
}

// Get any Bitcoin transaction including a Block commitment.
// Broadcasted payloads include Proof of Transfer reward information.
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "block_committed"
    }
}

// Get any transaction, including a key registration operation 
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "leader_key_registered"
    }
}

// Get any transaction, including an STX transfer operation 
// Coming soon
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "stx_transferred"
    }
}

// Get any transaction, including an STX lock operation
// Coming soon
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "stx_locked"
    }
}

// Get any transaction including a new Ordinal inscription (inscription revealed and transferred)
{
    "if_this": {
        "scope": "ordinals_protocol",
        "operation": "inscription_feed"
    }
}

```

In terms of actions available, the following `then_that` constructs are supported:

```jsonc
// HTTP Post block / transaction payload to a given endpoint.
// `http_post` construct admits:
//  - url (string type). Example: http://localhost:3000/api/v1/wrapBtc
//  - authorization_header (string type). Secret to add to the request `authorization` header when posting payloads
{
    "then_that": {
        "http_post": {
            "url": "http://localhost:3000/api/v1/wrapBtc",
            "authorization_header": "Bearer cn389ncoiwuencr"
        }
    }
}

// Append events to a file through the filesystem. Convenient for local tests.
// `file_append` construct admits:
//  - path (string type). Path to file on disk.
{
    "then_that": {
        "file_append": {
            "path": "/tmp/events.json",
        }
    }
}
```

Additional configuration knobs available:
```jsonc
// Ignore any block prior to given block:
"start_block": 101

// Ignore any block after given block:
"end_block": 201

// Stop evaluating chainhook after a given number of occurrences found:
"expire_after_occurrence": 1

// Include proof:
"include_proof": false

// Include Bitcoin transaction inputs in payload:
"include_inputs": false

// Include Bitcoin transaction outputs in payload:
"include_outputs": false

// Include Bitcoin transaction witness in payload:
"include_witness": false

```

Putting all the pieces together:
```jsonc
// Retrieve and HTTP Post to `http://localhost:3000/api/v1/wrapBtc` 
// the 5 first transfers to the p2wpkh `bcrt1qnxk...yt6ed99jg` address,
// of any amount, occurring after block height 10200. 
{
  "chain": "bitcoin",
  "uuid": "1",
  "name": "Wrap BTC",
  "version": 1,
  "networks": {
    "testnet": {
      "if_this": {
        "scope": "outputs",
        "p2wpkh": {
          "equals": "bcrt1qnxknq3wqtphv7sfwy07m7e4sr6ut9yt6ed99jg"
        }
      },
      "then_that": {
        "http_post": {
          "url": "http://localhost:3000/api/v1/transfers",
          "authorization_header": "Bearer cn389ncoiwuencr"
        }
      },
      "start_block": 10200,
      "expire_after_occurrence": 5,
    }
  }
}

// A specification file can also include different networks.
// In this case, the chainhook will select the predicate
// corresponding to the network it was launched against.
{
  "chain": "bitcoin",
  "uuid": "1",
  "name": "Wrap BTC",
  "version": 1,
  "networks": {
    "testnet": {
      "if_this": {
        "protocol": "ordinals",
        "operation": "inscription_feed"
      },
      "then_that": {
        "http_post": {
          "url": "http://localhost:3000/api/v1/ordinals",
          "authorization_header": "Bearer cn389ncoiwuencr"
        }
      },
      "start_block": 10200,
    },
    "mainnet": {
      "if_this": {
        "protocol": "ordinals",
        "operation": "inscription_feed"
      },
      "then_that": {
        "http_post": {
          "url": "http://my-protocol.xyz/api/v1/ordinals",
          "authorization_header": "Bearer cn389ncoiwuencr"
        }
      },
      "start_block": 90232,
    }

  }
}

```

### Guide to local Bitcoin testnet / mainnet predicate scanning

In order to scan the Bitcoin chain with a given predicate, a `bitcoind` instance with access to the RPC methods `getblockhash` and `getblock` must be accessible. The RPC calls latency will directly impact the speed of the scans.

*Note: the configuration of a `bitcoind` instance is out of scope for this guide.*

Assuming a `bitcoind` node correctly configured, scans can be performed using the following command:
```bash
$ chainhook predicates scan ./path/to/predicate.json --testnet
```
When using the flag `--testnet`, the scan operation will generate a configuration file in memory using the following settings:
```toml
[storage]
working_dir = "cache" # Directory used by chainhook node for caching data

[network]
mode = "testnet"
bitcoind_rpc_url = "http://0.0.0.0:18332"
bitcoind_rpc_username = "bitcoind_username"
bitcoind_rpc_password = "bitcoind_password"
# bitcoind_zmq_url = "http://0.0.0.0:18543"

[limits]
max_number_of_bitcoin_predicates = 100
max_number_of_concurrent_bitcoin_scans = 100
max_number_of_stacks_predicates = 10
max_number_of_concurrent_stacks_scans = 10
max_number_of_processing_threads = 16
max_number_of_networking_threads = 16
max_caching_memory_size_mb = 32000
```

When using the flag `--mainnet`, the scan operation will generate a configuration file in memory using the following settings:
```toml
[storage]
working_dir = "cache"

[network]
mode = "testnet"
bitcoind_rpc_url = "http://0.0.0.0:8332"
bitcoind_rpc_username = "bitcoind_username"
bitcoind_rpc_password = "bitcoind_password"
# bitcoind_zmq_url = "http://0.0.0.0:18543"

[limits]
max_number_of_bitcoin_predicates = 100
max_number_of_concurrent_bitcoin_scans = 100
max_number_of_stacks_predicates = 10
max_number_of_concurrent_stacks_scans = 10
max_number_of_processing_threads = 16
max_number_of_networking_threads = 16
max_caching_memory_size_mb = 32000
```

By passing the flag `--config=/path/to/config.toml`, developers can customize the credentials and network address of their Bitcoin node. 
```bash
$ chainhook config new --testnet
✔ Generated config file Chainhook.toml

$ chainhook predicates scan ./path/predicate.json --config-path=./Testnet.toml
```

**Tips and tricks**

To optimize their experience with scanning, developers have a few knobs they can play with:

- Use of adequate values for `start_block` and `end_block` in predicates will drastically improve the speed.
- Networking: reducing the number of network hops between the chainhook process and the bitcoind process can also help a lot.

---
## Development workflow for Stacks chainhooks

### Guide to `if_this` / `then_that` predicate design

To get started with stacks predicates, we can use the `chainhook` to generate a template: 

```bash
$ chainhook predicates new hello-arkadiko.json --stacks
```

We will focus on the `if_this` and `then_that` parts of the specifications.

The current `stacks` predicates support the following `if_this` constructs:

```jsonc
// Get any transaction matching a given txid
// `txid` mandatory argument admits:
//  - 32 bytes hex encoded type. example: "0xfaaac1833dc4883e7ec28f61e35b41f896c395f8d288b1a177155de2abd6052f" 
{
    "if_this": {
        "scope": "txid",
        "equals": "0xfaaac1833dc4883e7ec28f61e35b41f896c395f8d288b1a177155de2abd6052f"
    }
}

// Get any stacks block matching constraints
// `block_height` mandatory argument admits:
//  - `equals`, `higher_than`, `lower_than`, `between`: integer type.
{
    "if_this": {
        "scope": "block_height",
        "higher_than": 10000
    }
}

// Get any transaction related to a given fungible token asset identifier
// `asset-identifier` mandatory argument admits:
//  - string type, fully qualifying the asset identifier to observe. example: `ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.cbtc-sip10::cbtc`
// `actions` mandatory argument admits:
//  - array of string type constrained to `mint`, `transfer` and `burn` values. example: ["mint", "burn"]
{
    "if_this": {
        "scope": "ft_event",
        "asset_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.cbtc-token::cbtc",
        "actions": ["burn"]
    },
}

// Get any transaction related to a given non-fungible token asset identifier
// `asset-identifier` mandatory argument admits:
//  - string type, fully qualifying the asset identifier to observe. example: `ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09::monkeys`
// `actions` mandatory argument admits:
//  - array of string type constrained to `mint`, `transfer` and `burn` values. example: ["mint", "burn"]
{
    "if_this": {
        "scope": "nft_event",
        "asset_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09::monkeys",
        "actions": ["mint", "transfer", "burn"]
    },
}

// Get any transaction moving STX tokens
// `actions` mandatory argument admits:
//  - array of string type constrained to `mint`, `transfer`, and `lock` values. example: ["mint", "lock"]
{
    "if_this": {
        "scope": "stx_event",
        "asset_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09::monkeys",
        "actions": ["transfer", "lock"]
    },
}

// Get any transaction emitting given print events predicate
// `contract-identifier` mandatory argument admits:
//  - string type, fully qualifying the contract to observe. example: `ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09`
// `contains` mandatory argument admits:
//  - string type, used for matching event
{
    "if_this": {
        "scope": "print_event",
        "contract_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09",
        "contains": "vault"
    },
}

// Get any transaction calling a specific method for a given contract **directly**.
// Warning: if the watched method is being called by another contract, this predicate won't detect it.
// `contract-identifier` mandatory argument admits:
//  - string type, fully qualifying the contract to observe. example: `SP000000000000000000002Q6VF78.pox`
// `method` mandatory argument admits:
//  - string type, used for specifying the method to observe. example: `stack-stx`
{
    "if_this": {
        "scope": "contract_call",
        "contract_identifier": "SP000000000000000000002Q6VF78.pox",
        "method": "stack-stx"
    },
}

// Get any transaction, including a contract deployment
// `deployer` mandatory argument admits:
//  - string "*"
//  - string encoding a valid STX address. example: "ST2CY5V39NHDPWSXMW9QDT3HC3GD6Q6XX4CFRK9AG"
{
    "if_this": {
        "scope": "contract_deployment",
        "deployer": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM"
    },
}

// Get any transaction, including a contract deployment implementing a given trait (coming soon)
// `implement-trait` mandatory argument admits:
//  - string type, fully qualifying the trait's shape to observe. example: `ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.sip09-protocol`
{
    "if_this": {
        "scope": "contract_deployment",
        "implement_trait": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.sip09-protocol"
    },
}
```

In terms of actions available, the following `then_that` constructs are supported:

```jsonc
// HTTP Post block / transaction payload to a given endpoint.
// `http_post` construct admits:
//  - url (string type). Example: http://localhost:3000/api/v1/wrapBtc
//  - authorization_header (string type). Secret to add to the request `authorization` header when posting payloads
{
    "then_that": {
        "http_post": {
            "url": "http://localhost:3000/api/v1/wrapBtc",
            "authorization_header": "Bearer cn389ncoiwuencr"
        }
    }
}

// Append events to a file through the filesystem. Convenient for local tests.
// `file_append` construct admits:
//  - path (string type). Path to file on disk.
{
    "then_that": {
        "file_append": {
            "path": "/tmp/events.json",
        }
    }
}
```

Additional configuration knobs available:
```jsonc
// Ignore any block prior to given block:
"start_block": 101

// Ignore any block after given block:
"end_block": 201

// Stop evaluating chainhook after a given number of occurrences found:
"expire_after_occurrence": 1

// Include decoded clarity values in payload
"decode_clarity_values": true
```

Putting all the pieces together:
```jsonc
// Retrieve and HTTP Post to `http://localhost:3000/api/v1/wrapBtc` 
// the 5 first transactions interacting with ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09,
// emitting print events containing the word 'vault'. 
{
  "chain": "stacks",
  "uuid": "1",
  "name": "Lorem ipsum",
  "version": 1,
  "networks": {
    "testnet": {
        "if_this": {
            "scope": "print_event",
            "contract_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09",
            "contains": "vault"
        },
        "then_that": {
            "http_post": {
            "url": "http://localhost:3000/api/v1/vaults",
            "authorization_header": "Bearer cn389ncoiwuencr"
            }
        },
        "start_block": 10200,
        "expire_after_occurrence": 5,
    }
  }
}

// A specification file can also include different networks.
// In this case, the chainhook will select the predicate
// corresponding to the network it was launched against.
{
  "chain": "stacks",
  "uuid": "1",
  "name": "Lorem ipsum",
  "version": 1,
  "networks": {
    "testnet": {
        "if_this": {
            "scope": "print_event",
            "contract_identifier": "ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM.monkey-sip09",
            "contains": "vault"
        },
        "then_that": {
            "http_post": {
                "url": "http://localhost:3000/api/v1/vaults",
                "authorization_header": "Bearer cn389ncoiwuencr"
            }
        },
        "start_block": 10200,
        "expire_after_occurrence": 5,
    },
    "mainnet": {
        "if_this": {
            "scope": "print_event",
            "contract_identifier": "SP456HQKV0RJXZFY1DGX8MNSNYVE3VGZJSRT459863.monkey-sip09",
            "contains": "vault"
        },
      "then_that": {
            "http_post": {
                "url": "http://my-protocol.xyz/api/v1/vaults",
                "authorization_header": "Bearer cn389ncoiwuencr"
            }
      },
      "start_block": 90232,
      "expire_after_occurrence": 5,
    }
  }
}

```

### Guide to local Stacks testnet / mainnet predicate scanning

Developers can test their Stacks predicates without spinning up a Stacks node.
To date, the Stacks blockchain has just over 2 years of activity, and the `chainhook` utility is able to work with both `testnet` and `mainnet` chainstates, in memory.  

To test a Stacks `if_this` / `then_that` predicate, the following command can by used:

```bash
$ chainhook predicates scan ./path/to/predicate.json  --testnet
```

The first time this command run, a chainstate archive will be downloaded, uncompressed, and written to disk (around 3GB required for the testnet, 10GB for the mainnet).

The subsequent scans will use the cached chainstate if already present, speeding up iterations and the overall feedback loop. 

---
## Run `chainhook` as a service for streaming new blocks

`chainhook` can be run as a background service for streaming and processing new canonical blocks appended to the Bitcoin and Stacks blockchains.

When running chainhook as a service, `if_this` / `then_that` predicates can be registered by passing the path of the `json` file in the command line: 

```bash
$ chainhook service start --predicate-path=./path/to/predicate-1.json --predicate-path=./path/to/predicate-2.json --config-path=./path/to/config.toml
```

Predicates can also be added dynamically. When the `--predicate-path` option is not passed or when the `--start-http-api` option is passed, `chainhook` will instantiate a REST API allowing developers to list, add, and removes predicates at runtime:

```bash
$ chainhook service start --config-path=./path/to/config.toml
```

```bash
$ chainhook service start --predicate-path=./path/to/predicate-1.json --start-http-api --config-path=./path/to/config.toml
```

A comprehensive OpenAPI specification explaining how to interact with the Chainhook REST API can be found [here](./docs/chainhook-openapi.json).
