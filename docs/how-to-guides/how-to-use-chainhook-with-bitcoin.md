---
title: Use Chainhook with Bitcoin
---

# Use Chainhook with Bitcoin

The following guide helps you define predicates to use chainhook with Bitcoin.

## Guide to `if_this` / `then_that` predicate design

To get started with Bitcoin predicates, we can use the `chainhook` to generate a template: 

```bash
$ chainhook predicates new hello-ordinals.json --bitcoin
```

The above command generates a hello-ordinals.json file that looks like this:

```
{
  "chain": "bitcoin",
  "uuid": "a618b9ab-b836-43c9-954d-e31e8940322e",
  "name": "Hello world",
  "version": 1,
  "networks": {
    "mainnet": {
      "start_block": 0,
      "end_block": 100,
      "if_this": {
        "scope": "ordinals_protocol",
        "operation": "inscription_feed"
      },
      "then_that": {
        "file_append": {
          "path": "ordinals.txt"
        }
      }
    }
  }
}
```

##  `if_this` and `then_that` specifications

The current `bitcoin` predicates support the following `if_this` constructs. Get any transaction matching a given txid. The `txid` mandatory argument admits: - 32 bytes hex encoded type. 


```json

{
    "if_this": {
        "scope": "txid",
        "equals": "0xfaaac1833dc4883e7ec28f61e35b41f896c395f8d288b1a177155de2abd6052f"
    }
}
```
Get any transaction, including an OP_RETURN output starting with a set of characters. The `starts_with` mandatory argument admits:
 - ASCII string type. example: `X2[`
 - hex encoded bytes. example: `0x589403`
```json
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "starts_with": "X2["
        }
    }
}
```

Get any transaction, including an OP_RETURN output matching the sequence of bytes specified `equals` mandatory argument admits:
 - hex encoded bytes. example: `0x589403`
```json
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "equals": "0x69bd04208265aca9424d0337dac7d9e84371a2c91ece1891d67d3554bd9fdbe60afc6924d4b0773d90000006700010000006600012"
        }
    }
}
```

Get any transaction, including an OP_RETURN output ending with a set of characters `ends_with` mandatory argument admits:
- ASCII string type. example: `X2[`
- hex encoded bytes. example: `0x589403`

```json
{
    "if_this": {
        "scope": "outputs",
        "op_return": {
            "ends_with": "0x76a914000000000000000000000000000000000000000088ac"
        }
    }
}
```

Get any transaction including a p2pkh output paying a given recipient `p2pkh` construct admits:
- string type. example: "mr1iPkD9N3RJZZxXRk7xF9d36gffa6exNC"
- hex encoded bytes type. example: "0x76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac"

```json
{
    "if_this": {
        "scope": "outputs",
        "p2pkh": "mr1iPkD9N3RJZZxXRk7xF9d36gffa6exNC"
    }
}
```

Get any transaction including a p2sh output paying a given recipient `p2sh` construct admits:
- string type. example: "2MxDJ723HBJtEMa2a9vcsns4qztxBuC8Zb2"
- hex encoded bytes type. example: "0x76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac"

```json
{
    "if_this": {
        "scope": "outputs",
        "p2sh": "2MxDJ723HBJtEMa2a9vcsns4qztxBuC8Zb2"
    }
}
```

Get any transaction including a p2wpkh output paying a given recipient `p2wpkh` construct admits:
- string type. example: "bcrt1qnxknq3wqtphv7sfwy07m7e4sr6ut9yt6ed99jg"

```json
{
    "if_this": {
        "scope": "outputs",
        "p2wpkh": "bcrt1qnxknq3wqtphv7sfwy07m7e4sr6ut9yt6ed99jg"
    }
}
```

Get any transaction including a p2wsh output paying a given recipient `p2wsh` construct admits:
- string type. example: "bc1qklpmx03a8qkv263gy8te36w0z9yafxplc5kwzc"

```json
{
    "if_this": {
        "scope": "outputs",
        "p2wsh": "bc1qklpmx03a8qkv263gy8te36w0z9yafxplc5kwzc"
    }
}
```

Get any Bitcoin transaction, including a Block commitment. Broadcasted payloads include Proof of Transfer reward information.

```json
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "block_committed"
    }
}
```
Get any transaction, including a key registration operation
```json
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "leader_key_registered"
    }
}
```

Get any transaction, including an STX transfer operation 
// Coming soon
```json
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "stx_transferred"
    }
}
```

Get any transaction, including an STX lock operation
// Coming soon
```json
{
    "if_this": {
        "scope": "stacks_protocol",
        "operation": "stx_locked"
    }
}
```
Get any transaction including a new Ordinal inscription (inscription revealed and transferred)
```json
{
    "if_this": {
        "scope": "ordinals_protocol",
        "operation": "inscription_feed"
    }
}
```

In terms of actions available, the following `then_that` constructs are supported:

HTTP Post block/transaction payload to a given endpoint. The `http_post` construct admits:
- url (string type). Example: http://localhost:3000/api/v1/wrapBtc
- authorization_header (string type). Secret to add to the request `authorization` header when posting payloads

```jsonc

{
    "then_that": {
        "http_post": {
            "url": "http://localhost:3000/api/v1/wrapBtc",
            "authorization_header": "Bearer cn389ncoiwuencr"
        }
    }
}

Append events to a file through the filesystem. Convenient for local tests. The `file_append` construct admits:
- path (string type). Path to the file on disk.

```jsonc
{
    "then_that": {
        "file_append": {
            "path": "/tmp/events.json",
        }
    }
}
```

## Additional configuration knobs available

```json
// Ignore any block before the given block:
"start_block": 101

// Ignore any block after the given block:
"end_block": 201

// Stop evaluating chainhook after a given number of occurrences found:
"expire_after_occurrence": 1

// Include proof:
"include_proof": false

// Include Bitcoin transaction inputs in the payload:
"include_inputs": false

// Include Bitcoin transaction outputs in the payload:
"include_outputs": false

// Include Bitcoin transaction witness in the payload:
"include_witness": false

```

## Putting all the above configurations together

Retrieve and HTTP Post to `http://localhost:3000/api/v1/wrapBtc` the five first transfers to the p2wpkh `bcrt1qnxk...yt6ed99jg` address of any amount, occurring after block height 10200.

```json
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
```

## Another example

A specification file can also include different networks. In this case, the chainhook will select the predicate corresponding to the network it was launched against.


```json
{
  "chain": "bitcoin",
  "uuid": "1",
  "name": "Wrap BTC",
  "version": 1,
  "networks": {
    "testnet": {
      "if_this": {
        "scope": "ordinals_protocol",
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
        "scope": "ordinals_protocol",
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

## Guide to local Bitcoin testnet / mainnet predicate scanning

To scan the Bitcoin chain with a given predicate, a `bitcoind` instance with access to the RPC methods `getblockhash` and `getblock` must be accessible. The RPC calls latency will directly impact the speed of the scans.

:::note

Configuring a `bitcoind` instance is out of the scope of this guide.

:::


Assuming a `bitcoind` node is correctly configured, you can perform scans using the following command:

```bash
$ chainhook predicates scan ./path/to/predicate.json --testnet
```
When using the flag `--testnet`, the scan operation will generate a configuration file in memory using the following settings:

```toml
[storage]
driver = "memory"

[chainhooks]
max_stacks_registrations = 500
max_bitcoin_registrations = 500

[network]
mode = "testnet"
bitcoind_rpc_url = "http://0.0.0.0:18332"
bitcoind_rpc_username = "testnet"
bitcoind_rpc_password = "testnet"
stacks_node_rpc_url = "http://0.0.0.0:20443"
```

When using the flag `--mainnet`, the scan operation will generate a configuration file in memory using the following settings:

```toml
[storage]
driver = "memory"

[chainhooks]
max_stacks_registrations = 500
max_bitcoin_registrations = 500

[network]
mode = "mainnet"
bitcoind_rpc_url = "http://0.0.0.0:8332"
bitcoind_rpc_username = "mainnet"
bitcoind_rpc_password = "mainnet"
stacks_node_rpc_url = "http://0.0.0.0:20443"

```

Developers can customize their Bitcoin node's credentials and network address by adding the flag `-config=/path/to/config.toml`.

```bash
$ chainhook config new --testnet
✔ Generated config file Testnet.toml

$ chainhook predicates scan ./path/predicate.json --config=./Testnet.toml
```

## Tips and tricks

To optimize your experience with scanning, the following are a few knobs you can play with:

- Use of adequate values for `start_block` and `end_block` in predicates will drastically improve the speed.
- Networking: reducing the number of network hops between the chainhook and the bitcoind processes can also help a lot.
