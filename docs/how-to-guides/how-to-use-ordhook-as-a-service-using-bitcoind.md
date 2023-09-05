---
title: Run Ordhook as a Service using Bitcoind
---

## Run ordhook as a service using Bitcoind

In this section, you'll learn how to run Ordhook as a service using [Chainhook SDK](https://github.com/hirosystems/chainhook/tree/develop/components/chainhook-sdk) to create a predicate ordinal theory and post events to a server.

## Prerequisites

- [Bitcoind configuration](https://docs.hiro.so/chainhook/how-to-guides/how-to-run-chainhook-as-a-service-using-bitcoind#setting-up-a-bitcoin-node)
- Configure ordhook by following [this](./how-to-explore-ordinal-activities.md#configure-ordhook) section

## Run ordhook service for streaming blocks

Use the following command to start the ordhook service for streaming and processing new blocks appended to the Bitcoin blockchain.

`ordhook service start --post-to=http://localhost:3000/api/events --config-path=./Ordhook.toml`

When the ordhook service starts, the chainhook service gets initiated in the background to augment the blocks that Bitcoind sends. The bitcoind sends zeromq notifications to Ordhook to retrieve the inscriptions.

### Add `http-post` endpoints dynamically

To enable dynamically posting endpoints to the server, you can spin up the redis server by enabling the following lines of code in the `ordhook.toml` file.

```toml
[http_api]
http_port = 20456
database_uri = "redis://localhost:6379/"
```

## Run ordhook service

Based on the `Ordhook.toml` file configuration, the ordhook service spins up an HTTP API to manage event destinations. Use the following command to start the ordhook service.

`ordhook service start --config-path=./Ordhook.toml`

A comprehensive OpenAPI specification explaining how to interact with this HTTP REST API can be found [here](https://github.com/hirosystems/chainhook/blob/develop/docs/chainhook-openapi.json).
