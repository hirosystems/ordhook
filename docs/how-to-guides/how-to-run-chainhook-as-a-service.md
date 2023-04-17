---
title: How to run chainhook as a service
---

## Run `chainhook` as a service for streaming new blocks

`chainhook` can be run as a background service for streaming and processing new canonical blocks appended to the Bitcoin and Stacks blockchains.

When running chainhook as a service, `if_this` / `then_that` predicates can be registered by passing the path of the `JSON` file in the command line: 

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
