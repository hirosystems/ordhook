---
title: Overview
---

# Chainhooks Overview

Chainhook is a fork-aware transaction indexing engine aiming at helping developers focus on the information they need by helping with the on-chain data extraction. By focusing on the data they care about, developers work with much lighter datasets.

Chainhooks can be used as a tool in your local development environment and as a service in the cloud environment.

## What problem does it solve?

Crypto applications can improve customer experiences by promptly delivering business outcomes in response to blockchain events. However, developers face significant time, energy, and operational overhead costs when running blockchain indexers and writing additional code to crawl through real-time blockchain events.

To address this issue, Chainhooks takes away all that burden from developers, so they can focus on automating the trigger of predefined operations whenever blockchain events occur.

## Features of Chainhooks

1. **Improved Developer Experience**: instead of working with a generic blockchain indexer, taking hours to process every single transaction of every single block, developers can create their own indexes, build, iterate, and refine in minutes.
   
2. **Cost Optimization**: developers avoid massive storage management and unnecessary storage scaling issues by avoiding full chain indexation. Also, chainhook helps developers create elegant event-based architectures. Developers write if_this / then_that predicates, being evaluated on transactions and blocks. When the evaluation of these predicates appears to be true, the related transactions are packaged as events and forwarded to the configured destination. By using cloud functions as destinations, developers can also cut costs on processing by only paying for processing when a block that contains some data relevant to the developer's application is being mined.
   
3. **Optimized User Experience**: lighter indexes implies faster query results, which helps minimize end-user response time.

## Chainhook triggers

With chainhooks, developers can trigger actions based on predicates they can write. Chainhooks support the following predicates non-exhaustively:

- A certain amount of SIP-10 tokens were transferred.
- A particular blockchain address received some tokens on the Stacks/Bitcoin blockchain.
- A particular print event was emitted by a contract.
- A particular contract was involved in a transaction.
- A quantity of BTC was received at a Bitcoin address.
- A POX transfer occurred on the Bitcoin chain.
