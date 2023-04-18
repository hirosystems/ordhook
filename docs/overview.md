---
title: Chainhook Overview
---

# What is Chainhook?

Chainhook is a fork-aware transaction indexing engine aiming at helping developers focus on the information they need by helping with the on-chain data extraction. By focusing on the data they care about, developers work with much lighter datasets.

## What problem does it solve?

Crypto applications can improve customer experiences by promptly delivering business outcomes in response to blockchain events. However, developers face significant time, energy, and operational overhead costs when running blockchain indexers and writing additional code to crawl through real-time blockchain events.

To address this issue, Chainhooks takes away all that burden from developers, so they can focus on automating the trigger of predefined operations whenever blockchain events occur.

## Features of Chainhooks

1. **Improved Developer Experience**: instead of working with a generic blockchain indexer, taking hours to process every single transactions of every single block, developers can create their own indexes, build, iterate and refine in minutes.
   
2. **Cost Optimization**: by avoiding full chain indexation, developers avoid massive storage management and unnecessary storage scaling issues. Also chainhook helps developers creating elegant event based architectures. Developers write if_this / then_that predicates, being evaluated on transactions and blocks. When the evaluation of these predicates appears to be true, the related transactions are packaged as event and forwarded to the configured destination. By using cloud functions as destinations, developers can also cut costs on processing, by only paying processing when a block that contains some data relevant to developer's application is being mined.
   
3. **Optimized User Experience**: lighter indexes implies faster queries results, which helps minimizing end user responses time.

## Chain hook triggers

With chainhooks, developers can trigger actions, based on predicates they can write. Chainhooks support following predicates non-exhaustively:

- A certain amount of SIP-10 tokens were transferred.
- A particular blockchain address received some tokens on the Stacks/Bitcoin blockchain.
- A particular print event was emitted by a contract.
- A particular contract was involved in a transaction.
- A quantity of BTC was received on a Bitcoin address.
- A POX transfer occurred on the Bitcoin chain.


