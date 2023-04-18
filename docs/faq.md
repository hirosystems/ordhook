---
title: FAQ's
---

# Frequently Asked Questions

#### **Can Chainhooks target both bitcoin and stacks?**

Chainhooks can listen and act on events from both bitcoin and stacks network.

#### **Can I use chainhooks for cross-chain protocols?**

Yes, Chainhooks can be used for coordinating cross-chain actions. You can use chainhooks on Bitcoin, ordinals and Stacks.

#### **Can I use chainhooks for chain-indexing?**

Chainhooks can easily extract the information that they need for building (or rebuilding) databases they need for their frontend.

#### **Can I use chainhooks with distributed nodes?**

The chainhook event observer was designed as a library written in Rust, which makes it very portable. Bindings can easily be created from other languages (Node, Ruby, Python, etc), making this tool a very convenient and performant library, usable by anyone.

#### **How can I connect chainhooks with Oracles?**

An event emitted on-chain triggers a centralized logic that can be committed on-chain once computed.

#### **How can I use Chainhook in my application?**

Chainhook can be used from the exposed RESTful API endpoints. A comprehensive OpenAPI specification explaining how to interact with the Chainhook REST API can be found [here](../docs/chainhook-openapi.json).

#### **Can I run chainhooks on mainnet?**

Yes, you can run chainhooks on both testnet and mainnet.

### **How can I optimize chainhook scanning?**

Use of adequate values for `start_block` and `end_block` in predicates

Networking: reducing the number of network hops between chainhook and `bitcoind` process
