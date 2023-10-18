---
title: Overview
---

## Ordinal Theory

Ordinal Theory is a protocol with a specific goal: assigning unique identifiers to every single satoshi (sat). The protocol achieves this by introducing a numbering scheme. This numbering allows for the inclusion of arbitrary content on these satoshis.

This content is referred to as _inscriptions_. These inscriptions essentially turn satoshis into Bitcoin-native digital artifacts, often referred to as Non-Fungible Tokens (NFTs). This means that you can associate additional information or data with individual satoshis.

Ordinal Theory accomplishes this without requiring the use of sidechains or creating separate tokens. This makes it an attractive option for those looking to integrate, expand, or utilize this functionality within the Bitcoin ecosystem.

Satoshis with inscriptions can be transferred through standard Bitcoin transactions, sent to regular Bitcoin addresses, and held in Bitcoin Unspent Transaction Outputs (UTXOs). The uniqueness of these transactions is identified while sending individual satoshis with inscriptions, and transactions must be crafted to control the order and value of inputs and outputs following the rules defined by Ordinal Theory.

## Ordhook

Ordhook is an indexer designed to assist developers in creating new applications that are resistant to blockchain re-organizations (re-orgs) and are built on top of the [Ordinal Theory](https://trustmachines.co/glossary/ordinal-theory) protocol. Its primary purpose is to simplify the process for both protocol developers and users to track and discover the ownership of Ordinal Theory's inscriptions. It also provides a wealth of information about each inscription.

Ordhook utilizes the Chainhook Software Development Kit (SDK) from the Chainhook project. This SDK is a transaction indexing engine that is aware of re-orgs and is designed for use with both Stacks and Bitcoin. The Chainhook SDK operates on first-class event-driven principles. This means it efficiently extracts transaction data from blocks and maintains a consistent view of the state of the blockchain. This helps ensure that the data retrieved remains accurate and up-to-date.

Ordhook offers a valuable tool for Bitcoin developers. They can rely on it to implement feature-rich protocols and business models that make use of near-real-time information related to ordinal inscriptions and their transfers.
