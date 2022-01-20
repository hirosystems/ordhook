
# Clarity for Visual Studio Code

Clarity is a **decidable** smart contract language that optimizes for predictability and security, designed by Blockstack. Smart contracts allow developers to encode essential business logic on a blockchain. 

A programming language is decidable if you can know, with certainty, from the code itself what the program will do. Clarity is intentionally Turing incomplete as it avoids `Turing complexity`. This allows for complete static analysis of the entire call graph of a given smart contract. Further, our support for types and type checker can eliminate whole classes of bugs like unintended casts, reentrancy bugs, and reads of uninitialized values.

The Language Server Protocol (LSP) defines the protocol used between an editor or IDE and a language server that provides language features like auto complete, go to definition, find all references etc.

This project aims at leveraging the decidability quality of Clarity and the LSP for providing some great insights about your code, without publishing your smart contracts to a blockchain.

![screenshot](doc/images/screenshot.png)

## Quick Start

### Dependencies

This extension relies on a local installation of Clarinet, at or above version 0.22.0. To install Clarinet, please follow the instructions [here](https://github.com/hirosystems/clarinet#installation).

### VSCode

You can install the latest release of the plugin from the [marketplace](https://marketplace.visualstudio.com/items?itemName=hirosystems.clarity-lsp).

### Building From Source

Alternatively, both the server and the plugin can be installed from source.

The first step is to ensure that you have Rust and the support software installed.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

From there, you can clone this repository:

```bash
git clone https://github.com/hirosystems/clarity-lsp.git

cd clarity-lsp
```

Then build and install:

```bash
cargo xtask install
```

## Initial feature set
- [x] Auto-complete native functions
- [x] Check contract on save, and display errors inline.
- [x] VS-Code support

## Additional desired features (not exhaustive, not prioritized)
- [x] Inline documentation
- [x] Auto-complete user defined functions
- [ ] Return and display cost analysis
- [x] Resolve contract-call targeting local contracts 
- [ ] Resolve contract-call targeting deployed contracts
- [x] Support for traits
- [ ] Support for multiple errors


