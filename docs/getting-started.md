---
title: Getting Started
---

Ordhook is a developer tool designed for interacting with [Bitcoin ordinals](https://www.hiro.so/blog/what-are-bitcoin-ordinals), enabling you to retrieve ordinal activity from the Bitcoin chain. Follow the steps below to install Ordhook.

## Installing Ordhook

> **_NOTE_**
> Before proceeding, make sure you have the Rust and Cargo (Rust's package manager) installed on your computer.

### Clone the Repository

Open your terminal and navigate to the directory where you want to clone the Ordhook repository. Then run the following command to clone the repository:

```bash
git clone https://github.com/hirosystems/ordhook.git
```

This will download the Ordhook source code to your local machine.

### Navigate to the Ordhook Directory

Move into the newly cloned Ordhook directory by running the following command:

```bash
cd ordhook
```

You are now inside the Ordhook directory.

### Install Ordhook

> **_NOTE:_**
>
> Ordhook requires Rust to be installed on your system. If you haven't installed Rust yet, you can do so by following the instructions on the [official Rust website](https://www.rust-lang.org/tools/install).

Use the Rust package manager, Cargo, to install Ordhook. Run the following command:

```bash
cargo ordhook-install
```

This command compiles the Ordhook code and installs it on your system.

### Next Steps

With Ordhook installed, you can:

- Scan blocks in your terminal. See the [Scanning Ordinal Activities](./how-to-guides/how-to-scan-ordinal-activities.md) guide.
- Stream ordinal activity to an external API. Refer to the [Streaming Ordinal Activities](./how-to-guides/how-to-stream-ordinal-activities.md) guide.
