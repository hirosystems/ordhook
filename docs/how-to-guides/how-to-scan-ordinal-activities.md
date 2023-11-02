---
title: Scan Ordinal Activities in Your Terminal
---

Ordhook is a tool that helps you find ordinal activity on the Bitcoin chain. Think of it like a detective that can find and track this activity for you. Once it finds any activity, that data can be used to help build your own database. This guide will show you how to use Ordhook to scan this activity on the Bitcoin chain in your terminal.

### Explore Ordinal Activity

You can use the following command to scan a range of blocks on mainnet or testnet:

`ordhook scan blocks 767430 767753 --mainnet`

The above command generates a `hord.sqlite.gz` file in your directory and displays inscriptions and transfers activities occurring in the range of the specified blocks.

> **_NOTE_**
> By default, Ordhook creates a folder named `ordhook` in your current directory and stores the `hord.sqlite` file there. This file is used to pull in the latest ordinal data and scan against it based on the block numbers you provide.

The output of the above command looks like this:

```
Inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 revealed at block #767430 (inscription_number 0, ordinal_number 1252201400444387)
Inscription 26482871f33f1051f450f2da9af275794c0b5f1c61ebf35e4467fb42c2813403i0 revealed at block #767753 (inscription_number 1, ordinal_number 727624168684699)
```

You can also get activity for a given inscription by using the following command:

`ordhook scan inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 --mainnet`

The above command returns ordinal activity for that inscription and also the number of transfers in the transactions.

```
Inscription 6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0 revealed at block #767430 (inscription_number 0, ordinal_number 1252201400444387)
	→ Transferred in transaction 0x2c8a11858825ae2056be90c3e49938d271671ac4245b452cd88b1475cbea8971 (block #785391)
	→ Transferred in transaction 0xbc4c30829a9564c0d58e6287195622b53ced54a25711d1b86be7cd3a70ef61ed (block #785396)
Number of transfers: 2
```
