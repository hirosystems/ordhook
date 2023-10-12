import test from 'ava'

import { OrdinalsIndexer } from "../index.js";

const indexer = new OrdinalsIndexer({
    bitcoinRpcUrl: 'http://0.0.0.0:8332',
    bitcoinRpcUsername: 'devnet',
    bitcoinRpcPassword: 'devnet',
    workingDirectory: '/Users/ludovic/ordhook-sdk-js',
    logs: false
});

indexer.onBlock(block => {
    console.log(`Hello from JS ${JSON.stringify(block)}`);
});

indexer.onBlockRollBack(block => {
    console.log(`Hello from JS ${JSON.stringify(block)}`);
});

indexer.replayBlocks([767430, 767431]);
