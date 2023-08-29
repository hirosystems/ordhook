import { OrdinalsIndexer } from "./index";

const indexer = new OrdinalsIndexer({
    bitcoinRpcUrl: 'http://0.0.0.0:8332',
    bitcoinRpcUsername: 'devnet',
    bitcoinRpcPassword: 'devnet',
    workingDirectory: '/Users/ludovic/ordhook-sdk-js',
    logs: false
});

indexer.applyBlock(block => {
    console.log(`Hello from JS ${JSON.stringify(block)}`);
});

indexer.undoBlock(block => {
    console.log(`Hello from JS ${JSON.stringify(block)}`);
});

indexer.start();
