# ordhook-sdk-js

`ordhook-sdk-js` is a node library, designed to let developers write protocols sitting on top of the Ordinals theory.
It is implemented as a dynamic library that can be loaded by Node.

### Installation

```console
# Yarn
yarn add dev @hirosystems/ordhook-sdk-js

# NPM
npm install --save-dev @hirosystems/ordhook-sdk-js
```

If any error occurs during the installation of this package, feel free to open an issue on this repository.


### Usage

```typescript
import { OrdinalsIndexer } from "@hirosystems/ordhook-sdk-js";

let indexer = new OrdinalsIndexer({
  bitcoinRpcUrl: 'http://localhost:8332',
  bitcoinRpcUsername: 'devnet',
  bitcoinRpcPassword: 'devnet',
  workingDirectory: '/etc/ordinals/db'
  logs: true
});

indexer.applyBlock((block) => {

})

indexer.undoBlock((block) => {

})

indexer.onTermination(() => {

})

indexer.start();


// indexer.dropBlock();
// indexer.recomputeBlock();
// indexer.replayBlocks();

```

### Case Study

BRC20

### Screencasts

