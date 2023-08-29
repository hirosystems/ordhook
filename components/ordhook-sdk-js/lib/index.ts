"use strict";

const {
  ordinalsIndexerNew,
  ordinalsIndexerStart,
  ordinalsIndexerOnBlockApply,
  ordinalsIndexerOnBlockUndo,
} = require("../native/index.node");

// import {
//   BitcoinChainUpdate,
//   Block,
//   StacksBlockMetadata,
//   StacksBlockUpdate,
//   StacksChainUpdate,
//   StacksTransaction,
//   StacksTransactionMetadata,
//   Transaction,
// } from "@hirosystems/chainhook-types";
// export * from "@hirosystems/chainhook-types";

export class OrdinalsIndexer {
  handle: any;

  /**
   * @summary Construct a new OrdinalsIndexer
   * @param 
   * @memberof OrdinalsIndexer
   */
  constructor(settings: {
    bitcoinRpcUrl: string,
    bitcoinRpcUsername: string,
    bitcoinRpcPassword: string,
    workingDirectory: string,
    logs: boolean,
  }) {
    this.handle = ordinalsIndexerNew(settings);
  }

  /**
   * @summary Start indexing ordinals
   * @memberof OrdinalsIndexer
   */
  start() {
    return ordinalsIndexerStart.call(this.handle);
  }

  /**
   * @summary Apply Block
   * @memberof OrdinalsIndexer
   */
  applyBlock(callback: (block: any) => void) {
    return ordinalsIndexerOnBlockApply.call(this.handle, callback);
  }

  /**
   * @summary Undo Block
   * @memberof OrdinalsIndexer
   */
  undoBlock(callback: (block: any) => void) {
    return ordinalsIndexerOnBlockUndo.call(this.handle, callback);
  }

  // /**
  //  * @summary Terminates the containers
  //  * @memberof DevnetNetworkOrchestrator
  //  */
  // terminate(): boolean {
  //   return stacksDevnetTerminate.call(this.handle);
  // }
}
