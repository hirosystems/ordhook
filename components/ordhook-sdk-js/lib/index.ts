"use strict";

const {
  ordinalsIndexerNew,
  ordinalsIndexerStreamBlocks,
  ordinalsIndexerReplayBlocks,
  ordinalsIndexerDropBlocks,
  ordinalsIndexerSyncBlocks,
  ordinalsIndexerRewriteBlocks,
  ordinalsIndexerOnBlockApply,
  ordinalsIndexerOnBlockUndo,
  ordinalsIndexerTerminate,
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
   * @summary Start streaming blocks
   * @memberof OrdinalsIndexer
   */
  streamBlocks() {
    return ordinalsIndexerStreamBlocks.call(this.handle);
  }

  /**
   * @summary Drop a set of blocks
   * @memberof OrdinalsIndexer
   */
  dropBlocks(blocks: number[]) {
    return ordinalsIndexerDropBlocks.call(this.handle, blocks);
  }

  /**
   * @summary Drop, downloard and re-index a set of blocks
   * @memberof OrdinalsIndexer
   */
  rewriteBlocks(blocks: number[]) {
    return ordinalsIndexerRewriteBlocks.call(this.handle, blocks);
  }

  /**
   * @summary Replay a set of blocks
   * @memberof OrdinalsIndexer
   */
  replayBlocks(blocks: number[]) {
    return ordinalsIndexerReplayBlocks.call(this.handle, blocks);
  }

  /**
   * @summary Download and index blocks
   * @memberof OrdinalsIndexer
   */
  syncBlocks() {
    return ordinalsIndexerSyncBlocks.call(this.handle);
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

  /**
   * @summary Terminates indexer
   * @memberof DevnetNetworkOrchestrator
   */
  terminate() {
    return ordinalsIndexerTerminate.call(this.handle);
  }
}
