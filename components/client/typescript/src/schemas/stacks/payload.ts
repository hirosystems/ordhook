import { Static, Type } from '@sinclair/typebox';
import {
  BlockIdentifierSchema,
  Nullable,
  RosettaOperationSchema,
  TransactionIdentifierSchema,
} from '../common';
import { StacksTransactionEventSchema } from './tx_events';
import { StacksTransactionKindSchema } from './tx_kind';

export const StacksPrincipalSchema = Type.String();
export type StacksPrincipal = Static<typeof StacksPrincipalSchema>;

export const StacksExecutionCostSchema = Type.Optional(
  Type.Object({
    read_count: Type.Integer(),
    read_length: Type.Integer(),
    runtime: Type.Integer(),
    write_count: Type.Integer(),
    write_length: Type.Integer(),
  })
);
export type StacksExecutionCost = Static<typeof StacksExecutionCostSchema>;

export const StacksTransactionReceiptSchema = Type.Object({
  contract_calls_stack: Type.Array(Type.String()),
  events: Type.Array(StacksTransactionEventSchema),
  mutated_assets_radius: Type.Array(Type.String()),
  mutated_contracts_radius: Type.Array(Type.String()),
});
export type StacksTransactionReceipt = Static<typeof StacksTransactionReceiptSchema>;

export const StacksTransactionPositionSchema = Type.Object({
  index: Type.Integer(),
  micro_block_identifier: Type.Optional(BlockIdentifierSchema),
});
export type StacksTransactionPosition = Static<typeof StacksTransactionPositionSchema>;

export const StacksTransactionMetadataSchema = Type.Object({
  description: Type.String(),
  execution_cost: StacksExecutionCostSchema,
  fee: Type.Integer(),
  kind: StacksTransactionKindSchema,
  nonce: Type.Integer(),
  position: StacksTransactionPositionSchema,
  proof: Nullable(Type.String()),
  raw_tx: Type.String(),
  receipt: StacksTransactionReceiptSchema,
  result: Type.String(),
  sender: StacksPrincipalSchema,
  sponsor: Type.Optional(StacksPrincipalSchema),
  success: Type.Boolean(),
});
export type StacksTransactionMetadata = Static<typeof StacksTransactionMetadataSchema>;

const StacksTransactionSchema = Type.Object({
  transaction_identifier: TransactionIdentifierSchema,
  operations: Type.Array(RosettaOperationSchema),
  metadata: StacksTransactionMetadataSchema,
});
export type StacksTransaction = Static<typeof StacksTransactionSchema>;

export const StacksEventMetadataSchema = Type.Object({
  bitcoin_anchor_block_identifier: BlockIdentifierSchema,
  confirm_microblock_identifier: BlockIdentifierSchema,
  pox_cycle_index: Type.Integer(),
  pox_cycle_length: Type.Integer(),
  pox_cycle_position: Type.Integer(),
  stacks_block_hash: Type.String(),
});
export type StacksEventMetadata = Static<typeof StacksEventMetadataSchema>;

export const StacksEventSchema = Type.Object({
  block_identifier: BlockIdentifierSchema,
  parent_block_identifier: BlockIdentifierSchema,
  timestamp: Type.Integer(),
  transactions: Type.Array(StacksTransactionSchema),
  metadata: StacksEventMetadataSchema,
});
export type StacksEvent = Static<typeof StacksEventSchema>;
