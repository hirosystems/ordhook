import { Static, Type } from '@sinclair/typebox';
import {
  BlockIdentifierSchema,
  Nullable,
  OperationIdentifierSchema,
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

export const StacksOperationAccountSchema = Type.Object({
  address: StacksPrincipalSchema,
  sub_account: Type.Optional(StacksPrincipalSchema),
});
export type StacksOperationAccount = Static<typeof StacksOperationAccountSchema>;

export const StacksOperationAmountSchema = Type.Object({
  currency: Type.Object({
    decimals: Type.Integer(),
    symbol: Type.String(),
    metadata: Type.Object({
      asset_class_identifier: Type.String(),
      asset_identifier: Nullable(Type.String()),
      standard: Type.String(),
    }),
  }),
  value: Type.Integer(),
});
export type StacksOperationAmount = Static<typeof StacksOperationAmountSchema>;

export const StacksOperationPublicKeySchema = Type.Object({
  hex_bytes: Type.Optional(Type.String()),
  curve_type: Type.String(),
});
export type StacksOperationPublicKey = Static<typeof StacksOperationPublicKeySchema>;

export const StacksOperationMetadataSchema = Type.Object({
  public_key: Type.Optional(StacksOperationPublicKeySchema),
  code: Type.Optional(Type.String()),
  method_name: Type.Optional(Type.String()),
  args: Type.Optional(Type.String()),
});
export type StacksOperationMetadata = Static<typeof StacksOperationMetadataSchema>;

export const StacksOperationSchema = Type.Object({
  account: StacksOperationAccountSchema,
  amount: Type.Optional(StacksOperationAmountSchema),
  metadata: Type.Optional(StacksOperationMetadataSchema),
  operation_identifier: OperationIdentifierSchema,
  related_operations: Type.Optional(Type.Array(OperationIdentifierSchema)),
  status: Type.Optional(Type.Literal('SUCCESS')),
  type: Type.Union([Type.Literal('CREDIT'), Type.Literal('DEBIT'), Type.Literal('LOCK')]),
});
export type StacksOperation = Static<typeof StacksOperationSchema>;

const StacksTransactionSchema = Type.Object({
  transaction_identifier: TransactionIdentifierSchema,
  operations: Type.Array(StacksOperationSchema),
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
