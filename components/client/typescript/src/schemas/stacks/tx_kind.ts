import { Static, Type } from '@sinclair/typebox';
import { StacksPrincipalSchema } from './payload';

export const StacksTransactionContractCallKindSchema = Type.Object({
  type: Type.Literal('ContractCall'),
  data: Type.Object({
    args: Type.Array(Type.String()),
    contract_identifier: StacksPrincipalSchema,
    method: Type.String(),
  }),
});
export type StacksTransactionContractCallKind = Static<
  typeof StacksTransactionContractCallKindSchema
>;

export const StacksTransactionContractDeploymentKindSchema = Type.Object({
  type: Type.Literal('ContractDeployment'),
  data: Type.Object({
    contract_identifier: StacksPrincipalSchema,
    code: Type.String(),
  }),
});
export type StacksTransactionContractDeploymentKind = Static<
  typeof StacksTransactionContractDeploymentKindSchema
>;

export const StacksTransactionCoinbaseKindSchema = Type.Object({
  type: Type.Literal('Coinbase'),
});
export type StacksTransactionCoinbaseKind = Static<typeof StacksTransactionCoinbaseKindSchema>;

export const StacksTransactionNativeTokenTransferKindSchema = Type.Object({
  type: Type.Literal('NativeTokenTransfer'),
});
export type StacksTransactionNativeTokenTransferKind = Static<
  typeof StacksTransactionNativeTokenTransferKindSchema
>;

export const StacksTransactionBitcoinOpStackStxKindSchema = Type.Object({
  type: Type.Literal('BitcoinOp'),
  data: Type.Object({
    locked_amount: Type.String(),
    stacking_address: Type.String(),
    unlock_height: Type.String(),
  }),
});
export type StacksTransactionBitcoinOpStackStxKind = Static<
  typeof StacksTransactionBitcoinOpStackStxKindSchema
>;

export const StacksTransactionBitcoinOpDelegateStackStxKindSchema = Type.Object({
  type: Type.Literal('BitcoinOp'),
  data: Type.Object({
    stacking_address: Type.String(),
    amount: Type.String(),
    delegate: Type.String(),
    pox_address: Type.Optional(Type.String()),
    unlock_height: Type.Optional(Type.String()),
  }),
});
export type StacksTransactionBitcoinOpDelegateStackStxKind = Static<
  typeof StacksTransactionBitcoinOpDelegateStackStxKindSchema
>;

export const StacksTransactionUnsupportedKindSchema = Type.Object({
  type: Type.Literal('Unsupported'),
});
export type StacksTransactionUnsupportedKind = Static<
  typeof StacksTransactionUnsupportedKindSchema
>;

export const StacksTransactionKindSchema = Type.Union([
  StacksTransactionCoinbaseKindSchema,
  StacksTransactionContractCallKindSchema,
  StacksTransactionContractDeploymentKindSchema,
  StacksTransactionNativeTokenTransferKindSchema,
  StacksTransactionBitcoinOpStackStxKindSchema,
  StacksTransactionBitcoinOpDelegateStackStxKindSchema,
  StacksTransactionUnsupportedKindSchema,
]);
export type StacksTransactionKind = Static<typeof StacksTransactionKindSchema>;
