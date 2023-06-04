import { Static, TSchema, Type } from '@sinclair/typebox';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const RosettaOperationIdentifierSchema = Type.Object({
  index: Type.Integer(),
  network_index: Type.Optional(Type.Integer()),
});
export type RosettaOperationIdentifier = Static<typeof RosettaOperationIdentifierSchema>;

export const RosettaOperationTypeSchema = Type.Union([
  Type.Literal('CREDIT'),
  Type.Literal('DEBIT'),
  Type.Literal('LOCK'),
]);
export type RosettaOperationType = Static<typeof RosettaOperationTypeSchema>;

export const RosettaOperationAccountSchema = Type.Object({
  address: Type.String(),
  sub_account: Type.Optional(Type.String()),
});
export type RosettaOperationAccount = Static<typeof RosettaOperationAccountSchema>;

export const RosettaOperationAmountSchema = Type.Object({
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
export type RosettaOperationAmount = Static<typeof RosettaOperationAmountSchema>;

export const RosettaOperationPublicKeySchema = Type.Object({
  hex_bytes: Type.Optional(Type.String()),
  curve_type: Type.String(),
});
export type RosettaOperationPublicKey = Static<typeof RosettaOperationPublicKeySchema>;

export const RosettaOperationMetadataSchema = Type.Object({
  public_key: Type.Optional(RosettaOperationPublicKeySchema),
  code: Type.Optional(Type.String()),
  method_name: Type.Optional(Type.String()),
  args: Type.Optional(Type.String()),
});
export type RosettaOperationMetadata = Static<typeof RosettaOperationMetadataSchema>;

export const RosettaOperationSchema = Type.Object({
  account: RosettaOperationAccountSchema,
  amount: Type.Optional(RosettaOperationAmountSchema),
  metadata: Type.Optional(RosettaOperationMetadataSchema),
  operation_identifier: RosettaOperationIdentifierSchema,
  related_operations: Type.Optional(Type.Array(RosettaOperationIdentifierSchema)),
  status: Type.Optional(Type.Literal('SUCCESS')),
  type: RosettaOperationTypeSchema,
});
export type RosettaOperation = Static<typeof RosettaOperationSchema>;

export const BlockIdentifierSchema = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});
export type BlockIdentifier = Static<typeof BlockIdentifierSchema>;

export const TransactionIdentifierSchema = Type.Object({
  hash: Type.String(),
});
export type TransactionIdentifier = Static<typeof TransactionIdentifierSchema>;
