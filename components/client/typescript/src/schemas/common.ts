import { Static, TSchema, Type } from '@sinclair/typebox';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const OperationIdentifierSchema = Type.Object({
  index: Type.Integer(),
});
export type OperationIdentifier = Static<typeof OperationIdentifierSchema>;

export const BlockIdentifierSchema = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});
export type BlockIdentifier = Static<typeof BlockIdentifierSchema>;

export const TransactionIdentifierSchema = Type.Object({
  hash: Type.String(),
});
export type TransactionIdentifier = Static<typeof TransactionIdentifierSchema>;
