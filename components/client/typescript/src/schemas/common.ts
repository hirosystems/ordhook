import { TSchema, Type } from '@sinclair/typebox';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const BlockIdentifier = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});

export const TransactionIdentifier = Type.Object({
  hash: Type.String(),
});
