import { Static, TSchema, Type } from '@sinclair/typebox';
import { TypeCompiler } from '@sinclair/typebox/compiler';
import { StacksEvent } from './stacks';
import { BitcoinEvent } from './bitcoin';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const BlockIdentifier = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});

export const TransactionIdentifier = Type.Object({
  hash: Type.String()
});

const EventArray = Type.Union([
  Type.Array(StacksEvent),
  Type.Array(BitcoinEvent)
]);

const Payload = Type.Object({
  apply: EventArray,
  rollback: EventArray,
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: Type.Object({
      scope: Type.String(),
      operation: Type.String(),
    }),
  }),
});
export type Payload = Static<typeof Payload>;
export const PayloadCType = TypeCompiler.Compile(Payload);
