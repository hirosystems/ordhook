import { Static, TSchema, Type } from '@sinclair/typebox';
import { StacksEvent } from './stacks';
import { BitcoinEvent } from './bitcoin';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const BlockIdentifier = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});

export const TransactionIdentifier = Type.Object({
  hash: Type.String(),
});

const EventArray = Type.Union([Type.Array(StacksEvent), Type.Array(BitcoinEvent)]);

export const IfThisSchema = Type.Object({
  scope: Type.String(),
  operation: Type.String(),
});
export type IfThis = Static<typeof IfThisSchema>;

export const ThenThatSchema = Type.Union([
  Type.Object({
    file_append: Type.Object({
      path: Type.String(),
    }),
  }),
  Type.Object({
    http_post: Type.Object({
      url: Type.String({ format: 'uri' }),
      authorization_header: Type.String(),
    }),
  }),
]);

export const IfThisThenThatSchema = Type.Object({
  start_block: Type.Optional(Type.Integer()),
  end_block: Type.Optional(Type.Integer()),
  if_this: IfThisSchema,
  then_that: ThenThatSchema,
});

export const PredicateSchema = Type.Object({
  uuid: Type.String({ format: 'uuid' }),
  name: Type.String(),
  version: Type.Integer(),
  chain: Type.String(),
  networks: Type.Object({
    mainnet: Type.Optional(IfThisThenThatSchema),
    testnet: Type.Optional(IfThisThenThatSchema),
  }),
});
export type Predicate = Static<typeof PredicateSchema>;

export const PayloadSchema = Type.Object({
  apply: EventArray,
  rollback: EventArray,
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: IfThisSchema,
  }),
});
export type Payload = Static<typeof PayloadSchema>;
