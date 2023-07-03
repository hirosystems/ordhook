import { Static, Type } from '@sinclair/typebox';
import { BitcoinIfThisThenThatSchema } from './bitcoin/if_this';
import { StacksIfThisThenThatSchema } from './stacks/if_this';

export const ThenThatFileAppendSchema = Type.Object({
  file_append: Type.Object({
    path: Type.String(),
  }),
});
export type ThenThatFileAppend = Static<typeof ThenThatFileAppendSchema>;

export const ThenThatHttpPostSchema = Type.Object({
  http_post: Type.Object({
    url: Type.String({ format: 'uri' }),
    authorization_header: Type.String(),
  }),
});
export type ThenThatHttpPost = Static<typeof ThenThatHttpPostSchema>;

export const ThenThatSchema = Type.Union([ThenThatFileAppendSchema, ThenThatHttpPostSchema]);
export type ThenThat = Static<typeof ThenThatSchema>;

export const PredicateHeaderSchema = Type.Object({
  uuid: Type.String({ format: 'uuid' }),
  name: Type.String(),
  version: Type.Integer(),
  chain: Type.String(),
});
export type PredicateHeader = Static<typeof PredicateHeaderSchema>;

export const PredicateSchema = Type.Composite([
  PredicateHeaderSchema,
  Type.Object({
    networks: Type.Union([
      Type.Object({
        mainnet: Type.Union([BitcoinIfThisThenThatSchema, StacksIfThisThenThatSchema]),
      }),
      Type.Object({
        testnet: Type.Union([BitcoinIfThisThenThatSchema, StacksIfThisThenThatSchema]),
      }),
    ]),
  }),
]);
export type Predicate = Static<typeof PredicateSchema>;
