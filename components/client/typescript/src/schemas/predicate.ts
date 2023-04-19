import { Static, Type } from '@sinclair/typebox';
import {
  BitcoinIfThisTxIdSchema,
  BitcoinIfThisOpReturnStartsWithSchema,
  BitcoinIfThisOpReturnEqualsSchema,
  BitcoinIfThisOpReturnEndsWithSchema,
  BitcoinIfThisP2PKHSchema,
  BitcoinIfThisP2SHSchema,
  BitcoinIfThisP2WPKHSchema,
  BitcoinIfThisP2WSHSchema,
  BitcoinIfThisStacksBlockCommittedSchema,
  BitcoinIfThisStacksLeaderKeyRegisteredSchema,
  BitcoinIfThisStacksStxTransferredSchema,
  BitcoinIfThisStacksStxLockedSchema,
  BitcoinIfThisOrdinalsFeedSchema,
} from './bitcoin/predicate';
import {
  StacksIfThisTxIdSchema,
  StacksIfThisBlockHeightHigherThanSchema,
  StacksIfThisFtEventSchema,
  StacksIfThisNftEventSchema,
  StacksIfThisStxEventSchema,
  StacksIfThisPrintEventSchema,
  StacksIfThisContractCallSchema,
  StacksIfThisContractDeploymentSchema,
  StacksIfThisContractDeploymentTraitSchema,
} from './stacks/predicate';

export const IfThisSchema = Type.Union([
  BitcoinIfThisTxIdSchema,
  BitcoinIfThisOpReturnStartsWithSchema,
  BitcoinIfThisOpReturnEqualsSchema,
  BitcoinIfThisOpReturnEndsWithSchema,
  BitcoinIfThisP2PKHSchema,
  BitcoinIfThisP2SHSchema,
  BitcoinIfThisP2WPKHSchema,
  BitcoinIfThisP2WSHSchema,
  BitcoinIfThisStacksBlockCommittedSchema,
  BitcoinIfThisStacksLeaderKeyRegisteredSchema,
  BitcoinIfThisStacksStxTransferredSchema,
  BitcoinIfThisStacksStxLockedSchema,
  BitcoinIfThisOrdinalsFeedSchema,
  StacksIfThisTxIdSchema,
  StacksIfThisBlockHeightHigherThanSchema,
  StacksIfThisFtEventSchema,
  StacksIfThisNftEventSchema,
  StacksIfThisStxEventSchema,
  StacksIfThisPrintEventSchema,
  StacksIfThisContractCallSchema,
  StacksIfThisContractDeploymentSchema,
  StacksIfThisContractDeploymentTraitSchema,
]);
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
export type ThenThat = Static<typeof ThenThatSchema>;

export const IfThisThenThatSchema = Type.Object({
  start_block: Type.Optional(Type.Integer()),
  end_block: Type.Optional(Type.Integer()),
  expire_after_occurrence: Type.Optional(Type.Integer()),
  include_proof: Type.Optional(Type.Boolean()),
  include_inputs: Type.Optional(Type.Boolean()),
  include_outputs: Type.Optional(Type.Boolean()),
  include_witness: Type.Optional(Type.Boolean()),
  if_this: IfThisSchema,
  then_that: ThenThatSchema,
});
export type IfThisThenThat = Static<typeof IfThisThenThatSchema>;

export const PredicateSchema = Type.Object({
  uuid: Type.String({ format: 'uuid' }),
  name: Type.String(),
  version: Type.Integer(),
  chain: Type.String(),
  networks: Type.Union([
    Type.Object({
      mainnet: IfThisThenThatSchema,
    }),
    Type.Object({
      testnet: IfThisThenThatSchema,
    }),
  ]),
});
export type Predicate = Static<typeof PredicateSchema>;
