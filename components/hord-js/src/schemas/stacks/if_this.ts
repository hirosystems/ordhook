import { Static, Type } from '@sinclair/typebox';
import { ThenThatSchema } from '../predicate';

export const StacksIfThisTxIdSchema = Type.Object({
  scope: Type.Literal('txid'),
  equals: Type.String(),
});
export type StacksIfThisTxId = Static<typeof StacksIfThisTxIdSchema>;

export const StacksIfThisBlockHeightHigherThanSchema = Type.Object({
  scope: Type.Literal('block_height'),
  higher_than: Type.Integer(),
});
export type StacksIfThisBlockHeightHigherThan = Static<
  typeof StacksIfThisBlockHeightHigherThanSchema
>;

export const StacksIfThisFtEventSchema = Type.Object({
  scope: Type.Literal('ft_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});
export type StacksIfThisFtEvent = Static<typeof StacksIfThisFtEventSchema>;

export const StacksIfThisNftEventSchema = Type.Object({
  scope: Type.Literal('nft_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});
export type StacksIfThisNftEvent = Static<typeof StacksIfThisNftEventSchema>;

export const StacksIfThisStxEventSchema = Type.Object({
  scope: Type.Literal('stx_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});
export type StacksIfThisStxEvent = Static<typeof StacksIfThisStxEventSchema>;

export const StacksIfThisPrintEventSchema = Type.Object({
  scope: Type.Literal('print_event'),
  contract_identifier: Type.String(),
  contains: Type.String(),
});
export type StacksIfThisPrintEvent = Static<typeof StacksIfThisPrintEventSchema>;

export const StacksIfThisContractCallSchema = Type.Object({
  scope: Type.Literal('contract_call'),
  contract_identifier: Type.String(),
  method: Type.String(),
});
export type StacksIfThisContractCall = Static<typeof StacksIfThisContractCallSchema>;

export const StacksIfThisContractDeploymentSchema = Type.Object({
  scope: Type.Literal('contract_deployment'),
  deployer: Type.String(),
});
export type StacksIfThisContractDeployment = Static<typeof StacksIfThisContractDeploymentSchema>;

export const StacksIfThisContractDeploymentTraitSchema = Type.Object({
  scope: Type.Literal('contract_deployment'),
  implement_trait: Type.String(),
});
export type StacksIfThisContractDeploymentTrait = Static<
  typeof StacksIfThisContractDeploymentTraitSchema
>;

export const StacksIfThisOptionsSchema = Type.Object({
  start_block: Type.Optional(Type.Integer()),
  end_block: Type.Optional(Type.Integer()),
  expire_after_occurrence: Type.Optional(Type.Integer()),
  decode_clarity_values: Type.Optional(Type.Boolean()),
});
export type StacksIfThisOptions = Static<typeof StacksIfThisOptionsSchema>;

export const StacksIfThisSchema = Type.Union([
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
export type StacksIfThis = Static<typeof StacksIfThisSchema>;

export const StacksIfThisThenThatSchema = Type.Composite([
  StacksIfThisOptionsSchema,
  Type.Object({
    if_this: StacksIfThisSchema,
    then_that: ThenThatSchema,
  }),
]);
export type StacksIfThisThenThat = Static<typeof StacksIfThisThenThatSchema>;
