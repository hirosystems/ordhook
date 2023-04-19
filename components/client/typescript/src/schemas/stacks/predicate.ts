import { Type } from '@sinclair/typebox';

export const StacksIfThisTxIdSchema = Type.Object({
  scope: Type.Literal('txid'),
  equals: Type.String(),
});

export const StacksIfThisBlockHeightHigherThanSchema = Type.Object({
  scope: Type.Literal('block_height'),
  higher_than: Type.Integer(),
});

export const StacksIfThisFtEventSchema = Type.Object({
  scope: Type.Literal('ft_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});

export const StacksIfThisNftEventSchema = Type.Object({
  scope: Type.Literal('nft_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});

export const StacksIfThisStxEventSchema = Type.Object({
  scope: Type.Literal('stx_event'),
  asset_identifier: Type.String(),
  actions: Type.Array(Type.String()),
});

export const StacksIfThisPrintEventSchema = Type.Object({
  scope: Type.Literal('print_event'),
  contract_identifier: Type.String(),
  contains: Type.String(),
});

export const StacksIfThisContractCallSchema = Type.Object({
  scope: Type.Literal('contract_call'),
  contract_identifier: Type.String(),
  method: Type.String(),
});

export const StacksIfThisContractDeploymentSchema = Type.Object({
  scope: Type.Literal('contract_deployment'),
  deployer: Type.String(),
});

export const StacksIfThisContractDeploymentTraitSchema = Type.Object({
  scope: Type.Literal('contract_deployment'),
  implement_trait: Type.String(),
});
