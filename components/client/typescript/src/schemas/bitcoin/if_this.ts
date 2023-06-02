import { Static, Type } from '@sinclair/typebox';
import { ThenThatSchema } from '../predicate';

export const BitcoinIfThisTxIdSchema = Type.Object({
  scope: Type.Literal('txid'),
  equals: Type.String(),
});
export type BitcoinIfThisTxId = Static<typeof BitcoinIfThisTxIdSchema>;

export const BitcoinIfThisOpReturnStartsWithSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    starts_with: Type.String(),
  }),
});
export type BitcoinIfThisOpReturnStartsWith = Static<typeof BitcoinIfThisOpReturnStartsWithSchema>;

export const BitcoinIfThisOpReturnEqualsSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    equals: Type.String(),
  }),
});
export type BitcoinIfThisOpReturnEquals = Static<typeof BitcoinIfThisOpReturnEqualsSchema>;

export const BitcoinIfThisOpReturnEndsWithSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    ends_with: Type.String(),
  }),
});
export type BitcoinIfThisOpReturnEndsWith = Static<typeof BitcoinIfThisOpReturnEndsWithSchema>;

export const BitcoinIfThisP2PKHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2pkh: Type.String(),
});
export type BitcoinIfThisP2PKH = Static<typeof BitcoinIfThisP2PKHSchema>;

export const BitcoinIfThisP2SHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2sh: Type.String(),
});
export type BitcoinIfThisP2SH = Static<typeof BitcoinIfThisP2SHSchema>;

export const BitcoinIfThisP2WPKHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2wpkh: Type.String(),
});
export type BitcoinIfThisP2WPKH = Static<typeof BitcoinIfThisP2WPKHSchema>;

export const BitcoinIfThisP2WSHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2wsh: Type.String(),
});
export type BitcoinIfThisP2WSH = Static<typeof BitcoinIfThisP2WSHSchema>;

export const BitcoinIfThisStacksBlockCommittedSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('block_committed'),
});
export type BitcoinIfThisStacksBlockCommitted = Static<
  typeof BitcoinIfThisStacksBlockCommittedSchema
>;

export const BitcoinIfThisStacksLeaderKeyRegisteredSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('leader_key_registered'),
});
export type BitcoinIfThisStacksLeaderKeyRegistered = Static<
  typeof BitcoinIfThisStacksLeaderKeyRegisteredSchema
>;

export const BitcoinIfThisStacksStxTransferredSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('stx_transfered'),
});
export type BitcoinIfThisStacksStxTransferred = Static<
  typeof BitcoinIfThisStacksStxTransferredSchema
>;

export const BitcoinIfThisStacksStxLockedSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('stx_locked'),
});
export type BitcoinIfThisStacksStxLocked = Static<typeof BitcoinIfThisStacksStxLockedSchema>;

export const BitcoinIfThisOrdinalsFeedSchema = Type.Object({
  scope: Type.Literal('ordinals_protocol'),
  operation: Type.Literal('inscription_feed'),
});
export type BitcoinIfThisOrdinalsFeed = Static<typeof BitcoinIfThisOrdinalsFeedSchema>;

export const BitcoinIfThisOptionsSchema = Type.Object({
  start_block: Type.Optional(Type.Integer()),
  end_block: Type.Optional(Type.Integer()),
  expire_after_occurrence: Type.Optional(Type.Integer()),
  include_proof: Type.Optional(Type.Boolean()),
  include_inputs: Type.Optional(Type.Boolean()),
  include_outputs: Type.Optional(Type.Boolean()),
  include_witness: Type.Optional(Type.Boolean()),
});
export type BitcoinIfThisOptions = Static<typeof BitcoinIfThisOptionsSchema>;

export const BitcoinIfThisSchema = Type.Union([
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
]);
export type BitcoinIfThis = Static<typeof BitcoinIfThisSchema>;

export const BitcoinIfThisThenThatSchema = Type.Composite([
  BitcoinIfThisOptionsSchema,
  Type.Object({
    if_this: BitcoinIfThisSchema,
    then_that: ThenThatSchema,
  }),
]);
export type BitcoinIfThisThenThat = Static<typeof BitcoinIfThisThenThatSchema>;
