import { Type } from '@sinclair/typebox';

export const BitcoinIfThisTxIdSchema = Type.Object({
  scope: Type.Literal('txid'),
  equals: Type.String(),
});

export const BitcoinIfThisOpReturnStartsWithSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    starts_with: Type.String(),
  }),
});

export const BitcoinIfThisOpReturnEqualsSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    equals: Type.String(),
  }),
});

export const BitcoinIfThisOpReturnEndsWithSchema = Type.Object({
  scope: Type.Literal('outputs'),
  op_return: Type.Object({
    ends_with: Type.String(),
  }),
});

export const BitcoinIfThisP2PKHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2pkh: Type.String(),
});

export const BitcoinIfThisP2SHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2sh: Type.String(),
});

export const BitcoinIfThisP2WPKHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2wpkh: Type.String(),
});

export const BitcoinIfThisP2WSHSchema = Type.Object({
  scope: Type.Literal('outputs'),
  p2wsh: Type.String(),
});

export const BitcoinIfThisStacksBlockCommittedSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('block_committed'),
});

export const BitcoinIfThisStacksLeaderKeyRegisteredSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('leader_key_registered'),
});

export const BitcoinIfThisStacksStxTransferredSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('stx_transfered'),
});

export const BitcoinIfThisStacksStxLockedSchema = Type.Object({
  scope: Type.Literal('stacks_protocol'),
  operation: Type.Literal('stx_locked'),
});

export const BitcoinIfThisOrdinalsFeedSchema = Type.Object({
  scope: Type.Literal('ordinals_protocol'),
  operation: Type.Literal('inscription_feed'),
});
