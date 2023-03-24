import { Static, TSchema, Type } from '@sinclair/typebox';
import { TypeCompiler } from '@sinclair/typebox/compiler';

export const Nullable = <T extends TSchema>(type: T) => Type.Union([type, Type.Null()]);

export const BlockIdentifier = Type.Object({
  index: Type.Integer(),
  hash: Type.String(),
});

export const TransactionIdentifier = Type.Object({
  hash: Type.String()
});

const BitcoinOutput = Type.Object({
  value: Type.Integer(),
  script_pubkey: Type.String(),
});

const InscriptionRevealed = Type.Object({
  inscription_revealed: Type.Object({
    content_bytes: Type.String(),
    content_type: Type.String(),
    content_length: Type.Integer(),
    inscription_number: Type.Integer(),
    inscription_fee: Type.Integer(),
    inscription_id: Type.String(),
    inscriber_address: Type.String(),
    ordinal_number: Type.Integer(),
    ordinal_block_height: Type.Integer(),
    ordinal_offset: Type.Integer(),
    satpoint_post_inscription: Type.String(),
  }),
});

const InscriptionTransferred = Type.Object({
  inscription_transferred: Type.Object({
    inscription_number: Type.Integer(),
    inscription_id: Type.String(),
    ordinal_number: Type.Integer(),
    updated_address: Type.String(),
    satpoint_pre_transfer: Type.String(),
    satpoint_post_transfer: Type.String(),
  })
});

const BitcoinTransaction = Type.Object({
  transaction_identifier: TransactionIdentifier,
  operations: Type.Array(Type.Any()),
  metadata: Type.Object({
    ordinal_operations: Type.Union([InscriptionRevealed, InscriptionTransferred]),
    outputs: Type.Array(BitcoinOutput),
    proof: Type.String(),
  }),
});

const BitcoinEvent = Type.Object({
  block_identifier: BlockIdentifier,
  parent_block_identifier: BlockIdentifier,
  timestamp: Type.Integer(),
  transactions: Type.Array(BitcoinTransaction),
  metadata: Type.Any(),
});

const ChainhookPayload = Type.Object({
  apply: Type.Array(BitcoinEvent),
  rollback: Type.Array(BitcoinEvent),
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: Type.Object({
      scope: Type.String(),
      ordinal: Type.String(),
    }),
  }),
});
export type ChainhookPayload = Static<typeof ChainhookPayload>;
export const ChainhookPayloadCType = TypeCompiler.Compile(ChainhookPayload);
