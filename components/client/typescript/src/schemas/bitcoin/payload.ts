import { Static, Type } from '@sinclair/typebox';
import {
  Nullable,
  BlockIdentifierSchema,
  TransactionIdentifierSchema,
  RosettaOperationSchema,
} from '../common';

export const BitcoinInscriptionRevealedSchema = Type.Object({
  content_bytes: Type.String(),
  content_type: Type.String(),
  content_length: Type.Integer(),
  inscription_number: Type.Integer(),
  inscription_fee: Type.Integer(),
  inscription_id: Type.String(),
  inscription_output_value: Type.Integer(),
  inscriber_address: Nullable(Type.String()),
  ordinal_number: Type.Integer(),
  ordinal_block_height: Type.Integer(),
  ordinal_offset: Type.Integer(),
  satpoint_post_inscription: Type.String(),
});
export type BitcoinInscriptionRevealed = Static<typeof BitcoinInscriptionRevealedSchema>;

export const BitcoinInscriptionTransferredSchema = Type.Object({
  inscription_id: Type.String(),
  updated_address: Nullable(Type.String()),
  satpoint_pre_transfer: Type.String(),
  satpoint_post_transfer: Type.String(),
  post_transfer_output_value: Nullable(Type.Integer()),
});
export type BitcoinInscriptionTransferred = Static<typeof BitcoinInscriptionTransferredSchema>;

export const BitcoinCursedInscriptionRevealedSchema = Type.Object({
  content_bytes: Type.String(),
  content_type: Type.String(),
  content_length: Type.Integer(),
  inscription_number: Type.Integer(),
  inscription_fee: Type.Integer(),
  inscription_id: Type.String(),
  inscription_output_value: Type.Integer(),
  inscriber_address: Nullable(Type.String()),
  ordinal_number: Type.Integer(),
  ordinal_block_height: Type.Integer(),
  ordinal_offset: Type.Integer(),
  satpoint_post_inscription: Type.String(),
  curse_type: Nullable(Type.Union([Type.String(), Type.Object({ tag: Type.Number() })])),
});
export type BitcoinCursedInscriptionRevealed = Static<
  typeof BitcoinCursedInscriptionRevealedSchema
>;

export const BitcoinOrdinalOperationSchema = Type.Object({
  cursed_inscription_revealed: Type.Optional(BitcoinCursedInscriptionRevealedSchema),
  inscription_revealed: Type.Optional(BitcoinInscriptionRevealedSchema),
  inscription_transferred: Type.Optional(BitcoinInscriptionTransferredSchema),
});
export type BitcoinOrdinalOperation = Static<typeof BitcoinOrdinalOperationSchema>;

export const BitcoinOutputSchema = Type.Object({
  script_pubkey: Type.String(),
  value: Type.Integer(),
});
export type BitcoinOutput = Static<typeof BitcoinOutputSchema>;

export const BitcoinTransactionMetadataSchema = Type.Object({
  ordinal_operations: Type.Array(BitcoinOrdinalOperationSchema),
  outputs: Type.Optional(Type.Array(BitcoinOutputSchema)),
  proof: Nullable(Type.String()),
});
export type BitcoinTransactionMetadata = Static<typeof BitcoinTransactionMetadataSchema>;

export const BitcoinTransactionSchema = Type.Object({
  transaction_identifier: TransactionIdentifierSchema,
  operations: Type.Array(RosettaOperationSchema),
  metadata: BitcoinTransactionMetadataSchema,
});
export type BitcoinTransaction = Static<typeof BitcoinTransactionSchema>;

export const BitcoinEventSchema = Type.Object({
  block_identifier: BlockIdentifierSchema,
  parent_block_identifier: BlockIdentifierSchema,
  timestamp: Type.Integer(),
  transactions: Type.Array(BitcoinTransactionSchema),
  metadata: Type.Any(),
});
export type BitcoinEvent = Static<typeof BitcoinEventSchema>;
