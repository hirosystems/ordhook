import { Type } from '@sinclair/typebox';
import { Nullable, BlockIdentifier } from '../common';

const InscriptionRevealed = Type.Object({
  content_bytes: Type.String(),
  content_type: Type.String(),
  content_length: Type.Integer(),
  inscription_number: Type.Integer(),
  inscription_fee: Type.Integer(),
  inscription_id: Type.String(),
  inscription_output_value: Type.Integer(),
  inscriber_address: Type.String(),
  ordinal_number: Type.Integer(),
  ordinal_block_height: Type.Integer(),
  ordinal_offset: Type.Integer(),
  satpoint_post_inscription: Type.String(),
});

const InscriptionTransferred = Type.Object({
  inscription_number: Type.Integer(),
  inscription_id: Type.String(),
  ordinal_number: Type.Integer(),
  updated_address: Nullable(Type.String()),
  satpoint_pre_transfer: Type.String(),
  satpoint_post_transfer: Type.String(),
  post_transfer_output_value: Nullable(Type.Integer()),
});

const OrdinalOperation = Type.Object({
  inscription_revealed: Type.Optional(InscriptionRevealed),
  inscription_transferred: Type.Optional(InscriptionTransferred),
});

const Output = Type.Object({
  script_pubkey: Type.String(),
  value: Type.Integer(),
});

const Transaction = Type.Object({
  transaction_identifier: Type.Object({ hash: Type.String() }),
  operations: Type.Array(Type.Any()),
  metadata: Type.Object({
    ordinal_operations: Type.Array(OrdinalOperation),
    outputs: Type.Optional(Type.Array(Output)),
    proof: Nullable(Type.String()),
  }),
});

export const BitcoinEvent = Type.Object({
  block_identifier: BlockIdentifier,
  parent_block_identifier: BlockIdentifier,
  timestamp: Type.Integer(),
  transactions: Type.Array(Transaction),
  metadata: Type.Any(),
});
