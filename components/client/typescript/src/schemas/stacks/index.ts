import { Type } from '@sinclair/typebox';
import { BlockIdentifier, Nullable, TransactionIdentifier } from '..';
import { Event } from './events';
import { Kind } from './kind';

export const Principal = Type.String();

const OperationIdentifier = Type.Object({
  index: Type.Integer(),
});

const Transaction = Type.Object({
  transaction_identifier: TransactionIdentifier,
  operations: Type.Array(Type.Object({
    account: Type.Object({
      address: Type.String(),
      sub_account: Type.Optional(Type.String()),
    }),
    amount: Type.Optional(
      Type.Object({
        currency: Type.Object({
          decimals: Type.Integer(),
          symbol: Type.String(),
          metadata: Type.Object({
            asset_class_identifier: Type.String(),
            asset_identifier: Nullable(Type.String()),
            standard: Type.String(),
          }),
        }),
        value: Type.Integer(),
      })
    ),
    metadata: Type.Optional(
      Type.Object({
        public_key: Type.Optional(
          Type.Object({
            hex_bypes: Type.Optional(Type.String()),
            curve_type: Type.String(),
          })
        ),
        code: Type.Optional(Type.String()),
        method_name: Type.Optional(Type.String()),
        args: Type.Optional(Type.String()),
      }),
    ),
    operation_identifier: OperationIdentifier,
    related_operations: Type.Optional(Type.Array(OperationIdentifier)),
    status: Type.Optional(Type.Literal('SUCCESS')),
    type: Type.Union([Type.Literal('CREDIT'), Type.Literal('DEBIT'), Type.Literal('LOCK')]),
  })),
  metadata: Type.Object({
    description: Type.String(),
    execution_cost: Type.Optional(
      Type.Object({
        read_count: Type.Integer(),
        read_length: Type.Integer(),
        runtime: Type.Integer(),
        write_count: Type.Integer(),
        write_length: Type.Integer(),
      })
    ),
    fee: Type.Integer(),
    kind: Kind,
    nonce: Type.Integer(),
    position: Type.Object({
      index: Type.Integer(),
      micro_block_identifier: Type.Optional(BlockIdentifier),
    }),
    proof: Nullable(Type.String()),
    raw_tx: Type.String(),
    receipt: Type.Object({
      contract_calls_stack: Type.Array(Type.String()),
      events: Type.Array(Event),
      mutated_assets_radius: Type.Array(Type.String()),
      mutated_contracts_radius: Type.Array(Type.String()),
    }),
    result: Type.String(),
    sender: Type.String(),
    sponsor: Type.Optional(Type.String()),
    success: Type.Boolean(),
  }),
});

export const StacksEvent = Type.Object({
  block_identifier: BlockIdentifier,
  parent_block_identifier: BlockIdentifier,
  timestamp: Type.Integer(),
  transactions: Type.Array(Transaction),
  metadata: Type.Object({
    bitcoin_anchor_block_identifier: BlockIdentifier,
    confirm_microblock_identifier: BlockIdentifier,
    pox_cycle_index: Type.Integer(),
    pox_cycle_length: Type.Integer(),
    pox_cycle_position: Type.Integer(),
    stacks_block_hash: Type.String(),
  }),
});
