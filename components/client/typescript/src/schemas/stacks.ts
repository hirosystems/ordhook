import { Type } from '@sinclair/typebox';
import { BlockIdentifier, Nullable, TransactionIdentifier } from '.';

const FtTransferEvent = Type.Object({
  type: Type.Literal('FTTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: Type.String(),
    sender: Type.String()
  }),
});

const FtMintEvent = Type.Object({
  type: Type.Literal('FTMintEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: Type.String(),
  }),
});

const SmartContractEvent = Type.Object({
  type: Type.Literal('SmartContractEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    raw_value: Type.String(),
    topic: Type.String()
  }),
});

const StxTransferEvent = Type.Object({
  type: Type.Literal('STXTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: Type.String(),
    recipient: Type.String(),
  })
});

const StacksTransaction = Type.Object({
  transaction_identifier: TransactionIdentifier,
  operations: Type.Array(Type.Object({
    account: Type.Object({
      address: Type.String(),
    }),
    amount: Type.Object({
      currency: Type.Object({
        decimals: Type.Integer(),
        symbol: Type.String(),
      }),
      value: Type.Integer(),
    }),
    operation_identifier: Type.Object({
      index: Type.Integer(),
    }),
    related_operations: Type.Array(Type.Object({
      index: Type.Integer(),
    })),
    status: Type.String(),
    type: Type.String(),
  })),
  metadata: Type.Object({
    description: Type.String(),
    execution_cost: Type.Object({
      read_count: Type.Integer(),
      read_length: Type.Integer(),
      runtime: Type.Integer(),
      write_count: Type.Integer(),
      write_length: Type.Integer(),
    }),
    fee: Type.Integer(),
    kind: Type.Any(), // TODO
    nonce: Type.Integer(),
    position: Type.Object({
      index: Type.Integer(),
      micro_block_identifier: Type.Optional(BlockIdentifier),
    }),
    proof: Nullable(Type.String()),
    raw_tx: Type.String(),
    receipt: Type.Object({
      contract_calls_stack: Type.Array(Type.Any()),
      events: Type.Array(Type.Union([
        FtTransferEvent, FtMintEvent, SmartContractEvent, StxTransferEvent
      ])),
      mutated_assets_radius: Type.Array(Type.String()),
      mutated_contracts_radius: Type.Array(Type.String()),
    }),
    result: Type.String(),
    sender: Type.String(),
    success: Type.Boolean(),
  }),
});

const StacksEvent = Type.Object({
  block_identifier: BlockIdentifier,
  parent_block_identifier: BlockIdentifier,
  timestamp: Type.Integer(),
  transactions: Type.Array(StacksTransaction),
  metadata: Type.Object({
    bitcoin_anchor_block_identifier: BlockIdentifier,
    confirm_microblock_identifier: BlockIdentifier,
    pox_cycle_index: Type.Integer(),
    pox_cycle_length: Type.Integer(),
    pox_cycle_position: Type.Integer(),
  }),
});
