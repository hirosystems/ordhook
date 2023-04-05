import { Type } from '@sinclair/typebox';
import { BlockIdentifier, Nullable, TransactionIdentifier } from '.';

const Principal = Type.String();

const NftTransferEvent = Type.Object({
  type: Type.Literal('NFTTransferEvent'),
  data: Type.Object({
    raw_value: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
    sender: Principal
  }),
});

const FtTransferEvent = Type.Object({
  type: Type.Literal('FTTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
    sender: Principal
  }),
});

const FtMintEvent = Type.Object({
  type: Type.Literal('FTMintEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
  }),
});

const FtBurnEvent = Type.Object({
  type: Type.Literal('FTBurnEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    sender: Principal,
  }),
});

const SmartContractEvent = Type.Object({
  type: Type.Literal('SmartContractEvent'),
  data: Type.Object({
    contract_identifier: Principal,
    raw_value: Type.String(),
    topic: Type.String()
  }),
});

const StxTransferEvent = Type.Object({
  type: Type.Literal('STXTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: Principal,
    recipient: Principal,
  })
});

const ContractCallKind = Type.Object({
  type: Type.Literal('ContractCall'),
  data: Type.Object({
    args: Type.Array(Type.String()),
    contract_identifier: Principal,
    method: Type.String(),
  }),
});

const Coinbase = Type.Object({
  type: Type.Literal('Coinbase')
});

const OperationIdentifier = Type.Object({
  index: Type.Integer(),
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
        metadata: Type.Object({
          asset_class_identifier: Type.String(),
          asset_identifier: Nullable(Type.String()),
          standard: Type.String(),
        }),
      }),
      value: Type.Integer(),
    }),
    operation_identifier: OperationIdentifier,
    related_operations: Type.Optional(Type.Array(OperationIdentifier)),
    status: Type.String(),
    type: Type.Union([Type.Literal('CREDIT'), Type.Literal('DEBIT')]),
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
    kind: Type.Union([
      Coinbase, ContractCallKind
    ]),
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
        FtTransferEvent, FtMintEvent, FtBurnEvent, NftTransferEvent, SmartContractEvent, StxTransferEvent
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
    stacks_block_hash: Type.String(),
  }),
});
