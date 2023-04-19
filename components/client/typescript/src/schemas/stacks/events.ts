import { Type } from '@sinclair/typebox';
import { Principal } from '.';

const NftMintEvent = Type.Object({
  type: Type.Literal('NFTMintEvent'),
  data: Type.Object({
    asset_class_identifier: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
  }),
});

const NftTransferEvent = Type.Object({
  type: Type.Literal('NFTTransferEvent'),
  data: Type.Object({
    raw_value: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
    sender: Principal,
  }),
});

const NftBurnEvent = Type.Object({
  type: Type.Literal('NFTBurnEvent'),
  data: Type.Object({
    asset_class_identifier: Type.String(),
    asset_identifier: Type.String(),
    sender: Principal,
  }),
});

const FtTransferEvent = Type.Object({
  type: Type.Literal('FTTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: Principal,
    sender: Principal,
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
    topic: Type.String(),
  }),
});

const StxTransferEvent = Type.Object({
  type: Type.Literal('STXTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: Principal,
    recipient: Principal,
  }),
});

const StxMintEvent = Type.Object({
  type: Type.Literal('STXMintEvent'),
  data: Type.Object({
    amount: Type.String(),
    recipient: Principal,
  }),
});

const StxLockEvent = Type.Object({
  type: Type.Literal('STXLockEvent'),
  data: Type.Object({
    locked_amount: Type.String(),
    unlock_height: Type.String(),
    locked_address: Type.String(),
  }),
});

const StxBurnEvent = Type.Object({
  type: Type.Literal('STXBurnEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: Principal,
  }),
});

const DataVarSetEvent = Type.Object({
  type: Type.Literal('DataVarSetEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    var: Type.String(),
    new_value: Type.Any(),
  }),
});

const DataMapInsertEvent = Type.Object({
  type: Type.Literal('DataMapInsertEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    inserted_key: Type.Any(),
    inserted_value: Type.Any(),
  }),
});

const DataMapUpdateEvent = Type.Object({
  type: Type.Literal('DataMapUpdateEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    key: Type.Any(),
    new_value: Type.Any(),
  }),
});

const DataMapDeleteEvent = Type.Object({
  type: Type.Literal('DataMapDeleteEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    deleted_key: Type.Any(),
  }),
});

export const Event = Type.Union([
  FtTransferEvent,
  FtMintEvent,
  FtBurnEvent,
  NftTransferEvent,
  NftMintEvent,
  NftBurnEvent,
  StxTransferEvent,
  StxMintEvent,
  StxLockEvent,
  StxBurnEvent,
  DataVarSetEvent,
  DataMapInsertEvent,
  DataMapUpdateEvent,
  DataMapDeleteEvent,
  SmartContractEvent,
]);
