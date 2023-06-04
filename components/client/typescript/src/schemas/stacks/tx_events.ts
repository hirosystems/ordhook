import { Static, Type } from '@sinclair/typebox';
import { StacksPrincipalSchema } from './payload';

export const StacksTransactionNftMintEventSchema = Type.Object({
  type: Type.Literal('NFTMintEvent'),
  data: Type.Object({
    asset_class_identifier: Type.String(),
    asset_identifier: Type.String(),
    recipient: StacksPrincipalSchema,
  }),
});
export type StacksTransactionNftMintEvent = Static<typeof StacksTransactionNftMintEventSchema>;

export const StacksTransactionNftTransferEventSchema = Type.Object({
  type: Type.Literal('NFTTransferEvent'),
  data: Type.Object({
    raw_value: Type.String(),
    asset_identifier: Type.String(),
    recipient: StacksPrincipalSchema,
    sender: StacksPrincipalSchema,
  }),
});
export type StacksTransactionNftTransferEvent = Static<
  typeof StacksTransactionNftTransferEventSchema
>;

export const StacksTransactionNftBurnEventSchema = Type.Object({
  type: Type.Literal('NFTBurnEvent'),
  data: Type.Object({
    asset_class_identifier: Type.String(),
    asset_identifier: Type.String(),
    sender: StacksPrincipalSchema,
  }),
});
export type StacksTransactionNftBurnEvent = Static<typeof StacksTransactionNftBurnEventSchema>;

export const StacksTransactionFtTransferEventSchema = Type.Object({
  type: Type.Literal('FTTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: StacksPrincipalSchema,
    sender: StacksPrincipalSchema,
  }),
});
export type StacksTransactionFtTransferEvent = Static<
  typeof StacksTransactionFtTransferEventSchema
>;

export const StacksTransactionFtMintEventSchema = Type.Object({
  type: Type.Literal('FTMintEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    recipient: StacksPrincipalSchema,
  }),
});
export type StacksTransactionFtMintEvent = Static<typeof StacksTransactionFtMintEventSchema>;

export const StacksTransactionFtBurnEventSchema = Type.Object({
  type: Type.Literal('FTBurnEvent'),
  data: Type.Object({
    amount: Type.String(),
    asset_identifier: Type.String(),
    sender: StacksPrincipalSchema,
  }),
});
export type StacksTransactionFtBurnEvent = Static<typeof StacksTransactionFtBurnEventSchema>;

export const StacksTransactionSmartContractEventSchema = Type.Object({
  type: Type.Literal('SmartContractEvent'),
  data: Type.Object({
    contract_identifier: StacksPrincipalSchema,
    raw_value: Type.String(),
    topic: Type.String(),
  }),
});
export type StacksTransactionSmartContractEvent = Static<
  typeof StacksTransactionSmartContractEventSchema
>;

export const StacksTransactionStxTransferEventSchema = Type.Object({
  type: Type.Literal('STXTransferEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: StacksPrincipalSchema,
    recipient: StacksPrincipalSchema,
  }),
});
export type StacksTransactionStxTransferEvent = Static<
  typeof StacksTransactionStxTransferEventSchema
>;

export const StacksTransactionStxMintEventSchema = Type.Object({
  type: Type.Literal('STXMintEvent'),
  data: Type.Object({
    amount: Type.String(),
    recipient: StacksPrincipalSchema,
  }),
});
export type StacksTransactionStxMintEvent = Static<typeof StacksTransactionStxMintEventSchema>;

export const StacksTransactionStxLockEventSchema = Type.Object({
  type: Type.Literal('STXLockEvent'),
  data: Type.Object({
    locked_amount: Type.String(),
    unlock_height: Type.String(),
    locked_address: Type.String(),
  }),
});
export type StacksTransactionStxLockEvent = Static<typeof StacksTransactionStxLockEventSchema>;

export const StacksTransactionStxBurnEventSchema = Type.Object({
  type: Type.Literal('STXBurnEvent'),
  data: Type.Object({
    amount: Type.String(),
    sender: StacksPrincipalSchema,
  }),
});
export type StacksTransactionStxBurnEvent = Static<typeof StacksTransactionStxBurnEventSchema>;

export const StacksTransactionDataVarSetEventSchema = Type.Object({
  type: Type.Literal('DataVarSetEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    var: Type.String(),
    new_value: Type.Any(),
  }),
});
export type StacksTransactionDataVarSetEvent = Static<
  typeof StacksTransactionDataVarSetEventSchema
>;

export const StacksTransactionDataMapInsertEventSchema = Type.Object({
  type: Type.Literal('DataMapInsertEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    inserted_key: Type.Any(),
    inserted_value: Type.Any(),
  }),
});
export type StacksTransactionDataMapInsertEvent = Static<
  typeof StacksTransactionDataMapInsertEventSchema
>;

export const StacksTransactionDataMapUpdateEventSchema = Type.Object({
  type: Type.Literal('DataMapUpdateEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    key: Type.Any(),
    new_value: Type.Any(),
  }),
});
export type StacksTransactionDataMapUpdateEvent = Static<
  typeof StacksTransactionDataMapUpdateEventSchema
>;

export const StacksTransactionDataMapDeleteEventSchema = Type.Object({
  type: Type.Literal('DataMapDeleteEvent'),
  data: Type.Object({
    contract_identifier: Type.String(),
    map: Type.String(),
    deleted_key: Type.Any(),
  }),
});
export type StacksTransactionDataMapDeleteEvent = Static<
  typeof StacksTransactionDataMapDeleteEventSchema
>;

export const StacksTransactionEventSchema = Type.Union([
  StacksTransactionFtTransferEventSchema,
  StacksTransactionFtMintEventSchema,
  StacksTransactionFtBurnEventSchema,
  StacksTransactionNftTransferEventSchema,
  StacksTransactionNftMintEventSchema,
  StacksTransactionNftBurnEventSchema,
  StacksTransactionStxTransferEventSchema,
  StacksTransactionStxMintEventSchema,
  StacksTransactionStxLockEventSchema,
  StacksTransactionStxBurnEventSchema,
  StacksTransactionDataVarSetEventSchema,
  StacksTransactionDataMapInsertEventSchema,
  StacksTransactionDataMapUpdateEventSchema,
  StacksTransactionDataMapDeleteEventSchema,
  StacksTransactionSmartContractEventSchema,
]);
export type StacksTransactionEvent = Static<typeof StacksTransactionEventSchema>;
