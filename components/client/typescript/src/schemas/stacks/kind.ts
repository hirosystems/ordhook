import { Type } from '@sinclair/typebox';
import { Principal } from '.';

const ContractCall = Type.Object({
  type: Type.Literal('ContractCall'),
  data: Type.Object({
    args: Type.Array(Type.String()),
    contract_identifier: Principal,
    method: Type.String(),
  }),
});

const ContractDeployment = Type.Object({
  type: Type.Literal('ContractDeployment'),
  data: Type.Object({
    contract_identifier: Principal,
    code: Type.String(),
  }),
});

const Coinbase = Type.Object({
  type: Type.Literal('Coinbase'),
});

const NativeTokenTransfer = Type.Object({
  type: Type.Literal('NativeTokenTransfer'),
});

const BitcoinOpStackStx = Type.Object({
  type: Type.Literal('BitcoinOp'),
  data: Type.Object({
    locked_amount: Type.String(),
    stacking_address: Type.String(),
    unlock_height: Type.String(),
  }),
});

const BitcoinOpDelegateStackStx = Type.Object({
  type: Type.Literal('BitcoinOp'),
  data: Type.Object({
    stacking_address: Type.String(),
    amount: Type.String(),
    delegate: Type.String(),
    pox_address: Type.Optional(Type.String()),
    unlock_height: Type.Optional(Type.String()),
  }),
});

const Unsupported = Type.Object({
  type: Type.Literal('Unsupported'),
});

export const Kind = Type.Union([
  Coinbase,
  ContractCall,
  ContractDeployment,
  NativeTokenTransfer,
  BitcoinOpStackStx,
  BitcoinOpDelegateStackStx,
  Unsupported,
]);
