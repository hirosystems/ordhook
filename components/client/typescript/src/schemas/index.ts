import { Static, Type } from '@sinclair/typebox';
import { StacksEvent } from './stacks';
import { BitcoinEvent } from './bitcoin';
import { IfThisSchema } from './predicate';

const EventArray = Type.Union([Type.Array(StacksEvent), Type.Array(BitcoinEvent)]);

export const PayloadSchema = Type.Object({
  apply: EventArray,
  rollback: EventArray,
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: IfThisSchema,
  }),
});
export type Payload = Static<typeof PayloadSchema>;
