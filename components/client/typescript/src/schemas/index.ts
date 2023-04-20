import { Static, Type } from '@sinclair/typebox';
import { StacksEvent } from './stacks';
import { BitcoinEvent } from './bitcoin';
import { BitcoinIfThisSchema } from './bitcoin/if_this';
import { StacksIfThisSchema } from './stacks/if_this';

const EventArray = Type.Union([Type.Array(StacksEvent), Type.Array(BitcoinEvent)]);

export const PayloadSchema = Type.Object({
  apply: EventArray,
  rollback: EventArray,
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: Type.Union([BitcoinIfThisSchema, StacksIfThisSchema]),
  }),
});
export type Payload = Static<typeof PayloadSchema>;
