import { Static, Type } from '@sinclair/typebox';
import { StacksEventSchema } from './stacks/payload';
import { BitcoinEventSchema } from './bitcoin/payload';
import { BitcoinIfThisSchema } from './bitcoin/if_this';
import { StacksIfThisSchema } from './stacks/if_this';

const EventArray = Type.Union([Type.Array(StacksEventSchema), Type.Array(BitcoinEventSchema)]);

export const PayloadSchema = Type.Object({
  apply: EventArray,
  rollback: EventArray,
  chainhook: Type.Object({
    uuid: Type.String(),
    predicate: Type.Union([BitcoinIfThisSchema, StacksIfThisSchema]),
    is_streaming_blocks: Type.Boolean(),
  }),
});
export type Payload = Static<typeof PayloadSchema>;
