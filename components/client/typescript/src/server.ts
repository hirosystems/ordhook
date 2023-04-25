import { Static, Type, TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import Fastify, {
  FastifyInstance,
  FastifyPluginCallback,
  FastifyReply,
  FastifyRequest,
} from 'fastify';
import { Server } from 'http';
import { request } from 'undici';
import { logger, PINO_CONFIG } from './util/logger';
import { timeout } from './util/helpers';
import { Payload, PayloadSchema } from './schemas/payload';
import { Predicate, PredicateHeaderSchema, ThenThatHttpPost } from './schemas/predicate';
import { BitcoinIfThisOptionsSchema, BitcoinIfThisSchema } from './schemas/bitcoin/if_this';
import { StacksIfThisOptionsSchema, StacksIfThisSchema } from './schemas/stacks/if_this';

/** Function type for a Chainhook event callback */
export type OnEventCallback = (uuid: string, payload: Payload) => Promise<void>;

const ServerOptionsSchema = Type.Object({
  hostname: Type.String(),
  port: Type.Integer(),
  auth_token: Type.String(),
  external_host: Type.String(),
});
/** Local event server connection and authentication options */
export type ServerOptions = Static<typeof ServerOptionsSchema>;

const ChainhookNodeOptionsSchema = Type.Object({
  hostname: Type.String(),
  port: Type.Integer(),
});
/** Chainhook node connection options */
export type ChainhookNodeOptions = Static<typeof ChainhookNodeOptionsSchema>;

const IfThisThenNothingSchema = Type.Union([
  Type.Composite([
    BitcoinIfThisOptionsSchema,
    Type.Object({
      if_this: BitcoinIfThisSchema,
    }),
  ]),
  Type.Composite([
    StacksIfThisOptionsSchema,
    Type.Object({
      if_this: StacksIfThisSchema,
    }),
  ]),
]);
const ServerPredicateSchema = Type.Composite([
  PredicateHeaderSchema,
  Type.Object({
    networks: Type.Union([
      Type.Object({
        mainnet: IfThisThenNothingSchema,
      }),
      Type.Object({
        testnet: IfThisThenNothingSchema,
      }),
    ]),
  }),
]);
/** Chainhook predicates registerable by the local event server */
export type ServerPredicate = Static<typeof ServerPredicateSchema>;

/**
 * Build the Chainhook Fastify event server.
 * @param serverOpts - Server options
 * @param chainhookOpts - Chainhook node options
 * @param predicates - Predicates to register
 * @param callback - Event callback function
 * @returns Fastify instance
 */
export async function buildServer(
  serverOpts: ServerOptions,
  chainhookOpts: ChainhookNodeOptions,
  predicates: [ServerPredicate],
  callback: OnEventCallback
) {
  const base_path = `http://${chainhookOpts.hostname}:${chainhookOpts.port}`;

  async function waitForNode(this: FastifyInstance) {
    logger.info(`EventServer connecting to chainhook node at ${base_path}...`);
    while (true) {
      try {
        await request(`${base_path}/ping`, { method: 'GET', throwOnError: true });
        break;
      } catch (error) {
        logger.error(error, 'Chainhook node not available, retrying...');
        await timeout(1000);
      }
    }
  }

  async function registerPredicates(this: FastifyInstance) {
    logger.info(predicates, `EventServer registering predicates at ${base_path}...`);
    for (const predicate of predicates) {
      const thenThat: ThenThatHttpPost = {
        http_post: {
          url: `http://${serverOpts.external_host}/chainhook/${predicate.uuid}`,
          authorization_header: `Bearer ${serverOpts.auth_token}`,
        },
      };
      try {
        const body = predicate as Predicate;
        if ('mainnet' in body.networks) body.networks.mainnet.then_that = thenThat;
        if ('testnet' in body.networks) body.networks.testnet.then_that = thenThat;
        await request(`${base_path}/v1/chainhooks`, {
          method: 'POST',
          body: JSON.stringify(body),
          headers: { 'content-type': 'application/json' },
          throwOnError: true,
        });
        logger.info(`EventServer registered '${predicate.name}' predicate (${predicate.uuid})`);
      } catch (error) {
        logger.error(error, `EventServer unable to register predicate`);
      }
    }
  }

  async function removePredicates(this: FastifyInstance) {
    logger.info(`EventServer closing predicates at ${base_path}...`);
    for (const predicate of predicates) {
      try {
        await request(`${base_path}/v1/chainhooks/${predicate.chain}/${predicate.uuid}`, {
          method: 'DELETE',
          headers: { 'content-type': 'application/json' },
          throwOnError: true,
        });
        logger.info(`EventServer removed '${predicate.name}' predicate (${predicate.uuid})`);
      } catch (error) {
        logger.error(error, `EventServer unable to deregister predicate`);
      }
    }
  }

  async function isEventAuthorized(request: FastifyRequest, reply: FastifyReply) {
    const authHeader = request.headers.authorization;
    if (authHeader && authHeader === `Bearer ${serverOpts.auth_token}`) {
      return;
    }
    await reply.code(403).send();
  }

  const EventServer: FastifyPluginCallback<Record<never, never>, Server, TypeBoxTypeProvider> = (
    fastify,
    options,
    done
  ) => {
    fastify.addHook('preHandler', isEventAuthorized);
    fastify.post(
      '/chainhook/:uuid',
      {
        schema: {
          params: Type.Object({
            uuid: Type.String({ format: 'uuid' }),
          }),
          body: PayloadSchema,
        },
      },
      async (request, reply) => {
        try {
          await callback(request.params.uuid, request.body);
        } catch (error) {
          logger.error(error, `EventServer error processing payload`);
          await reply.code(422).send();
        }
        await reply.code(200).send();
      }
    );
    done();
  };

  const fastify = Fastify({
    trustProxy: true,
    logger: PINO_CONFIG,
    pluginTimeout: 0, // Disable so ping can retry indefinitely
    bodyLimit: 41943040, // 40 MB
  }).withTypeProvider<TypeBoxTypeProvider>();

  fastify.addHook('onReady', waitForNode);
  fastify.addHook('onReady', registerPredicates);
  fastify.addHook('onClose', removePredicates);

  await fastify.register(EventServer);
  return fastify;
}
