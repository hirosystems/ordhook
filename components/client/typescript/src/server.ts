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
  external_base_url: Type.String(),
});
/** Local event server connection and authentication options */
export type ServerOptions = Static<typeof ServerOptionsSchema>;

const ChainhookNodeOptionsSchema = Type.Object({
  base_url: Type.String(),
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
  predicates: ServerPredicate[],
  callback: OnEventCallback
) {
  async function waitForNode(this: FastifyInstance) {
    logger.info(
      `ChainhookEventObserver connecting to chainhook node at ${chainhookOpts.base_url}...`
    );
    while (true) {
      try {
        await request(`${chainhookOpts.base_url}/ping`, { method: 'GET', throwOnError: true });
        break;
      } catch (error) {
        logger.error(error, 'Chainhook node not available, retrying...');
        await timeout(1000);
      }
    }
  }

  async function registerPredicates(this: FastifyInstance) {
    logger.info(
      predicates,
      `ChainhookEventObserver registering predicates at ${chainhookOpts.base_url}`
    );
    for (const predicate of predicates) {
      const thenThat: ThenThatHttpPost = {
        http_post: {
          url: `${serverOpts.external_base_url}/chainhook/${encodeURIComponent(predicate.uuid)}`,
          authorization_header: `Bearer ${serverOpts.auth_token}`,
        },
      };
      try {
        const body = predicate as Predicate;
        if ('mainnet' in body.networks) body.networks.mainnet.then_that = thenThat;
        if ('testnet' in body.networks) body.networks.testnet.then_that = thenThat;
        await request(`${chainhookOpts.base_url}/v1/chainhooks`, {
          method: 'POST',
          body: JSON.stringify(body),
          headers: { 'content-type': 'application/json' },
          throwOnError: true,
        });
        logger.info(
          `ChainhookEventObserver registered '${predicate.name}' predicate (${predicate.uuid})`
        );
      } catch (error) {
        logger.error(error, `ChainhookEventObserver unable to register predicate`);
      }
    }
  }

  async function removePredicates(this: FastifyInstance) {
    logger.info(`ChainhookEventObserver closing predicates at ${chainhookOpts.base_url}`);
    const removals = predicates.map(
      predicate =>
        new Promise<void>((resolve, reject) => {
          request(
            `${chainhookOpts.base_url}/v1/chainhooks/${predicate.chain}/${encodeURIComponent(
              predicate.uuid
            )}`,
            {
              method: 'DELETE',
              headers: { 'content-type': 'application/json' },
              throwOnError: true,
            }
          )
            .then(() => {
              logger.info(
                `ChainhookEventObserver removed '${predicate.name}' predicate (${predicate.uuid})`
              );
              resolve();
            })
            .catch(error => {
              logger.error(error, `ChainhookEventObserver unable to deregister predicate`);
              reject(error);
            });
        })
    );
    await Promise.allSettled(removals);
  }

  async function isEventAuthorized(request: FastifyRequest, reply: FastifyReply) {
    const authHeader = request.headers.authorization;
    if (authHeader && authHeader === `Bearer ${serverOpts.auth_token}`) {
      return;
    }
    await reply.code(403).send();
  }

  const ChainhookEventObserver: FastifyPluginCallback<
    Record<never, never>,
    Server,
    TypeBoxTypeProvider
  > = (fastify, options, done) => {
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
          logger.error(error, `ChainhookEventObserver error processing payload`);
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

  await fastify.register(ChainhookEventObserver);
  return fastify;
}
