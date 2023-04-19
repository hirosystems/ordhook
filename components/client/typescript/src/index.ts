import { Type, TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
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
import { Payload, PayloadSchema } from './schemas';
import { Predicate, ThenThat } from './schemas/predicate';

export type OnEventCallback = (uuid: string, payload: Payload) => Promise<void>;

type ServerOptions = {
  server: {
    host: string;
    port: number;
    auth_token: string;
    external_hostname: string;
  };
  chainhook_node: {
    hostname: string;
    port: number;
  };
};

/**
 * Starts the chainhook event server.
 * @returns Fastify instance
 */
export async function startServer(
  opts: ServerOptions,
  predicates: [Predicate],
  callback: OnEventCallback
) {
  const base_path = `http://${opts.chainhook_node.hostname}:${opts.chainhook_node.port}`;

  async function waitForNode(this: FastifyInstance) {
    logger.info(`EventServer connecting to chainhook node...`);
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
    logger.info(predicates, `EventServer registering predicates on ${base_path}...`);
    for (const predicate of predicates) {
      const thenThat: ThenThat = {
        http_post: {
          url: `http://${opts.server.external_hostname}/chainhook/${predicate.uuid}`,
          authorization_header: `Bearer ${opts.server.auth_token}`,
        },
      };
      try {
        const body = predicate;
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
    logger.info(`EventServer closing predicates...`);
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
    if (authHeader && authHeader === `Bearer ${opts.server.auth_token}`) {
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

  await fastify.listen({ host: opts.server.host, port: opts.server.port });
  return fastify;
}
