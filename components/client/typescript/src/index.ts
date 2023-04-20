import { FastifyInstance } from 'fastify';
import {
  ServerOptions,
  ChainhookNodeOptions,
  ServerPredicate,
  OnEventCallback,
  buildServer,
} from './server';

export class ChainhookEventServer {
  private fastify?: FastifyInstance;
  private serverOpts: ServerOptions;
  private chainhookOpts: ChainhookNodeOptions;

  constructor(serverOpts: ServerOptions, chainhookOpts: ChainhookNodeOptions) {
    this.serverOpts = serverOpts;
    this.chainhookOpts = chainhookOpts;
  }

  async start(predicates: [ServerPredicate], callback: OnEventCallback) {
    if (this.fastify) return;
    this.fastify = await buildServer(this.serverOpts, this.chainhookOpts, predicates, callback);
    return this.fastify.listen({ host: this.serverOpts.hostname, port: this.serverOpts.port });
  }

  async close() {
    await this.fastify?.close();
    this.fastify = undefined;
  }
}
