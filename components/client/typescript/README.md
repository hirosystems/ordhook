# Chainhook Client

A TypeScript client library that allows you to configure and listen for fully-typed Chainhook
predicate events.

You can either create predicates and send them to a Chainhook node manually or use the included web
server helpers to handle all node interactions transparently.

## Quick Start

1. Install npm package
    ```
    npm install @hirosystems/chainhook-client
    ```

1. Create a `Predicate` object for every Chainhook event you're interested in. See the Chainhook
   [README](https://github.com/hirosystems/chainhook#readme) for more examples.
    ```typescript
    import { ServerPredicate } from "@hirosystems/chainhook-client";
    import { randomUUID } from "crypto";

    const uuid = randomUUID();
    const predicate: ServerPredicate = {
        uuid,
        name: "test",
        version: 1,
        chain: "stacks",
        networks: {
            mainnet: {
                // `then_that` will be filled in automatically.
                if_this: {
                    scope: 'block_height',
                    higher_than: 100000,
                }
            }
        }
    };
    ```

1. Create configuration objects for the local server and the Chainhook node you'll be interacting
   with
    ```typescript
    import { ServerOptions, ChainhookNodeOptions } from "@hirosystems/chainhook-client";

    // Local server options
    const opts: ServerOptions = {
        hostname: "0.0.0.0",
        port: 3000,
        auth_token: "<random_string>",
        // Configure this value to a hostname the Chainhook node can use to reach our local server.
        // e.g. http://local.server:3000
        external_base_url: "<external_base_url>"
    };

    // Chainhook node options
    const chainhook: ChainhookNodeOptions = {
        base_url: "<node_base_url>"
    };
    ```

1. Declare and start the event server
    ```typescript
    import { Payload } from "@hirosystems/chainhook-client";

    const server = new ChainhookEventObserver(opts, chainhook);
    server.start(
        [predicate],
        async (uuid: string, payload: Payload) => {
            // This handler will be called for every chainhook event received by our server
            console.log(uuid);
            console.log(payload);
        }
    )
        .catch(e => console.error(e));
    const close = async () => {
        await server.close();
    }
    process.once('SIGINT', close);
    process.once('unhandledRejection', close);
    process.once('uncaughtException', close);
    process.once('beforeExit', close);
    ```
    Make sure you close the server gracefully in the event of an error so we can de-register our
    predicates correctly from the remote Chainhook node.

## Type Definitions

This library includes type definitions for all possible Chainhook predicate configurations and event
payloads in the form of [Typebox](https://github.com/sinclairzx81/typebox) schemas. If you need to
compile or validate any of the supplied types, generate JSON schemas, etc., you can use all of
Typebox's tools to do so.
