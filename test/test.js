const assert = require('assert');
const configService = require('@opuscapita/config');
const { EventClient } = require('../lib');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

describe('Main', () =>
{
    const consulOverride = { };

    /**
    * Check rabbitMQ is ready
    */
    before('ACL connection', async () =>
    {
        await configService.init();
        const config = EventClient.DefaultConfig.consul;
        const endpoint = await configService.getEndPoint(config.mqServiceName);
        const username = config.mqUserKey && await configService.get(config.mqUserKey);
        const password = config.mqPasswordKey && await configService.get(config.mqPasswordKey);

        consulOverride.host = endpoint.host;
        consulOverride.port = endpoint.port;
        consulOverride.username = username;
        consulOverride.password = password;
    });

    /*it('Blupp 1', async () =>
    {
        const client = new EventClient();
        await client.init();

        assert.equal(await client.queueExists('nix').catch(e => null), false);
        await client.subscribe('event-client.shit', console.log);
        await client.emit('event-client.shit', { truth : 42 });

        await sleep(5000);
    });*/

    it('Simple test (1 client)', async () =>
    {
        const client = new EventClient({ context : { nix : 1 } });

        client.contextify({ truth : 42 });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };
        const result =  { };

        await client.init();

        await client.subscribe(routingKey, async (payload, context, key) =>
        {
            delete context.timestamp;

            result.payload = payload;
            result.context = context;
            result.key = key;
        })

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.exchangeExists('event-client'), true);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await client.unsubscribe(routingKey), true);
    });

    it('Simple test (2 clients)', async () =>
    {
        const subscriberClient = new EventClient({ context : { nix : 1 } });
        const publisherClient = new EventClient();

        publisherClient.contextify({ truth : 42 });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };
        const result =  { };

        await subscriberClient.init();

        await subscriberClient.subscribe(routingKey, async (payload, context, key) =>
        {
            delete context.timestamp;

            result.payload = payload;
            result.context = context;
            result.key = key;
        })

        await sleep(500);

        assert(await subscriberClient.hasSubscription(routingKey));
        await publisherClient.emit(routingKey, input);
        assert(await subscriberClient.hasSubscription(routingKey));

        await sleep(500);

        assert(await subscriberClient.exchangeExists('event-client'), true);
        assert.equal(await subscriberClient.hasSubscription('invalid'), false);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await subscriberClient.unsubscribe(routingKey), true);
    });

    it('Simple connection with ACK (1 clients)', async () =>
    {
        let iteration = 0;

        const client = new EventClient();
        const routingKey = 'event-client.ACK';
        const input = { message: 'Test-ACK-Value' };

        await client.subscribe(routingKey, async (payload, context, key) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if(iteration == 1)
                return false;
            else if(iteration == 2)
                throw new Error();

            return true;
        });

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(iteration, 3);

        await client.unsubscribe(routingKey);
    });

    it('Simple connection with ACK (2 clients)', async () =>
    {
        let iteration = 0;

        const subscriberClient = new EventClient();
        const publisherClient = new EventClient();
        const routingKey = 'event-client.ACK';
        const input = { message: 'Test-ACK-Value' };

        await subscriberClient.subscribe(routingKey, async (payload, context, key) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if(iteration == 1)
                return false;
            else if(iteration == 2)
                throw new Error();

            return true;
        });

        await sleep(500);

        await publisherClient.emit(routingKey, input);

        await sleep(500);

        assert.equal(iteration, 3);

        await subscriberClient.unsubscribe(routingKey);
    });

    it('Multiple (different) instances', async () =>
    {
        let iteration = 0;

        const routingKey = 'event-client.Instances';
        const input = { message: 'Test-ACK-Value' };
        const publisherClient = new EventClient();
        const subscriberClient1 = new EventClient({ consulOverride });
        const subscriberClient2 = new EventClient();

        const callback = (payload, context, key) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if(iteration === 1)
                return false;
            if(iteration === 2)
                throw new Error();
        };

        await Promise.all([
            subscriberClient1.subscribe(routingKey, callback),
            subscriberClient2.subscribe(routingKey, callback, { messageLimit : 5 })
        ]);

        await publisherClient.emit(routingKey, input);

        await sleep(2000);

        assert.equal(iteration, 3);

        await Promise.all([
            subscriberClient1.unsubscribe(routingKey),
            subscriberClient2.unsubscribe(routingKey)
        ]);
    });

    it('Pattern test (1 client)', async () =>
    {
        const client = new EventClient();
        const routingPattern = 'event-client.#';
        const routingKey = 'event-client.test';

        let iterator = 0;
        let output;
        const input = { message: 'Test-pattern' }

        await client.subscribe(routingPattern, (payload, context, key) =>
        {
            iterator++;
            output = payload;
        })

        await client.emit(routingKey, input);
        await sleep(2000);
        await client.unsubscribe(routingPattern);

        assert.equal(await client.queueExists(client.getQueueName(routingPattern)), true);
        assert.equal(await client.deleteQueue(client.getQueueName(routingPattern)), true);
        assert.equal(await client.queueExists(client.getQueueName(routingPattern)), false);

        assert.equal(iterator, 1);
        assert.deepEqual(output, input);
    });

    it('Pattern test (2 clients)', async () =>
    {
        const publisherClient = new EventClient({ new : 1 });
        const subscriberClient = new EventClient({ new : 2 });
        const routingPattern = 'event-client.#';
        const routingKey = 'event-client.test';

        let iterator = 0;
        let output;
        const input = { message: 'Test-pattern' }

        await subscriberClient.subscribe(routingPattern, (payload, context, key) =>
        {
            iterator++;
            output = payload;
        })

        await publisherClient.emit(routingKey, input);
        await sleep(2000);
        await subscriberClient.unsubscribe(routingPattern);

        assert.equal(await subscriberClient.queueExists(subscriberClient.getQueueName(routingPattern)), true);
        await subscriberClient.deleteQueue(subscriberClient.getQueueName(routingPattern));
        assert.equal(await subscriberClient.queueExists(subscriberClient.getQueueName(routingPattern)), false);

        assert.equal(iterator, 1);
        assert.deepEqual(output, input);
    });

    it('Double subscription (2 clients)', async () =>
    {
        const client1 = new EventClient();
        const client2 = new EventClient();

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };
        let result = false;

        await client1.init();
        await client2.init();

        await client1.subscribe(routingKey, async (payload, context, key) => null);
        await client2.subscribe(routingKey, async (payload, context, key) => null).catch(e => result = true);

        assert.equal(result, true);

        client1.dispose();
        client2.dispose();
    });

    it('Dispose test 1', async () =>
    {
        const subscriberClient = new EventClient();
        const publisherClient = new EventClient();
        const routingKey = 'event-client.dispose';
        const input = { message : 'Gone!' };

        const callback = (payload) => assert.deepEqual(payload, input);

        await subscriberClient.subscribe(routingKey, callback);
        assert.equal(42, await subscriberClient.subscribe(routingKey, callback).catch(e => 42));
        await subscriberClient.disposeSubscriber();
        await publisherClient.emit(routingKey, input);
        await publisherClient.disposePublisher();
        await publisherClient.disposePublisher();
        await subscriberClient.subscribe(routingKey, callback);
        await subscriberClient.dispose();
    });

    it('Dispose test 2', async () =>
    {
        const subscriberClient = new EventClient({ queueName : 'test' });
        const publisherClient = new EventClient();
        const routingKey = 'event-client.dispose';
        const input = { message : 'Gone!' };

        const callback = (payload) => assert.deepEqual(payload, input);

        await subscriberClient.subscribe(routingKey, callback);
        await subscriberClient.emit(routingKey, input);
        await subscriberClient.unsubscribe(routingKey);
        await subscriberClient.disposeSubscriber();
        await subscriberClient.disposeSubscriber();
        await subscriberClient.unsubscribe(routingKey);
        await publisherClient.emit(routingKey, input);
        await publisherClient.disposePublisher();
        await publisherClient.disposePublisher();
        await subscriberClient.subscribe(routingKey, callback);
        await subscriberClient.dispose();
    });

    it('Dispose test 3', async () =>
    {
        const client = new EventClient({ queueName : 'test' });
        const routingKey = 'event-client.dispose';
        const input = { message : 'Gone!' };

        let emitCount = 0;
        const callback = (payload) => { assert.deepEqual(payload, input); emitCount++ }

        await client.subscribe(routingKey, callback);
        await client.emit(routingKey, input);
        await sleep(2000);
        await client.dispose();
        await client.subscribe(routingKey, callback);
        await client.emit(routingKey, input);
        await sleep(2000);
        await client.dispose();

        assert.equal(emitCount, 2);
    });

    it('Error test 1', async () =>
    {
        const client = new EventClient();

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();

        await client.subscribe(routingKey, async (payload, context, key) =>
        {
            throw new Error();
        });

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    it('Error test 2', async () =>
    {
        const client = new EventClient({ parserContentType : 'fail' });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();
        await client.subscribe(routingKey, async (payload, context, key) => null);

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    // it('Error test 3', async () =>
    // {
    //     const client = new EventClient({ parserContentType : 'fail' });
    //
    //     const routingKey = 'event-client.Test';
    //     const input = { message: 'Simple_Test' };
    //
    //     await client.init();
    //     await client.subscribe(routingKey, async (payload, context, key) => null);
    //
    //     await sleep(40000);
    //
    //     await client.emit(routingKey, input);
    //
    //     await sleep(500);
    //
    //     await client.dispose();
    // });
});
