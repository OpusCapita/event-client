/* global after:true, before:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const { EventClient } = require('../lib');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

describe('EventClient multiple instances', () => {

    const consulOverride = { };
    let publisherClient,
        subscriberClient,
        subscriberClient1,
        subscriberClient2;

    before('Init', async () =>
    {
        await configService.init({ logger : Logger.DummyLogger });
        const config = EventClient.DefaultConfig.consul;
        const endpoint = await configService.getEndPoint(config.mqServiceName);
        const username = config.mqUserKey && await configService.get(config.mqUserKey);
        const password = config.mqPasswordKey && await configService.get(config.mqPasswordKey);

        consulOverride.host = endpoint.host;
        consulOverride.port = endpoint.port;
        consulOverride.username = username;
        consulOverride.password = password;

    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });

    afterEach(async () => {
            publisherClient && await publisherClient.dispose(); publisherClient = null;
            subscriberClient && await subscriberClient.dispose(); subscriberClient = null;
            subscriberClient1 && await subscriberClient1.dispose(); subscriberClient1 = null;
            subscriberClient2 && await subscriberClient2.dispose(); subscriberClient2 = null;
    });

    it('Simple test (2 clients)', async () =>
    {
        subscriberClient = new EventClient({ logger : Logger.DummyLogger,  context : { nix : 1 } });
        publisherClient  = new EventClient({ logger : Logger.DummyLogger });

        publisherClient.contextify({truth: 42});

        const routingKey = 'event-client.Test';
        const input      = {message: 'Simple_Test'};
        const result     = {};

        await subscriberClient.init();

        await subscriberClient.subscribe(routingKey, async (payload, context, key) =>
        {
            delete context.timestamp;

            result.payload = payload;
            result.context = context;
            result.key = key;
        });

        await sleep(500);

        assert(await subscriberClient.hasSubscription(routingKey));

        await publisherClient.emit(routingKey, input);
        assert(await subscriberClient.hasSubscription(routingKey));

        assert(await subscriberClient.exchangeExists('event-client'), true);
        assert.equal(await subscriberClient.hasSubscription('invalid'), false);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await subscriberClient.unsubscribe(routingKey), true);
    });

    it('Simple connection with ACK (2 clients)', async () =>
    {
        let iteration = 0;

        subscriberClient = new EventClient({ logger : Logger.DummyLogger });
        publisherClient = new EventClient({ logger : Logger.DummyLogger });

        const routingKey = 'event-client.ACK';
        const input = { message: 'Test-ACK-Value' };

        await subscriberClient.subscribe(routingKey, (payload, context, key) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if(iteration === 1)
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

    it('Pattern test (2 clients)', async () =>
    {
        publisherClient = new EventClient({ logger : Logger.DummyLogger, new : 1 });
        subscriberClient = new EventClient({ logger : Logger.DummyLogger, new : 2 });

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
        subscriberClient1 = new EventClient({ logger : Logger.DummyLogger });
        subscriberClient2 = new EventClient({ logger : Logger.DummyLogger });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };
        let result = false;

        await subscriberClient1.init();
        await subscriberClient2.init();

        await subscriberClient1.subscribe(routingKey, async (payload, context, key) => null);
        await subscriberClient2.subscribe(routingKey, async (payload, context, key) => null).catch(e => result = true);

        assert.equal(result, true);

        await sleep(1000);
    });

    it('Dispose test 1', async () =>
    {
        subscriberClient = new EventClient({ logger : Logger.DummyLogger });
        publisherClient = new EventClient({ logger : Logger.DummyLogger });

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
    });


    it('Dispose test 2', async () =>
    {
        subscriberClient = new EventClient({ logger : Logger.DummyLogger, queueName : 'test' });
        publisherClient = new EventClient({ logger : Logger.DummyLogger });

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
    });
});

