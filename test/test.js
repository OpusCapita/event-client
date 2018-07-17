/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

describe('EventClient basics', () =>
{
    const consulOverride = { };

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

    it('Should subscribe, emit, receive, unsubscribe and dispose with a single instance (1 client)', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger, context : { nix : 1 } });

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
        });

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.exchangeExists('event-client'), true);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    it('Should reconnect on config change.', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger, context : { nix : 1 } });

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
        });

        await sleep(500);

        await configService.setProperty('mq/password', process.env.RABBITMQ_PASS);

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.exchangeExists('event-client'), true);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    it('Simple test (2 clients)', async () =>
    {
        const subscriberClient = new EventClient({ logger : Logger.DummyLogger,  context : { nix : 1 } });
        const publisherClient = new EventClient({ logger : Logger.DummyLogger });

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
        });

        await sleep(500);

        assert(await subscriberClient.hasSubscription(routingKey));
        await publisherClient.emit(routingKey, input);
        assert(await subscriberClient.hasSubscription(routingKey));

        await sleep(500);

        assert.equal(await subscriberClient.exchangeExists('event-client'), true);
        assert.equal(await subscriberClient.hasSubscription('invalid'), false);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await subscriberClient.unsubscribe(routingKey), true);

        await subscriberClient.dispose();
        await publisherClient.dispose();
    });

    it('Should reenqueue messages that are rejected by the callback function. (1 client)', async () =>
    {
        let iteration = 0;

        const client = new EventClient({ logger : Logger.DummyLogger });
        const routingKey = 'event-client.ACK';
        const input = { message: 'Test-ACK-Value' };

        await client.subscribe(routingKey, async (payload) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if (iteration === 1)
                return false;
            else if (iteration === 2)
                throw new Error();

            return true;
        });

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(iteration, 3);

        await client.unsubscribe(routingKey);
        await client.dispose();
    });

    it('Should reenqueue messages that are rejected by the callback function. (2 client)', async () =>
    {
        let iteration = 0;

        const subscriberClient = new EventClient({ logger : Logger.DummyLogger });
        const publisherClient = new EventClient({ logger : Logger.DummyLogger });
        const routingKey = 'event-client.ACK';
        const input = { message: 'Test-ACK-Value' };

        await subscriberClient.subscribe(routingKey, async (payload) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if (iteration === 1)
                return false;
            else if (iteration === 2)
                throw new Error();

            return true;
        });

        await sleep(500);

        await publisherClient.emit(routingKey, input);

        await sleep(500);

        assert.equal(iteration, 3);

        await subscriberClient.unsubscribe(routingKey);

        await subscriberClient.dispose();
        await publisherClient.dispose();
    });

    it('Should reenqueue messages that are rejected by the callback function. (1 publisher, 2 subscribers)', async () =>
    {
        let iteration = 0;

        const routingKey = 'event-client.Instances';
        const input = { message: 'Test-ACK-Value' };
        const publisherClient = new EventClient({ logger : Logger.DummyLogger });
        const subscriberClient1 = new EventClient({ logger : Logger.DummyLogger, consulOverride });
        const subscriberClient2 = new EventClient({ logger : Logger.DummyLogger });

        const callback = (payload) =>
        {
            iteration++;

            assert.deepEqual(payload, input);

            if (iteration === 1) {
                return false;
            }

            if (iteration === 2) {
                throw new Error();
            }
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

        await publisherClient.dispose();
        await subscriberClient1.dispose();
        await subscriberClient2.dispose();
    });

    it('It should receive messages based on a pattern (1 client)', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger });
        const routingPattern = 'event-client.#';
        const routingKey = 'event-client.test';

        let iterator = 0;
        let output;
        const input = { message: 'Test-pattern' };

        await client.subscribe(routingPattern, (payload) =>
        {
            iterator++;
            output = payload;
        });

        await client.emit(routingKey, input);
        await sleep(2000);
        await client.unsubscribe(routingPattern);

        assert.equal(await client.queueExists(client.getQueueName(routingPattern)), true);
        assert.equal(await client.deleteQueue(client.getQueueName(routingPattern)), true);
        assert.equal(await client.queueExists(client.getQueueName(routingPattern)), false);

        assert.equal(iterator, 1);
        assert.deepEqual(output, input);

        await sleep(1000);

        await client.dispose();
    });

    it('It should receive messages based on a pattern (1 publisher, 1 subscriber)', async () =>
    {
        const publisherClient = new EventClient({ logger : Logger.DummyLogger, new : 1 });
        const subscriberClient = new EventClient({ logger : Logger.DummyLogger, new : 2 });
        const routingPattern = 'event-client.#';
        const routingKey = 'event-client.test';

        let iterator = 0;
        let output;
        const input = { message: 'Test-pattern' };

        await subscriberClient.subscribe(routingPattern, (payload) =>
        {
            iterator++;
            output = payload;
        });

        await publisherClient.emit(routingKey, input);
        await sleep(2000);
        await subscriberClient.unsubscribe(routingPattern);

        assert.equal(await subscriberClient.queueExists(subscriberClient.getQueueName(routingPattern)), true);
        await subscriberClient.deleteQueue(subscriberClient.getQueueName(routingPattern));
        assert.equal(await subscriberClient.queueExists(subscriberClient.getQueueName(routingPattern)), false);

        assert.equal(iterator, 1);
        assert.deepEqual(output, input);

        await sleep(1000);

        await publisherClient.dispose();
        await subscriberClient.dispose();
    });

    it('It should fail on double subscriptions (2 client instances)', async () =>
    {
        const client1 = new EventClient({ logger : Logger.DummyLogger });
        const client2 = new EventClient({ logger : Logger.DummyLogger });

        const routingKey = 'event-client.Test';
        let result = false;

        await client1.init();
        await client2.init();

        await client1.subscribe(routingKey, async () => null);
        await client2.subscribe(routingKey, async () => null).catch(() => result = true);

        assert.equal(result, true);

        await sleep(1000);

        client1.dispose();
        client2.dispose();
    });

    it('Dispose test 1', async () =>
    {
        const subscriberClient = new EventClient({ logger : Logger.DummyLogger });
        const publisherClient = new EventClient({ logger : Logger.DummyLogger });
        const routingKey = 'event-client.dispose';
        const input = { message : 'Gone!' };

        const callback = (payload) => assert.deepEqual(payload, input);

        await subscriberClient.subscribe(routingKey, callback);
        assert.equal(42, await subscriberClient.subscribe(routingKey, callback).catch(() => 42)); // FIXME why?
        await subscriberClient.disposeSubscriber();
        await publisherClient.emit(routingKey, input);
        await publisherClient.disposePublisher();
        await publisherClient.disposePublisher();
        await subscriberClient.subscribe(routingKey, callback);
        await subscriberClient.dispose();

        await publisherClient.dispose();
        await subscriberClient.dispose();
    });


    it('Dispose test 2', async () =>
    {
        const subscriberClient = new EventClient({ logger : Logger.DummyLogger, queueName : 'test' });
        const publisherClient = new EventClient({ logger : Logger.DummyLogger });
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
        const client = new EventClient({ logger : Logger.DummyLogger, queueName : 'test' });
        const routingKey = 'event-client.dispose';
        const input = { message : 'Gone!' };

        let emitCount = 0;
        const callback = (payload) => { assert.deepEqual(payload, input); emitCount++; };

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
        const client = new EventClient({ logger : Logger.DummyLogger });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();

        await client.subscribe(routingKey, async () =>
        {
            throw new Error();
        });

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    it('Should successfully unsubscribe from a topic.', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger, parserContentType : 'fail' });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();
        await client.subscribe(routingKey, async () => null);

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.unsubscribe(routingKey), true);

        await client.dispose();
    });

    it('Error test 3', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger, parserContentType : 'fail' });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();
        await client.subscribe(routingKey, async () => null);

        await sleep(1000);

        await client.emit(routingKey, input);

        await sleep(500);

        await client.dispose();
    });

    it('Testing getMessage', async () =>
    {
        const client = new EventClient({ logger : Logger.DummyLogger });

        const routingKey = 'event-client.TestGetMessage';
        const input = { message: 'Hello, world!' };

        await client.init();
        await client.subscribe(routingKey);

        {
            await client.emit(routingKey, input);

            await sleep(500);

            const result = await client.getMessage(routingKey);
            assert.equal(await client.getMessage(routingKey), false);

            delete result.context.timestamp;

            assert.deepEqual(result.payload, input);
            assert.deepEqual(result.context, { senderService : 'event-client' });
            assert.equal(result.topic, routingKey);
        }

        {
            await client.emit(routingKey, input);

            await sleep(500);

            let message = await client.getMessage(routingKey, false);
            assert.equal(await client.getMessage(routingKey), false);

            delete message.context.timestamp;

            assert.deepEqual(message.payload, input);
            assert.deepEqual(message.context, { senderService : 'event-client' });
            assert.equal(message.topic, routingKey);

            await client.nackMessage(message);

            message = await client.getMessage(routingKey, false);
            assert.equal(await client.getMessage(routingKey), false);

            delete message.context.timestamp;

            assert.deepEqual(message.payload, input);
            assert.deepEqual(message.context, { senderService : 'event-client' });
            assert.equal(message.topic, routingKey);

            await client.ackMessage(message);

            assert.equal(await client.getMessage(routingKey), false);
        }

        assert.equal(await client.unsubscribe(routingKey), null);

        await client.dispose();
    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });
});

describe('EventClient: connection blocked behaviour', () =>
{
    const consulOverride = { };
    let subscriberClient,
        publisherClient;

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

    beforeEach(() => {
        subscriberClient = new EventClient({ logger : Logger.DummyLogger });
        publisherClient = new EventClient({ logger : new Logger() });
    });

    afterEach(async () => {
        await publisherClient.dispose();
        await subscriberClient.dispose();
    });

    it('Should queue messages when the connection is in blocked.', async () => {

        const routingKey = 'event-client.test.waitqueue';

        let receivedMessageCounter = 0;

        await subscriberClient.subscribe(routingKey, () => {
            receivedMessageCounter++;
            return true;
        });

        await publisherClient.init();

        publisherClient.connection.events.emit('connection_blocked');

        for (let i of [1, 2, 3]) {
            await publisherClient.emit(routingKey, {count: i});
        }

        assert.strictEqual(receivedMessageCounter, 0);
        assert.strictEqual(publisherClient.pubChannel.waitQueue.size, 3);
    });

    it('Should flush the waitQueue after the connection is unblocked.', async () => {
        const routingKey = 'event-client.test.waitqueue';

        let receivedCounter = 0;

        await publisherClient.init();
        await subscriberClient.subscribe(routingKey, () => {
            receivedCounter++;
            return true;
        });

        publisherClient.connection.events.emit('connection_blocked');

        await sleep(100);

        for (let i of [1, 2, 3]) {
            await sleep(100);
            await publisherClient.emit(routingKey, {count: i});
        }

        publisherClient.connection.events.emit('connection_unblocked');
        // publisherClient.pubChannel.useWaitQueue = false;
        // await publisherClient.pubChannel.flushWaitQueue();

        await sleep(100);

        assert.strictEqual(receivedCounter, 3);
        assert.strictEqual(publisherClient.pubChannel.waitQueue.size, 0);

    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });
});
