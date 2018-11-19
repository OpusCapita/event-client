/* global after:true, before:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');

const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');
const rabbitCmd = require('./helpers/rabbitmq');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));


describe('EventClient single instance tests', () => {

    const consulOverride = { };
    let client;

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

        await rabbitCmd.awaitRabbitCluster(endpoint, username, password);

        return true;
    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });

    afterEach(async () => {
        client && await client.dispose();
        client = null;
    });

    it('Simple test (1 client)', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger, context : { nix : 1 } });

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
    });

    it('Simple test (reconnect)', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger, context : { nix : 1 } });

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

        await configService.setProperty('mq/password', process.env.RABBITMQ_PASS);

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.exchangeExists('event-client'), true);

        assert.deepEqual(result.payload, input);
        assert.deepEqual(result.context, { truth : 42, senderService : 'event-client' });
        assert.equal(result.key, routingKey);

        assert.equal(await client.unsubscribe(routingKey), true);
    });

    it('Simple connection with ACK (1 clients)', async () =>
    {
        let iteration = 0;

        client = new EventClient({ logger : Logger.DummyLogger });
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

    it('Pattern test (1 client)', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger });

        const routingPattern = 'event-client.pattern.#';
        const routingKey = 'event-client.pattern.test';

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
    });

    it('Dispose test 3', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger, queueName : 'test' });
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

        assert.equal(emitCount, 2);
    });

    it('Error test 1', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger });

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
    });

    it('Error test 2', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger, parserContentType : 'fail' });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();
        await client.subscribe(routingKey, async (payload, context, key) => null);

        await sleep(500);

        await client.emit(routingKey, input);

        await sleep(500);

        assert.equal(await client.unsubscribe(routingKey), true);
    });

    it('Error test 3', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger, parserContentType : 'fail' });

        const routingKey = 'event-client.Test';
        const input = { message: 'Simple_Test' };

        await client.init();
        await client.subscribe(routingKey, async (payload, context, key) => null);

        await sleep(1000);

        await client.emit(routingKey, input);
    });

    it('Testing getMessage', async () =>
    {
        client = new EventClient({ logger : Logger.DummyLogger });

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
    });

});

