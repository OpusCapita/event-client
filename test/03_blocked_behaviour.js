/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');

const rabbitCmd = require('./helpers/rabbitmq');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

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
        publisherClient = new EventClient({ logger : Logger.DummyLogger });
    });

    afterEach(async () => {
        try {
            await publisherClient.dispose();
            await subscriberClient.dispose();
        } catch (e) {
            /* handle error */
            console.error(e);
        }
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

    it('Should change the connection state to BLOCKED on cluster warning', async () => {
        await publisherClient.init();

        await rabbitCmd.blockRabbit(1);
        await rabbitCmd.blockRabbit(2);
        await sleep(500);

        await publisherClient.emit('event-client.test', { pickle: 'rick'}, null, {ttl: 1000});

        let state = publisherClient.connection.connectionState;

        try {
            await rabbitCmd.unblockRabbit(1);
            await rabbitCmd.unblockRabbit(2);
        } catch (e) {
            console.log(e);
        }

        assert.strictEqual(state, publisherClient.connection.constructor.CS_BLOCKED);
    });

    it('Should not send the message that triggered the block state twice (eg. on flushWaitQueue)', async () => {
        const routingKey = 'event-client.test.waitqueue';
        let receivedCounter = 0;

        await publisherClient.init();
        await subscriberClient.init();

        let waitOnBlock = new Promise((resolve) => {
            publisherClient.connection.events.on('connection_blocked', () => resolve(true));
        });
        let waitOnUnblock = new Promise((resolve) => {
            publisherClient.connection.events.on('connection_unblocked', () => resolve(true));
        });
        let waitOnFlush = new Promise((resolve) => {
            publisherClient.pubChannel.events.on('waitqueue_flushed', () => resolve(true));
        });

        await publisherClient.emit(routingKey, {sent: Date.now()}, null, {ttl: 500});
        await publisherClient.emit(routingKey, {sent: Date.now()}, null, {ttl: 500});
        await publisherClient.emit(routingKey, {sent: Date.now()}, null, {ttl: 500});

        await rabbitCmd.blockRabbit(1);
        await rabbitCmd.blockRabbit(2);

        await publisherClient.emit(routingKey, {should: 'block', sent: Date.now()}, null, {ttl: 500});

        await publisherClient.emit(routingKey, {should: 'be in waitqueue', sent: Date.now()}, null, {ttl: 500});
        await publisherClient.emit(routingKey, {should: 'be in waitqueue', sent: Date.now()}, null, {ttl: 500});

        await waitOnBlock;

        await rabbitCmd.unblockRabbit(1);
        await rabbitCmd.unblockRabbit(2);

        /* Wait for unblock event */
        await waitOnUnblock;

        await subscriberClient.subscribe(routingKey, () => {
            receivedCounter++;
            return true;
        });

        await waitOnFlush;
        await sleep(600); // Give it one more second for the last message to be acked

        assert.strictEqual(receivedCounter, 6);
    });

    it('Should allow dispose on blocked connections.');
    it('EventClient#init should not wait indefintly when client comes up with broker in blocking state.');

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });

});
