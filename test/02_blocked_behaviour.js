/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');

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
