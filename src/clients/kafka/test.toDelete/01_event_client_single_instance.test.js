/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const extend = require('extend');
const retry  = require('bluebird-retry');

const Promise = require('bluebird');

const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../src');
const {ConsumerError} = require('../src/err');

const subscribedTopics = require('../src/TopicSubscription');

const consulOverride = {
    host:  'kafka1',
    port: 9092
};

const eventClientFactory = (config) => {
    return new EventClient(extend(true, {
        serviceName: 'event-client-2000',
        consumerGroupId: 'test',
        consulOverride,
        logger: Logger.DummyLogger
    }, config));
};

const noopFn = () => {};

describe('EventClient single instance tests', () => {

    before(async () =>
    {
        return await configService.init({logger : Logger.DummyLogger});
    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });

    describe('#constructor', () => {
        let client;

        before(() => client = eventClientFactory({consumerGroupId:null}));

        after(async () => {
            client && await client.dispose();
            client = null;
        });

        it('Creates a new instance', () => {
            assert(client !== null);
        });

        it('Should setup the service name via configService', () => {
            assert.equal(client.config.serviceName, 'event-client-2000');
        });

        it('Should set the consumer group to the service name.', () => {
            assert.equal(client.config.consumerGroupId, 'event-client-2000');
        });

        it('Should set the mqServiceName to kafka.', () => {
            assert.equal(client.config.consul.mqServiceName, 'kafka');
        });
    });

    describe('Consuming.', () => {
        let client;

        beforeEach(() => client = eventClientFactory());

        afterEach(async () => {
            client && await client.dispose();
            client = null;
        });

        it('Should subscribe to a topic based on a pattern.', async () => {
            const result = await client.subscribe('^pattern.*.com', () => {});

            assert.strictEqual(result, true);
        });

        it('Should fail on double subscription to the same topic.', async () => {
            let ok = false;

            try {
                await client.subscribe('test.double.subscriptions', () => {});
                await client.subscribe('test.double.subscriptions', () => {});
            } catch (e) {
                ok = e instanceof ConsumerError && e.errno === 409;
            }

            assert.equal(ok, true);
        });

        it('Should indicate failure on trying to unsubscribe from a not subscribed topic.', async () => {
            const result = await client.unsubscribe('test.unsubscribe.notexist');
            assert.strictEqual(result, false);
        });

        it('Should unsubscribe from a topic.', async () => {
            await client.subscribe('test.unsubscribe.test1', () => true);
            await client.subscribe('test.unsubscribe.test2', () => true);
            assert(client.consumer._subscribedTopics.size === 2);

            await client.unsubscribe('test.unsubscribe.test1');
            assert(client.consumer._subscribedTopics.size === 1);

            await client.dispose();
        });

        it('Should successfully remove all transactions (sinek bug workaround test).', async () => {
            await client.subscribe('test.unsubscribe.all', () => true);
            assert(client.consumer._subscribedTopics.size === 1);

            await client.unsubscribe('test.unsubscribe.all');
            assert(client.consumer._subscribedTopics.size === 0);
        });

    });

    describe('Producing.', () => {
        let client;

        beforeEach(() => client = eventClientFactory({
            consumerGroupId: `test-${Date.now()}` // Consumer group id randomization fixes problem with timeouts on rebalancing on kafka
        }));

        afterEach(async () => {
            client && await client.dispose();
            client = null;
        });

        it('Should create topics before publishing to them.', async () => {
            await client.init();

            assert(client.producer.knownTopics.length === 0);

            await client.publish(`test.producing.create.topic.${Date.now()}`, 'ping');
            assert(client.producer.knownTopics.length === 1);
        });

        it('Should publish messages to a topic.', async () => {
            const msg = `ping ${Date.now()}`;
            const receivedMessages = [];

            await client.subscribe('test.producing', (message) => {
                receivedMessages.push(message);
            });

            await client.publish('test.producing', msg);

            let ok = await retry(() => {
                if (receivedMessages.includes(msg)) {
                    return Promise.resolve(true);
                } else {
                    return Promise.reject(new Error('Message not yet received'));
                }
            }, {'max_tries': 80}); // Long wait interval, kafka rebalancing takes some time

            assert(ok);
        });

        it('Messages should allow custom context to be applied to the mesasge.', async () => {
            const msg = `ping ${Date.now()}`;
            const ctx = {chuck: 'testa'};

            const receivedMessages = new Map();

            await client.subscribe('test.producing', (message, context) => receivedMessages.set(message, context));
            await client.publish('test.producing', msg, ctx);

            let receivedCtx = await retry(() => {
                if (receivedMessages.has(msg) && receivedMessages) {
                    return Promise.resolve(receivedMessages.get(msg));
                } else {
                    return Promise.reject(new Error('Message not yet received'));
                }
            }, {'max_tries': 50}); // Long wait interval, kafka rebalancing takes some time

            assert(receivedCtx.hasOwnProperty('chuck'));
        });

    });

    describe('Methods', () => {

        describe('#dispose', () => {
            it('Should remove all subscriptions from the global subscription registry.', async () => {
                if (subscribedTopics.size > 0) { console.log('TOCHECK: ', subscribedTopics.keys()); }
                subscribedTopics.clear(); // Clean up if some disposing in previous tests did not work.

                const client = eventClientFactory();
                await client.subscribe('test.disposetest', noopFn);

                assert(subscribedTopics.size >= 1);

                let r = await client.dispose();

                assert.strictEqual(r, true);
                assert.strictEqual(subscribedTopics.size, 0);
            });
        });

        describe('#hasSubscription', () => {
            let client;

            beforeEach(() => client = eventClientFactory());

            afterEach(async () => {
                client && await client.dispose();
                client = null;
            });

            it('Should indicate if a topic was subscribed.', async () => {
                await client.subscribe('test.hassubscription', noopFn);

                assert.equal(client.hasSubscription('test.hassubscription'), true);
                assert.equal(client.hasSubscription('test.baz'), false);
                assert.throws(() => client.hasSubscription([]));
            });

        });

    });


});
