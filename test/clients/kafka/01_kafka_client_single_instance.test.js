/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const extend = require('extend');
const retry  = require('bluebird-retry');

const Promise = require('bluebird');

const configService = require('@opuscapita/config');
const Logger        = require('ocbesbn-logger');

const KafkaClient      = require('../../../src/clients/kafka/');
const {ConsumerError}  = require('../../../src/clients/kafka/err/');

const consulOverride = {
    hosts: 'kafka1:9092,kafka2:9093,kafka3:9094'
};

const eventClientFactory = (config) => {
    return new KafkaClient(extend(true, {
        serviceName: 'event-client-test',
        consumerGroupId: 'test',
        consulOverride,
        logger: Logger.DummyLogger
    }, config));
};

const noopFn = () => {};
const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

describe('KafkaClient single instance tests', () => {

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
            assert.equal(client.config.serviceName, 'event-client-test');
        });

        it('Should set the consumer group to the service name.', () => {
            assert.equal(client.config.consumerGroupId, 'event-client-test');
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

        it('Should fail on double subscription to the same topic w/o pattern.', async () => {
            let ok = false;

            try {
                await client.subscribe('test.double.subscriptions', () => {});
                await client.subscribe('test.double.subscriptions', () => {});
            } catch (e) {
                ok = e instanceof ConsumerError && e.errno === 409;
            }

            assert.equal(ok, true);
        });

        it('Should fail on double subscription to the same topic w/ pattern.', async () => {
            let ok = false;

            try {
                await client.subscribe('^test.double\\S*', () => {});
                await client.subscribe('^test.double\\S*', () => {});
            } catch (e) {
                ok = e instanceof ConsumerError && e.errno === 409;
            }

            assert.equal(ok, true);
        });

        it('Should indicate failure on trying to unsubscribe from a not subscribed topic.', async () => {
            const result = await client.unsubscribe('test.unsubscribe.notexist');
            assert.strictEqual(result, false);
        });

        it('Should subscribe to multiple subjects on the same topic.', async () => {
            await client.subscribe('test.subscribe.test1', () => true, {}, true);
            await client.subscribe('test.subscribe.test2', () => true, {}, true);

            assert(client.consumer._subjectRegistry.get('^test\\.subscribe$').size === 2);

            await client.dispose();
        });

        it('Should unsubscribe from a subject.', async () => {
            await client.subscribe('test.unsubscribe.test1', () => true, {}, true);
            await client.subscribe('test.unsubscribe.test2', () => true, {}, true);

            await client.unsubscribe('test.unsubscribe.test1');

            assert(client.consumer._subjectRegistry.get('^test\\.unsubscribe$').size === 1);

            await client.dispose();
        });

        // TODO check if this is fixed in v6.27.0
        it('Should successfully remove all transactions (sinek bug workaround test).', async () => {
            await client.subscribe('test.unsubscribe.all', () => true, {}, true);
            assert(client.consumer._subjectRegistry.get('^test\\.unsubscribe$').size === 1);

            await client.unsubscribe('test.unsubscribe.all');
            assert.strictEqual(client.consumer._subjectRegistry.has('^test\\.unsubscribe'), false);
        });

    });

    describe('Producing.', () => {
        let client;

        beforeEach(() => client = eventClientFactory({
            consumerGroupId: `test-${Date.now()}` // Consumer group id randomization fixes problem with timeouts on rebalancing on kafka
        }));

        afterEach(async () => {
            client && await client.dispose(); client = null;
        });

        after(() => {
            const timers = process._getActiveHandles().filter((handle) => {
                return Object.prototype.toString.call(handle) === '[object Timer]';
            });
            console.log(timers);
        });

        it('Should create topics before publishing to them.', async () => {
            await client.init();

            assert(client.producer.knownTopics.length === 0);

            await client.publish(`test.producing.create.topic.${Date.now()}`, 'ping');
            assert(client.producer.knownTopics.length === 1);
        });

        it('Should allow to publish messages to a rabbitmq style routing key.', async () => {
            const msg = `ping ${Date.now()}`;
            const receivedMessages = [];

            const randStr = Math.random().toString(36).substring(8);
            const topic = `test.rabbitproducing-${randStr}`;

            await client.init();

            /**
             * Creating the topic first manually so that the combination of
             * the following Consumer configurations works:
             *
             *   - 'topic.metadata.refresh.interval.ms': 1000
             *   -  'auto.offset.reset': 'latest',
             *
             * If we do not do this we will create a race condition
             * between metadata fetching (and actual reading from the topic
             * that is created by #publish) and publishing to the topic.
             * If we publish before we got the correct metadata 'latest'
             * will not fetch the message because it was there before we started
             * to read from the topic.
             */
            await client.consumer.createTopic(topic, 15000);
            await client.subscribe(`${topic}#`, (message) => {
                receivedMessages.push(message);
                return true;
            }, {}, true);

            await sleep(5000); // Time for rebalance, otherwise race condition between latest and earlist will hit again
            await client.publish(`${topic}.ping`, msg, null, {}, true);

            let ok = await retry(() => {

                if (receivedMessages.includes(msg))
                    return Promise.resolve(true);
                else
                    return Promise.reject(new Error('Message not yet received'));

            }, {'max_tries': 140, interval:500}); // Long wait interval, kafka rebalancing takes some time

            assert(ok);
        });

        it('Should allow to publish to kafka topics w/o converting the topic.', async () => {
            const topic = 'test.producing.ping';

            const msg = `ping ${Date.now()}`;
            const receivedMessages = [];

            await client.init();

            /**
             * Creating the topic first manually so that the combination of
             * the following Consumer configurations works:
             *
             *   - 'topic.metadata.refresh.interval.ms': 1000
             *   -  'auto.offset.reset': 'latest',
             *
             * If we do not do this we will create a race condition
             * between metadata fetching (and actual reading from the topic
             * that is created by #publish) and publishing to the topic.
             * If we publish before we got the correct metadata 'latest'
             * will not fetch the message because it was there before we started
             * to read from the topic.
             */
            await client.consumer.createTopic(topic, 15000);
            await client.subscribe(topic, (message) => {
                receivedMessages.push(message);
            });

            await sleep(5000); // Time for rebalance, otherwise race condition between latest and earlist will hit again
            await client.publish(topic, msg, null, {});

            let ok = await retry(() => {

                if (receivedMessages.includes(msg))
                    return Promise.resolve(true);
                else
                    return Promise.reject(new Error('Message not yet received'));

            }, {'max_tries': 140, interval: 500}); // Long wait interval, kafka rebalancing takes some time

            assert(ok);
        });

        it('Messages should allow custom context to be applied to the mesasge.', async () => {
            const topic = 'test.producing';

            const msg = `ping ${Date.now()}`;
            const ctx = {chuck: 'testa'};

            const receivedMessages = new Map();

            await client.init();

            await client.consumer.createTopic(topic, 15000);
            await client.subscribe(topic, (message, context) => receivedMessages.set(message, context));

            await sleep(5000); // Time for rebalance, otherwise race condition between latest and earlist will hit again
            await client.publish('test.producing', msg, ctx);

            let receivedCtx = await retry(() => {

                if (receivedMessages.has(msg) && receivedMessages)
                    return Promise.resolve(receivedMessages.get(msg));
                else
                    return Promise.reject(new Error('Message not yet received'));

            }, {'max_tries': 140, interval: 500}); // Long wait interval, kafka rebalancing takes some time

            assert(receivedCtx.hasOwnProperty('chuck'));
        });

    });

    describe('Methods', () => {

        describe('#dispose', () => {
            it('Should remove all subscriptions from the client\'s subject registry.', async () => {

                const client = eventClientFactory();
                await client.subscribe('test.disposetest', noopFn);

                assert(client.consumer._subjectRegistry.size >= 1);

                let r = await client.dispose();

                assert.strictEqual(r, true);
                assert.strictEqual(client.consumer._subjectRegistry.size, 0);
            });
        });

        describe('#hasSubscription', () => {
            let client;

            beforeEach(() => client = eventClientFactory());

            afterEach(async () => {
                client && await client.dispose();
                client = null;
            });

            it('Should indicate if a kafka-esque topic was subscribed.', async () => {
                await client.subscribe('test.hassubscription.sub1', noopFn);

                assert.equal(client.hasSubscription('test.hassubscription.sub1'), true);
                assert.equal(client.hasSubscription('test.baz'), false);

                assert.throws(() => client.hasSubscription([]));
            });

            it('Should indicate if a rabbitmq-style subject was subscribed.', async () => {
                await client.subscribe('test.hassubscription.sub1', noopFn, {}, true);

                assert.equal(client.hasSubscription('test.hassubscription.sub1', true), true);
                assert.equal(client.hasSubscription('test.baz'), false);

                assert.throws(() => client.hasSubscription([]));
            });

        });

    });


});
