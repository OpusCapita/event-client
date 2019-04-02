/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert        = require('assert');
const retry         = require('bluebird-retry');
const configService = require('@opuscapita/config');
const extend        = require('extend');
const Logger        = require('ocbesbn-logger');

const {EventClient} = require('../src/');

const consulOverride = {
    kafkaHost: 'kafka1',
    kafkaPort: 9092
};

const eventClientFactory = (config) => {
    return new EventClient(extend(true, {
        serviceName: 'event-client',
        consumerGroupId: 'test',
        consulOverride,
        logger: Logger.DebugLogger
    }, config));
};

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

describe('EventClient', () => {

    before(async () => {
        return await configService.init({logger : Logger.DummyLogger});
    });

    after('Shutdown', async () => {
        await configService.dispose();
    });

    describe('#constructor', () => {
        let client;

        before(() => client = eventClientFactory());

        after(async () => {
            client && await client.dispose(); client = null;
        });

        it('Creates a new instance', () => {
            assert(client !== null);
            assert.equal(client.kafkaClient.klassName, 'KafkaClient');
            assert.equal(client.amqpClient.klassName, 'AmqpClient');
        });

        it('Should setup the service name via configService', () => {
            assert.equal(client.config.serviceName, 'event-client');
        });

        it('Should set the consumer group to the service name.', () => {
            assert.equal(client.config.consumerGroupId, 'test');
        });

        it('Should set the mqServiceName depending on the client implementation.', () => {
            assert.equal(client.kafkaClient.config.consul.mqServiceName, 'kafka');
            assert.equal(client.amqpClient.config.consul.mqServiceName, 'rabbitmq-amqp');
        });
    });

    describe('#contextify', () => {
        let client;

        before(() => client = eventClientFactory());

        after(async () => {
            client && await client.dispose(); client = null;
        });

        it('Applies the given context to all instances.', () => {
            const ctx = {is: 'set'};

            client.contextify(ctx);

            assert.deepEqual(client.kafkaClient.config.context, ctx);
            assert.deepEqual(client.amqpClient.config.context, ctx);
        });
    });

    describe('#publish and #subscribe', () => {

        describe('with RabbitMQ', () => {
            let client;

            beforeEach(async () => {
                client = eventClientFactory({
                    consumerGroupId: `test-${Date.now()}` // Consumer group id randomization fixes problem with timeouts on rebalancing on kafka
                });

                return await client.init();
            });

            afterEach(async () => {
                client && await client.dispose(); client = null;
            });

            // after(() => {
            //     const timers = process._getActiveHandles().filter((handle) => {
            //         return Object.prototype.toString.call(handle) === '[object Timer]';
            //     });
            //     console.log(timers);
            // });

            it('Should publish messages to a topic.', async () => {
                const msg = `ping ${Date.now()}`;
                const receivedMessages = [];

                await client.subscribe('event-client.test.producing', (message, headers, topic, routingKey) => {
                    receivedMessages.push(message);
                    return true;
                });

                await client.publish('event-client.test.producing', msg);

                let ok = await retry(() => {
                    if (receivedMessages.includes(msg))
                        return Promise.resolve(true);
                    else
                        return Promise.reject(new Error('Message not yet received'));
                }, {'max_tries': 80}); // Long wait interval, kafka rebalancing takes some time

                assert(ok);
            });
        });

        describe('with Kafka', () => {
            let client;

            beforeEach(async () => {
                client = eventClientFactory({
                    consumerGroupId: `test-${Date.now()}`, // Consumer group id randomization fixes problem with timeouts on rebalancing on kafka
                    sendWith: 'kafka'
                });

                return await client.init();
            });

            afterEach(async () => {
                client && await client.dispose(); client = null;
            });

            // after(() => {
            //     const timers = process._getActiveHandles().filter((handle) => {
            //         return Object.prototype.toString.call(handle) === '[object Timer]';
            //     });
            //     console.log(timers);
            // });

            it('Should publish messages to a topic.', async () => {
                const topic = 'event-client.test';

                const msg = `ping ${Date.now()}`;
                const receivedMessages = [];

                await client._kafkaClient.consumer.createTopic(topic, 15000);
                await client.subscribe('event-client.#', (message) => {
                    receivedMessages.push(message);
                    return true;
                });

                await sleep(5000); // Time for rebalance, otherwise race condition between latest and earlist will hit again
                await client.publish('event-client.test.producing', msg);

                let ok = await retry(() => {
                    if (receivedMessages.includes(msg))
                        return Promise.resolve(true);
                    else
                        return Promise.reject(new Error('Message not yet received'));
                }, {'max_tries': 50, interval: 500 }); // Long wait interval, kafka rebalancing takes some time

                assert(ok);
            });

            it('Should send messages that are not handled by the application to the DLQ.');
        });

    });

    describe('#unsubscribe', () => {
        it('Should unsubscribe from a topic without pattern');
        it('Should unsubscribe from a topic wit pattern');
    });

});

