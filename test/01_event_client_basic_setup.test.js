/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert        = require('assert');
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
        logger: Logger.DummyLogger
    }, config));
};

describe('EventClient basic setup test.', () => {
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

        before(() => client = eventClientFactory());

        after(async () => {
            client && await client.dispose();
            client = null;
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
            client && await client.dispose();
            client = null;
        });

        it('Applies the given context to all instances.', () => {
            const ctx = {is: 'set'};

            client.contextify(ctx);

            assert.deepEqual(client.kafkaClient.config.context, ctx);
            assert.deepEqual(client.amqpClient.config.context, ctx);
        });
        
    });
});

