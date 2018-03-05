const EventClient = require('../index');
const Promise = require('bluebird');
const configService = require('ocbesbn-config');
const assert = require('assert');

const waitForService = (serviceName) => configService.getEndPoint(serviceName).catch(e => waitForService(serviceName));

describe('Main', () =>
{
    before('Init', async () =>
    {
        return configService.init();
    });

    before('Wait for services', async () =>
    {
        return waitForService('rabbitmq-amqp');
    });

    describe('#init()', () =>
    {
        const consulOverride = { };

        /**
        * Check rabbitMQ is ready
        */
        before('ACL connection', async () =>
        {
            const config = EventClient.DefaultConfig.consul;
            const props = await Promise.props({
                endpoint : configService.getEndPoint(config.mqServiceName),
                username : config.mqUserKey && configService.get(config.mqUserKey),
                password : config.mqPasswordKey && configService.get(config.mqPasswordKey)
            });

            consulOverride.host = props.endpoint.host;
            consulOverride.port = props.endpoint.port;
            consulOverride.username = props.username;
            consulOverride.password = props.password;
        });

        /**
        * SImple test
        */
        it('Simple test', (done) =>
        {
            const subscriberClient = new EventClient({ context : { nix : 1 } });
            const publisherClient = new EventClient();

            publisherClient.contextify({ truth : 42 });

            const routingKey = 'event-client.Test';
            const input = { message: 'Simple_Test' };

            subscriberClient.init().then(async () =>
            {
                return subscriberClient.subscribe(routingKey, (payload, context, key) =>
                {
                    delete context.timestamp;

                    assert.deepEqual(payload, input);
                    assert.deepEqual(context, { truth : 42, senderService : 'event-client' });
                    assert.equal(key, routingKey);

                    subscriberClient.unsubscribe(routingKey).then(() => done()).catch(done);
                })
                .then(() => publisherClient.emit(routingKey, input))
            })
            .catch(done);
        })

        /**
        * Simple connection with acknowledgement
        * Test cases with interest to acknowledge the queue
        */
        it('Simple_Connection_With_ACK', (done) =>
        {
            let iteration = 0;

            const subscriberClient = new EventClient();
            const publisherClient = new EventClient({ consulOverride });
            const routingKey = 'event-client.ACK';
            const input = { message: 'Test-ACK-Value' };

            subscriberClient.subscribe(routingKey, (payload, context, key) =>
            {
                iteration++;

                assert.deepEqual(payload, input);

                if(iteration == 1)
                {
                    return false;
                }
                else if(iteration == 2)
                {
                    throw new Error();
                }

                return true;
            })
            .then(() => publisherClient.emit(routingKey, input))
            .delay(500)
            .then(() =>
            {
                assert.equal(iteration, 3);
                return subscriberClient.unsubscribe(routingKey);
            })
            .then(() => done())
            .catch(done);
        });

        /**
        * Simple with multiple instances
        * Test cases with no interest to acknowledge the queue and mulitple instance subscribed
        * to the same queue, to check there is no duplicates
        */
        it('Simple_Connection_With_Multiple', (done) =>
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
                    throw new Error()
            };

            Promise.all([
                subscriberClient1.subscribe(routingKey, callback),
                subscriberClient2.subscribe(routingKey, callback)
            ])
            .then(() =>
            {
                return publisherClient.emit(routingKey, input);
            })
            .delay(500)
            .then(() =>
            {
                assert.equal(iteration, 3);

                return Promise.all([
                    subscriberClient1.unsubscribe(routingKey),
                    subscriberClient2.unsubscribe(routingKey)
                ]);
            })
            .then(() => done())
            .catch(done);
        });

        // pattern test
        it('Pattern_test', (done) =>
        {

            const publisherClient = new EventClient({ new : 1 });
            const subscriberClient = new EventClient({ new : 2 });
            const routingPattern = 'event-client.#';
            const routingKey = 'event-client.test';

            let iterator = 0;
            let output;
            const input = { message: 'Test-pattern' }

            subscriberClient.subscribe(routingPattern, (payload, context, key) =>
            {
                iterator++;
                output = payload;
            })
            .then(() => publisherClient.emit(routingKey, input))
            .delay(500)
            .then(() =>
            {
                assert.equal(iterator, 1);
                assert.deepEqual(output, input);

                done();
            })
            .catch(done);
        });

        it('Dispose_test 1', (done) =>
        {
            const subscriberClient = new EventClient();
            const publisherClient = new EventClient();
            const routingKey = 'event-client.dispose';
            const input = { message : 'Gone!' };

            const callback = (client, msg) => null;

            subscriberClient.subscribe(routingKey, callback)
                .then(() => subscriberClient.disposeSubscriber())
                .then(() => publisherClient.emit(routingKey, input))
                .then(() => publisherClient.disposePublisher())
                .then(() => publisherClient.disposePublisher())
                .then(() => done())
                .catch(done);
        });


        // dispose all approach
        it('Dispose_test 2', (done) =>
        {
            const subscriberClient = new EventClient({ queueName : 'test' });
            const publisherClient = new EventClient();
            const routingKey = 'event-client.dispose';
            const input = { message : 'Gone!' };

            const callback = (client, msg) => null;

            subscriberClient.subscribe(routingKey, callback)
            .then(() => subscriberClient.emit(routingKey, input))
            .then(() =>
            {
                return subscriberClient.unsubscribe(routingKey)
                    .then(() => subscriberClient.disposeSubscriber())
                    .then(() => subscriberClient.disposeSubscriber())
                    .then(() => subscriberClient.unsubscribe(routingKey))
            })
            .then(() => publisherClient.emit(routingKey, input))
            .then(() => publisherClient.disposePublisher())
            .then(() => publisherClient.disposePublisher())
            .then(() => done())
            .catch(done);
        });
    });
});
