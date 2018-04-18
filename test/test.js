const EventClient = require('../index');
const Promise = require('bluebird');
const configService = require('@opuscapita/config');
const assert = require('assert');

const waitForService = (serviceName) => configService.getEndPoint(serviceName).catch(e => waitForService(serviceName));
const sleep = (timeout) => new Promise((resolve, reject) => setTimeout(resolve, timeout));

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

        it('Multiple (different) instances', async () =>
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
        });

        it('Pattern test', async () =>
        {
            const publisherClient = new EventClient({ new : 1 });
            const subscriberClient = new EventClient({ new : 2 });
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

            assert.equal(iterator, 1);
            assert.deepEqual(output, input);
        });

        it('Dispose test 1', async () =>
        {
            const subscriberClient = new EventClient();
            const publisherClient = new EventClient();
            const routingKey = 'event-client.dispose';
            const input = { message : 'Gone!' };

            const callback = (payload) => assert.deepEqual(payload, input);

            await subscriberClient.subscribe(routingKey, callback);
            await subscriberClient.disposeSubscriber();
            await publisherClient.emit(routingKey, input);
            await publisherClient.disposePublisher();
            await publisherClient.disposePublisher();
            await subscriberClient.subscribe(routingKey, callback);
        });

        it('Dispose test 2', async () =>
        {
            const subscriberClient = new EventClient({ queueName : 'test' });
            const publisherClient = new EventClient();
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
});
