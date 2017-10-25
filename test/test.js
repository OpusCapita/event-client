const EventClient = require('../src');

describe('Main', () =>
{
    describe('#init()', () =>
    {
        /**
        * Simple connection with no acknowledgement
        */
        it('Simple_Connection_With_NOACK', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.NoACK';

            const publisherClient = new EventClient();
            const subscriberClient = new EventClient({queueName: 'Simple_Connection_With_NOACK'});

            subscriberClient.subscribe((msg) =>
            {
                if (++iteration > 1)
                {
                    subscriberClient.unSubscribe(routingKey)
                    .then(() =>
                    {
                        done();
                    })
                    .catch(done)
                }
            }, routingKey)
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-NoACK-Value'});
                publisherClient.emit(routingKey, {message: 'Test-NoACK-Value-1'});
            });
        });

        /**
        * Simple connection with acknowledgement
        */
        it('Simple_Connection_With_ACK', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.ACK';

            const publisherClient = new EventClient();
            const subscriberClient = new EventClient({queueName: 'Simple_Connection_With_ACK'});

            subscriberClient.subscribe((msg, rawMsg) =>
            {
                iteration++;

                if (iteration > 2)
                {
                    subscriberClient.unSubscribe(routingKey)
                    .then(() =>
                    {
                        done();
                    })
                    .catch(done)
                }
                else if (iteration == 1)
                {
                    return Promise.reject(new Error('test'));
                }
            }, routingKey)
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value-1'});
            });
        });

        /**
        * Simple with multiple instances
        */
        it('Simple_Connection_With_Multiple', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.Instances';

            const publisherClient = new EventClient();
            const subscriberClient1 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES'});
            const subscriberClient2 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES'});

            const callback = (client, msg, rawMsg) =>
            {
                done();

                subscriberClient2.unSubscribe(routingKey);
                subscriberClient1.unSubscribe(routingKey);
            }

            Promise.all([
                subscriberClient1.subscribe(callback.bind(this, 'client1'), routingKey),
                subscriberClient2.subscribe(callback.bind(this, 'client2'), routingKey)
            ])
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
            });
        });

    });
});
