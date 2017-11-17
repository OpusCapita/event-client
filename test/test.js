const EventClient = require('../src');
const server = require('ocbesbn-web-init');

const publisherClient = new EventClient();

describe('Main', () =>
{
    describe('#init()', () =>
    {
        /**
        * Check rabbitMQ is ready
        */
        before('ACL connection', (done) =>
        {
            done();
        });
        /**
        * Simple connection with no acknowledgement
        * Test cases with no interest to acknowledge the queue
        */
        // it('Simple_Connection_With_NOACK', (done) =>
        // {
        //     let iteration = 0;
        //     const routingKey = 'test.NoACK';
        //
        //     // const publisherClient = new EventClient();
        //     const subscriberClient = new EventClient({queueName: 'Simple_Connection_With_NOACK'});
        //
        //     subscriberClient.subscribe((msg) =>
        //     {
        //         iteration++;
        //
        //         if (iteration == 2)
        //         {
        //             subscriberClient.unsubscribe(routingKey)
        //             .then(() =>
        //             {
        //                 done();
        //             })
        //             .catch(done)
        //         }
        //     }, routingKey, true)
        //     .then(() =>
        //     {
        //         return publisherClient.emit(routingKey, {message: 'Test-NoACK-Value'});
        //     })
        //     .then(() =>
        //     {
        //         publisherClient.emit(routingKey, {message: 'Test-NoACK-Value-1'});
        //     });
        // });

        /**
        * Simple connection with acknowledgement
        * Test cases with interest to acknowledge the queue
        */
        it('Simple_Connection_With_ACK', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.ACK';

            // const publisherClient = new EventClient();
            const subscriberClient = new EventClient({queueName: 'Simple_Connection_With_ACK'});

            subscriberClient.subscribe((msg, rawMsg) =>
            {
                iteration++;

                if (iteration == 2)
                {
                    subscriberClient.unsubscribe(routingKey)
                    .then(() =>
                    {
                        done();
                    })
                    .catch(done)
                }
                else if (iteration == 1)
                {
                    return Promise.reject(new Error('Test to acknowledge with error'));
                }

                return Promise.resolve();
            }, routingKey)
            .then(() =>
            {
                return publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
            })
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value-1'});
            })
            .catch((err) =>
            {
                console.log(err);
            })
        });

        /**
        * Simple with multiple instances
        * Test cases with no interest to acknowledge the queue and mulitple instance subscribed
        * to the same queue, to check there is no duplicates
        */
        it('Simple_Connection_With_Multiple', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.Instances';

            // const publisherClient = new EventClient();
            const subscriberClient1 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES'});
            const subscriberClient2 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES'});

            const callback = (client, msg, rawMsg) =>
            {
                done();

                subscriberClient2.unsubscribe(routingKey);
                subscriberClient1.unsubscribe(routingKey);
            }

            Promise.all([
                subscriberClient1.subscribe(callback.bind(this, 'client1'), routingKey, true),
                subscriberClient2.subscribe(callback.bind(this, 'client2'), routingKey, true)
            ])
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
            });
        });

        /**
        * Simple with multiple instances
        * Test cases with interest to acknowledge the queue and mulitple instances subscribed
        * to the same queue, to check there is no duplicates
        */
        it('Simple_Connection_With_Multiple_ACK', (done) =>
        {
            let iteration = 0;
            const routingKey = 'test.Instances.ACK';

            // const publisherClient = new EventClient();
            const subscriberClient1 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES_NACK'});
            const subscriberClient2 = new EventClient({queueName: 'Simple_Connection_With_INSTANCES_NACK'});


            const callback = (client, msg, rawMsg) =>
            {
                iteration++;

                if (iteration > 1)
                {
                    subscriberClient2.unsubscribe(routingKey);
                    subscriberClient1.unsubscribe(routingKey);

                    done();

                    return Promise.resolve();
                }
                else
                {
                    return Promise.reject(new Error('Test to acknowledge with error in multiple instances'))
                }
            }

            Promise.all([
                subscriberClient1.subscribe(callback.bind(this, 'client1'), routingKey),
                subscriberClient2.subscribe(callback.bind(this, 'client2'), routingKey)
            ])
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
            })
            .catch((err) =>
            {
                console.log(err);
            })
        });

        /**
        * Shutdown and restart
        * Simulate the shutdown and restart of subscriptio, a server goes down while processing item
        */
        it('Shutdown_On_Subscription_And_restart', (done) =>
        {
            // const publisherClient = new EventClient();
            const subscriberClient = new EventClient({queueName: 'Simple_Connection_With_Shutdown_Restart'});
            const routingKey = 'test.shutdown';

            const subscribe = (callback) => {return subscriberClient.subscribe(callback, routingKey)};
            const publish = () => {return publisherClient.emit(routingKey, {message: 'Test-Restart-Value'})};

            var app = server.init({
                routes: {
                    addRoutes : false
                },
                server: {
                    port: 3000,
                    mode: server.Server.Mode.Dev,
                    events: {
                        onStart: () => {
                            subscribe((msg) => {
                                console.log('======>FIRST', msg);
                            })
                            .then(() =>
                            {
                                publish().then(() => {
                                    console.log('======>Published');
                                    server.end();
                                });
                            })
                        },
                        onEnd: () =>  { startNewServer(); }
                    },
                    webpack: {
                        useWebpack : false
                    },
                    enableBouncer: false
                }
            });

            const startNewServer = function()
            {
                var newApp = server.init({
                    routes: {
                        addRoutes : false
                    },
                    server: {
                        port: 3001,
                        mode: server.Server.Mode.Dev,
                        events: {
                            onStart: () => { subscribe((msg, raw) => {
                                console.log('Recieved', msg);
                                done();
                                return Promise.resolve();
                            }) },
                            onEnd: () =>  {  }
                        },
                        webpack: {
                            useWebpack : false
                        },
                        enableBouncer: false
                    }
                });
            }
        });

        // pattern test
        it('Pattern_test', (done) =>
        {
            const queueName = "Pattern_test";
            const subscriberClient = new EventClient({queueName: queueName});
            const routingPattern = 'pattern.#';
            const routingKey = 'pattern.test';

            subscriberClient.subscribe((msg) =>
            {
                done();
            }, routingPattern, true)
            .then(() =>
            {
                return subscriberClient.subscribe((msg) =>
                {
                    done();
                }, routingKey, true);
            })
            .then(() =>
            {
                publisherClient.emit(routingKey, {message: 'Test-pattern'});
            })
        });

        // dispose all approach
        it('Dispose_test', (done) =>
        {
            // const publisherClient = new EventClient();
            const queueName = "Simple_Connection_To_Test_Dispose";
            const subscriberClient = new EventClient({queueName: queueName});
            const routingKey = 'test.dispose';

            const callback = (client, msg) =>
            {
                console.log('Recieved message:', client, msg);
                done();
            }

            subscriberClient.subscribe(callback.bind(this, 'Client0'), routingKey, true)
            .then(() =>
            {
                return subscriberClient.disposeSubscriber();
            })
            .then(() =>
            {
                const subscriberClient1 = new EventClient({queueName: queueName});
                subscriberClient1.subscribe(callback.bind(this, 'Client1'), routingKey, true);


                return publisherClient.emit(routingKey, {message: 'Test-ACK-Value'});
            })

        });

    });
});
