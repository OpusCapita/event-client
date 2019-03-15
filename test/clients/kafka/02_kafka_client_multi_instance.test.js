/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const extend = require('extend');
const retry = require('bluebird-retry');

const configService = require('@opuscapita/config');
const Logger        = require('ocbesbn-logger');

const KafkaClient      = require('../../../src/clients/kafka/');
const KafkaHelper      = require('../../../src/clients/kafka/KafkaHelper.js');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

const consulOverride = {
    host:  'kafka1',
    port: 9092
};

const eventClientFactory = (config) => {
    return new KafkaClient(extend(true, {
        serviceName: 'event-client-2000',
        consumerGroupId: 'test',
        consulOverride,
        logger: Logger.DummyLogger
    }, config));
};

const noopFn = () => {};

describe('KafkaClient multi instance tests', () => {

    before(async () =>
    {
        return await configService.init({ logger : Logger.DummyLogger });
    });

    after('Shutdown', async () =>
    {
        await configService.dispose();
    });

    describe('Simple consuming', () => {
        let c1, c2;

        beforeEach(async () => {
            c1 = eventClientFactory();
            await c1.init();

            c2 = eventClientFactory();
            await c2.init();
        });

        afterEach(async () => {
            c1 && await c1.dispose(); c1 = null;
            c2 && await c2.dispose(); c2 = null;
        });

        it('Should subscribe to a topic based on a pattern.', async () => {
            const result1 = await c1.subscribe('^pattern1.test', noopFn);
            const result2 = await c2.subscribe('^pattern2.test', noopFn);

            assert.equal(result1, true);
            assert.equal(result2, true);
        });

        // it('Should fail on double subscription from another KafkaClient instance to the same topic.', async () => {
        //     let ok = false;
        //     try {
        //         await c1.subscribe('dup.sub.multi.test', noopFn);
        //         await c2.subscribe('dup.sub.multi.test', noopFn);
        //     } catch (e) {
        //         ok = e instanceof ConsumerError && e.errno === 409;
        //     }

        //     assert.equal(ok, true);
        // });

        it('Should unsubscribe in the correct KafkaClient instance.', async () => {
            // This test is not very useful anymore since the global topic registry is gone.

            const subject1 = 'instance1.unsubscribe.test';
            const subject2 = 'instance2.unsubscribe.test';

            await c1.subscribe(subject1, noopFn, {}, true);
            await c2.subscribe(subject2, noopFn, {}, true);

            assert.equal(c2.consumer._subjectRegistry.has(KafkaHelper.getTopicFromRoutingKey(subject2).source), true);

            await c2.unsubscribe('instance2.unsubscribe.test');

            assert.equal(c1.consumer._subjectRegistry.size, 1);
            assert.equal(c2.consumer._subjectRegistry.size, 0);
            assert.equal(c2.consumer._subjectRegistry.has(KafkaHelper.getTopicFromRoutingKey(subject2).source), false);
        });

    });

    describe('Consumer groups', () => {
        let c1, c2, c3;

        beforeEach(() => {
            c1 = eventClientFactory({consumerGroupId: 'test-alpha'});
            c2 = eventClientFactory({consumerGroupId: 'test-beta'});
            c3 = eventClientFactory({consumerGroupId: 'test-beta'});
        });

        afterEach(async () => {
            c1 && await c1.dispose(); c1 = null;
            c2 && await c2.dispose(); c2 = null;
            c3 && await c3.dispose(); c3 = null;
        });

        it('Should receive messages once per consumer group.');
    });

    describe('Dead letter queueing behavior', () => {
        let c1, c2;

        beforeEach(() => {
            c1 = eventClientFactory({consumerGroupId: 'test-alpha'});
            c2 = eventClientFactory({consumerGroupId: 'test-beta'});
        });

        afterEach(async () => {
            c1 && await c1.dispose(); c1 = null;
            c2 && await c2.dispose(); c2 = null;
        });

        // FIXME This test does not test what it's descriptions says
        // it('Should move message to the DLQ after 3 attempts on application callback returning a falsy values.', () => {
        //     return new Promise(async (resolve) => {
        //         let rxCnt = 0;
        //         let txCnt = 0;

        //         const sendFn = async () => {
        //             await c2.publish('test.dlq', txCnt, {'txCnt': txCnt});
        //             txCnt++;
        //             if (txCnt < 5) {
        //                 setTimeout(sendFn, 200);
        //             }
        //         };

        //         await c1.subscribe('test.dlq', () => {
        //             rxCnt++;

        //             if (rxCnt >= 5) {
        //                 resolve();
        //             } else {
        //                 return false;
        //             }
        //         });

        //         sendFn();
        //     });
        // });

        it('Should send messages to the dead letter queue.', () => {
            return new Promise(async (resolve, reject) => {
                let txCnt = 0;
                let dlqReceived = false;

                const payload = Math.random().toString(36).substring(7);

                await c1.init();
                await c2.init();

                await c1.subscribe('test.dlqRx', () => {
                    return false;
                });

                await c2.subscribe('dlq__test.dlqRx', ({randMsg}) => {
                    console.log('::DEBUG::', 'Message received on dlq__test.dlqRx', randMsg);
                    if (randMsg === payload) {
                        dlqReceived = true;
                    }

                    return true;
                });

                await sleep(5000);

                await c2.publish('test.dlqRx', {randMsg: payload}, {'txCnt': txCnt});

                retry(() => {
                    if (dlqReceived)
                        resolve();
                    else
                        throw new Error();
                }, {'max_tries': 80, interval: 500 }).catch(reject);
            });
        });

    });

});
