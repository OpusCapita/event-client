/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');
const extend = require('extend');

const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../src');
const {ConsumerError} = require('../src/err');
const subscribedTopics = require('../src/TopicSubscription');

// const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

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

describe('EventClient multi instance tests', () => {

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

        before(() => {
            c1 = eventClientFactory();
            c2 = eventClientFactory();
        });

        after(async () => {
            c1 && await c1.dispose(); c1 = null;
            c2 && await c2.dispose(); c2 = null;
        });

        it('Should subscribe to a topic based on a pattern.', async () => {
            const result1 = await c1.subscribe('^pattern1.test', noopFn);
            const result2 = await c2.subscribe('^pattern2.test', noopFn);

            assert.equal(result1, true);
            assert.equal(result2, true);
        });

        it('Should fail on double subscription from another EventClient instance to the same topic.', async () => {
            let ok = false;
            try {
                await c1.subscribe('dup.sub.multi.test', noopFn);
                await c2.subscribe('dup.sub.multi.test', noopFn);
            } catch (e) {
                ok = e instanceof ConsumerError && e.errno === 409;
            }

            assert.equal(ok, true);
        });

        it('Should unsubscribe in the correct EventClient instance.', async () => {
            await c1.subscribe('instance1.unsubscribe.test', noopFn);
            await c2.subscribe('instance2.unsubscribe.test', noopFn);

            assert.equal(c2._consumer._subscription().includes('instance2.unsubscribe.test'), true);
            assert(subscribedTopics.has('instance2.unsubscribe.test'));

            await c2.unsubscribe('instance2.unsubscribe.test');
            assert.equal(c2._consumer._subscription().includes('instance2.unsubscribe.test'), false);
            assert.equal(subscribedTopics.has('instance2.unsubscribe.test'), false);
            assert.equal(subscribedTopics.has('instance1.unsubscribe.test'), true);
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

});
