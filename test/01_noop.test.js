/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert        = require('assert');
const configService = require('@opuscapita/config');
const extend        = require('extend');
const Logger        = require('ocbesbn-logger');

const {EventClient} = require('../src');

const consulOverride = {
    host:  'kafka1',
    port: 9092
};

const eventClientFactory = (config) => {
    return new EventClient(extend(true, {
        serviceName: 'event-client',
        consumerGroupId: 'test',
        consulOverride,
        logger: Logger.DummyLogger
    }, config));
};

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
    });
});

