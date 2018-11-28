/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');

const KafkaHelper = require('../../../src/clients/kafka/KafkaHelper');

describe.only('KafkaHelper', () => {

    it('Should exist', () => {
        const k = new KafkaHelper();
        assert.equal(k.constructor.name, 'KafkaHelper');
    });

    describe('#getTopicFromRoutingKey', () => {

        it('Should exist', () => {
            assert(KafkaHelper.hasOwnProperty('getTopicFromRoutingKey'));
        });

        it('Should work for one element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha'), 'alpha');
        });

        it('Should work for two element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta'), 'alpha.beta');
        });

        it('Should work for 3+ element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma'), 'alpha.beta');
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma.delta'), 'alpha.beta');
        });

        it('Should only accept strings as routingKey.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey([]));
        });

        it('Should not accept empty strings.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey(''));
        });
    });

    describe('#convertRabbitWildcard', () => {
        it('Should exist', () => {
            assert(KafkaHelper.hasOwnProperty('convertRabbitWildcard'));
        });
    });

});

