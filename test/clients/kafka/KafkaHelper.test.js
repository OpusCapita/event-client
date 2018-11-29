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
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta'), 'alpha.beta');
        });

        it('Should work for 3+ element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma'), 'alpha.beta');
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma.delta'), 'alpha.beta');
        });

        it('Should work for 3+ element routingKeys with # pattern', () => {
            const key = 'alpha.#.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^alpha\\.\\w*\\b/');
            assert.ok(result.test(topic));
        });

        it('Should work for 3+ element routingKeys with * pattern', () => {
            const key = 'alpha.b*.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^alpha\\.b\\S*/');
            assert.ok(result.test(topic));
        });

        it('Should work for 3+ element routingKeys with mixed pattern', () => {
            const key = '*.#.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^\\S*\\.\\w*\\b/');
            assert.ok(result.test(topic));
        });

        it('Should only accept strings as routingKey.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey([]));
        });

        it('Should not accept empty strings.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey(''));
        });
    });

    describe('#convertRabbitWildcard', () => {
        const routingKey             = 'alpha.beta.gamma.delta';
        const routingKeyWithPattern1 = 'alpha.*.gamma.*';
        const routingKeyWithPattern2 = 'alpha.#.gamma.#';
        const routingKeyWithPattern3 = 'alpha.#.gam*.delta';

        it('Should exist', () => {
            assert(KafkaHelper.hasOwnProperty('convertRabbitWildcard'));
        });

        it('Should not change routingKeys that do not contain patterns.', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKey);
            assert.strictEqual(result, routingKey);
        });

        it('Should return an instance of RegExp when routingKey contains a pattern.', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern1);
            assert.ok(result instanceof RegExp);
        });

        it('Should replace all * with the non-whitespace matcher', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern1);
            assert.strictEqual(result.toString(), '/^alpha\\.\\S*\\.gamma\\.\\S*/');
            assert.ok(result.test(routingKey));
        });

        it('Should replace all # with word delimiter', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern2);
            assert.strictEqual(result.toString(), '/^alpha\\.\\w*\\b\\.gamma\\.\\w*\\b/');
            assert.ok(result.test(routingKey));
        });

        it('Should rewrite in mixed wildcard routingKeys, # and *', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern3);
            assert.strictEqual(result.toString(), '/^alpha\\.\\w*\\b\\.gam\\S*\\.delta/');
            assert.ok(result.test(routingKey));
        });

    });

});

