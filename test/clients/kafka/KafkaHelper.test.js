/* global after:true, before:true beforeEach:true afterEach:true describe:true, it:true */
/* eslint object-curly-spacing: 0 */
/* eslint key-spacing: 0 */

const assert = require('assert');

const KafkaHelper = require('../../../src/clients/kafka/KafkaHelper');

describe('KafkaHelper', () => {

    it('Should exist', () => {
        const k = new KafkaHelper();
        assert.equal(k.constructor.name, 'KafkaHelper');
    });

    describe('#getTopicFromRoutingKey', () => {

        it('Should exist', () => {
            assert(KafkaHelper.hasOwnProperty('getTopicFromRoutingKey'));
        });

        it('Should work for one element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha').source, '^alpha');
        });

        it('Should work for two element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta').source, '^alpha\\.beta');
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta').source, '^alpha\\.beta');
        });

        it('Should work for 3+ element routingKeys', () => {
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma').source, '^alpha\\.beta');
            assert.equal(KafkaHelper.getTopicFromRoutingKey('alpha.beta.gamma.delta').source, '^alpha\\.beta');
        });

        it('Should work for 3+ element routingKeys with # pattern', () => {
            const key = 'alpha.#.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^alpha\\.\\S*/');
            assert.ok(result.test(topic));
        });

        it('Should work for 3+ element routingKeys with * pattern', () => {
            const key = 'alpha.b*.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^alpha\\.b\\w*\\b/');
            assert.ok(result.test(topic));
        });

        it('Should work for 3+ element routingKeys with mixed pattern', () => {
            const key = '*.#.gamma';
            const topic = 'alpha.beta';

            const result = KafkaHelper.getTopicFromRoutingKey(key);
            assert.strictEqual(result.toString(), '/^\\w*\\b\\.\\S*/');
            assert.ok(result.test(topic));
        });

        it('Should only accept strings as routingKey.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey([]));
        });

        it('Should not accept empty strings.', () => {
            assert.throws(() => KafkaHelper.getTopicFromRoutingKey(''));
        });
    });

    describe('#getTopicFromSubjectForPublish', () => {

        const errSubjects = [
            '',
            'alpha',
            'alpha.beta.*',
            'alpha.beta.gam#'
        ];

        const okSubjects = {
            'alpha.beta': 'alpha.beta',
            'alpha.beta.gamma' : 'alpha.beta',
            'alpha.beta.gamma.delta' : 'alpha.beta',
        };

        it('Should not accept invalid subjects', async () => {
            let failCount = 0;

            for (const subject of errSubjects) {

                try {
                    KafkaHelper.getTopicFromSubjectForPublish(subject);
                } catch (e) {
                    failCount++;
                }
            }

            assert.strictEqual(failCount, 4);
        });

        it('Should accept subjects of any level.', () => {
            for (const [subject, topic] of Object.entries(okSubjects))
                assert.strictEqual(KafkaHelper.getTopicFromSubjectForPublish(subject), topic);
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

        it('Should always convert to regex even if routingKey does not contain a pattern so we dont subscribe to DLQs.', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKey);
            assert.strictEqual(result.source, '^alpha\\.beta\\.gamma\\.delta');
        });

        it('Should return an instance of RegExp when routingKey contains a pattern.', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern1);
            assert.ok(result instanceof RegExp);
        });

        it('Should replace all * with the non-whitespace matcher', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern1);
            assert.strictEqual(result.toString(), '/^alpha\\.\\w*\\b\\.gamma\\.\\w*\\b/');
            assert.ok(result.test(routingKey));
        });

        it('Should replace all # with word delimiter', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern2);
            assert.strictEqual(result.toString(), '/^alpha\\.\\S*\\.gamma\\.\\S*/');
            assert.ok(result.test(routingKey));
        });

        it('Should rewrite in mixed wildcard routingKeys, # and *', () => {
            const result = KafkaHelper.convertRabbitWildcard(routingKeyWithPattern3);
            assert.strictEqual(result.toString(), '/^alpha\\.\\S*\\.gam\\w*\\b\\.delta/');
            assert.ok(result.test(routingKey));
        });

    });

});

