module.exports = class KafkaHelper {
    constructor() {}

    /**
     * Takes a RabbitMQ routingKey/topic and returns the kafka topic.
     *
     * Convention is to use the first two parts of a RabbitMQ routingKey
     * as the name for a topic to create and add the routingKey as subject
     * to every message that is emitted on the kafka topic (after the switch).
     *
     * @function getTopicFromRoutingKey
     * @param {string} routingKey - The RabbitMQ routingKey
     * @returns {string} The kafka topic matching the routing key
     * @throws {error}
     */
    static getTopicFromRoutingKey(routingKey = '') {
        if (typeof routingKey !== 'string') { throw new TypeError('String expected for param routingKey.'); }
        if (!routingKey.length) { throw new Error('Empty routing key not supported.'); }

        let result;
        const parts = routingKey.split('.');

        if (parts.length === 1 || parts.length === 2) {
            result = routingKey;
        } else {
            result = `${parts[0]}.${parts[1]}`;
        }

        result = KafkaHelper.convertRabbitWildcard(result);

        return result;
    }

    /**
     * Takes a RabbitMQ wildcard routingKey and converts it to
     * a kafka topic.
     *
     * From the RabbitMQ documentation:
     *   * (star) can substitute for exactly one word
     *   # (hash) can substitute for zero or more words
     *
     * @function convertRabbitWildcard
     * @param {string} routingKey - The RabbitMQ routingKey
     * @returns {string} The kafka topic matching the routing key
     */
    static convertRabbitWildcard(routingKey) {
        const hasWildcard = routingKey.indexOf('*') >= 0 || routingKey.indexOf('#') >= 0;

        let result;

        if (hasWildcard) {
            result = routingKey.replace(/\./g, '\\.');
            result = result.replace(/\*/g, '\\w*\\b');
            result = result.replace(/\#/g, '\\S*');
            result = '^' + result;
            result = new RegExp(result);
        } else {
            result = routingKey;
        }

        return result;
    }

};
