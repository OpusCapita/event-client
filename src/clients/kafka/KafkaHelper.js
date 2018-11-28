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

        const parts = routingKey.split('.');

        if (parts.length === 1 || parts.length === 2) {
            return routingKey;
        } else {
            return `${parts[0]}.${parts[1]}`;
        }
    }

    static convertRabbitWildcard() {
    }

};
