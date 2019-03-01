module.exports = class KafkaHelper {
    constructor() {}

    /**
     * Convert a given subject (rabbitmq routing key) to a
     * kafka topic for publishing. Validate subject and shorten
     * it to the BNP topic schema.
     *
     * @function getTopicFromSubjectForPublish
     * @param {string} subject - The subject that is converted to a Kafka topic.
     * @return {string} Kafka topic according to BNP naming schema.
     * @throws {Error}
     */
    static getTopicFromSubjectForPublish(subject)
    {
        if (typeof subject !== 'string')
            throw new TypeError('String expected for subject.');

        if (!subject.length)
            throw new Error('Empty routing key not supported.');

        if (subject.indexOf('*') >= 0 || subject.indexOf('#') >= 0)
            throw new Error('Subjects can not contain wildcards.');

        const parts = subject.split('.');

        if (parts.length <= 1)
            throw new Error('Subject need to contain at least to levels, eg. servicename.domain');

        return `${parts[0]}.${parts[1]}`;
    }

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
    static getTopicFromRoutingKey(routingKey = '', convertWildcards = true) {
        if (typeof routingKey !== 'string')
            throw new TypeError('String expected for param routingKey.');

        if (!routingKey.length)
            throw new Error('Empty routing key not supported.');

        const parts = routingKey.split('.');

        let topic;
        if (parts.length === 1 || parts.length === 2) {
            // Use routingKey if it only cotains two levels
            topic = routingKey;
        } else {
            // Use only first two levels of routing key
            topic = `${parts[0]}.${parts[1]}`;
        }

        const result = convertWildcards ? KafkaHelper.convertRabbitWildcard(topic) : topic;

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
     * Attention: The converted RegExp is handled down to librdkafka that does only support
     *            POSIX regex. So no lookahead for example.
     *
     * @function convertRabbitWildcard
     * @param {string|RegExp} routingKey - The RabbitMQ routingKey or a regular expression
     * @return {string} The kafka topic matching the routing key
     * @throw {TypeError}
     */
    static convertRabbitWildcard(routingKey) {
        let result;

        if (typeof routingKey === 'string') {
            if (routingKey[0] === '^')
                result = new RegExp(routingKey);
            else {
                result = routingKey.replace(/\./g, '\\.');
                result = result.replace(/\*/g, '\\w*\\b');
                result = result.replace(/\#/g, '\\S*');

                result = new RegExp('^' + result); // Always convert to regex so we do not subscribe to DLQs (topics starting with dlq__)
            }
        } else if (routingKey instanceof RegExp)
            result = routingKey;
        else
            throw new TypeError('The provided routingKey can not be converted to RegExp');

        return result;
    }

    /**
     * Escape a regular string for further use in `new RegExp(...)`
     *
     * @param {string} string
     * @return {string}
     */
    static escapeRegExp(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); // $& means the whole matched string
    }

};
