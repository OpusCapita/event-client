const AmqpClient     = require('./clients/amqp/');
const KafkaClient    = require('./clients/kafka/');
const KafkaHelper    = require('./clients/kafka/KafkaHelper');
const {NotImplError} = require('./err/');

const configService = require('@opuscapita/config');
const Logger        = require('ocbesbn-logger');

const extend   = require('extend');
const ON_DEATH = require('death'); // This is intentionally ugly

/**
 * @todo Implement config to switch publishing to kafka (defaults to rabbit)
 *
 * @todo Methods form 2x EventClient:
 *   - unsubscribe
 *
 *   - getMessage: Not supported by Kafka -> checked: Used by archive, blob
 *   - ackMessage: Not supported by Kafka -> checked: Used by archive, blob
 *   - nackMessage: Not supported by Kafka -> checked: Used by archive, blob
 *
 *   - queueExists: Kafka does not care, implement for Rabbit, check if this is used public interface -> Checked: Used by web-init, blob
 *   - deleteQueue: Kafka does not care, implement for Rabbit, check if this is used public interface -> Checked: Only used internally
 *   - exchangeExists: Exchanges do not exist in Kafka, check if this is used public interface -> Checked: Only used internally
 *   - hasSubscription: check if this is used on the public interface -> Checked: Only used internally
 *   - getQueueName: Check if this is used public -> Checked: Only used internally
 */
class EventClient {

    constructor(config = {})
    {
        const self = this;

        this._logger = config.logger || new Logger();

        this._config                  = extend(true, {}, EventClient.DefaultConfig, config);
        this._config.serviceName      = configService.serviceName || this.config.serviceName;
        this._config.consumerGroupId  = config.consumerGroupId || this.config.serviceName;

        this._amqpClient  = new AmqpClient(this.config);
        this._kafkaClient = new KafkaClient(this.config);

        ON_DEATH((signal, err) => {
            this.logger.info(this.klassName, '#onDeath: Got signal: ' + signal, ' and error: ', err);
        });

        /**
         * Return a proxified version of this to keep track of
         * property access to detect missing implementations.
         */
        return new Proxy(self, {
            get(target, key) {
                if (!Reflect.has(self, key)) {
                    self.logger.warn(self.klassName, `: Getter for undefined property ${key} called.`);
                } else {
                    return target[key];
                }
            }
        });
    }

    /** *** PUBLIC *** */

    get logger()
    {
        if (!this._logger) { this._logger = new Logger(); }
        return this._logger;
    }

    get amqpClient()  { return this._amqpClient; }
    get config()      { return this._config; }
    get kafkaClient() { return this._kafkaClient; }
    get klassName()   { return this.constructor.name || 'EventClient'; }

    /**
     * @public
     * @function contextify
     * @param {object} [context={}] - Overrides the current context
     * @return {boolean}
     */
    contextify(...args)
    {
        this.kafkaClient.contextify(...args);
        this.amqpClient.contextify(...args);

        return true;
    }

    async dispose()
    {
        return Promise.all([
            this.kafkaClient.dispose(),
            this.amqpClient.dispose()
        ]);
    }

    /**
     * Delegates to publish().
     *
     * TODO check for partition key
     * TODO First two elements of topic define the kafka topic, eg:
     *         topic: supplier.bank-account.created -> kafka topic: supplier.bank-account
     *
     * @async
     * @function emit
     * @param {string} topic - Full name of a topic.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     * @throws {Error}
     */
    async emit(...args)
    {
        this.logger.warn(this.klassName, '#emit: Deprecation warning. Using old interface to publish messages. Use publish().');
        return this.publish(...args);
    }

    async init()
    {
        await this.kafkaClient.init(this.config);
        await this.amqpClient.init(this.config);

        return true;

        // return Promise.all([this.kafkaClient.init(), this.amqpClient.init()]);
    }

    /**
     * Publish a message to the message broker.
     *
     * @async
     * @function publish
     * @param {string} routingKey - Full name of a topic.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise}
     * @fulfil null
     * @reject {Error}
     */
    async publish(routingKey, message, context = null, opts = {})
    {
        // throw new NotImplError(`${this.klassName}#publish: Not implemented.`, 'ENOTIMPL');

        return await this.amqpClient.emit(routingKey, message, context, opts);
    }

    /**
     * This method allows you to subscribe to one or more events. An event can either be an absolute name of a
     * topic to subscribe (e.g. my-service.status) or a pattern (e.g. my-servervice.#).
     *
     * @async
     * @function subscribe
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {function} callback - Optional function to be called when a message for a topic or a pattern arrives.
     * @param {SubscribeOpts} opts - Additional options to set for the subscription.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    async subscribe(topic, callback = null, opts = {})
    {
        return Promise.all([
            this._subscribeKafka(topic, callback, opts),
            this.amqpClient.subscribe(topic, callback, opts)
        ]);
    }

    /** *** PRIVATE *** */

    /**
     * Modify event-client v2x subscriptions to confirm to Kafka conventions.
     *
     * @todo rewrite RabbitMQ wildcard subscriptions to Kafka RegEx
     * @todo First two elements of topic define the kafka topic, eg:
     *         topic: supplier.bank-account.created -> kafka topic: supplier.bank-account
     *
     * @async
     * @function _subscribeKafka
     * @param {string} routingKey - EventClient v2x routingKey.
     * @param {function} callback - Optional function to be called when a message for a topic or a pattern arrives.
     * @param {SubscribeOpts} opts - Additional options to set for the subscription.
     * @returns {Promise}
     * @fulfil {null}
     * @reject {Error}
     */
    async _subscribeKafka(routingKey, callback = null, opts = {}) {
        const topic = KafkaHelper.getTopicFromRoutingKey(routingKey); // Convert routingKey to kafka topic
        this.logger.info(this.klassName, `#_subscribeKafka: Converted routing key ${routingKey} to topic ${topic}`);
        return this.kafkaClient.subscribe(topic, callback, opts);
    }

}

/**
* Static object representing a default configuration set.
*
* @property {object} serializer - Function to use for serializing messages in order to send them.
* @property {object} parser - Function to use for deserializing messages received.
* @property {string} serializerContentType - Content type of the serialized message added as a meta data field to each event emitted.
* @property {string} parserContentType - Content type for which events should be received and parsed using the configured parser.
* @property {string} consumerGroupId - The name of the consumerGroup the client uses for subscriptions. By default this is the name of the service as from [@opuscapita/config](https://github.com/OpusCapita/config/wiki#serviceName).
* @property {string} queueName - Name of the queue to connect to. By default this is the service name as of [@opuscapita/config](https://github.com/OpusCapita/config/wiki#serviceName).
* @property {string} exchangeName - The name of the exchnage to emit events to. By default this is the name of the service as from [@opuscapita/config](https://github.com/OpusCapita/config/wiki#serviceName).
* @property {object} logger - [Logger](https://github.com/OpusCapita/logger) object to be used for logging.
* @property {object} consul - Object for configuring consul related parameters.
* @property {string} consul.host - Hostname of a consul server.
* @property {string} consul.mqServiceName - Name of the endpoint for the message queue server in consul.
* @property {string} consul.mqUserKey - Consul configuration key for message queue authentication.
* @property {string} consul.mqPasswordKey - Consul configuration key for message queue authentication.
* @property {object} consulOverride - Configuraion object for manually overriding the message queue connection configuration.
* @property {string} consulOverride.host - Hostname of a message queue server.
* @property {number} consulOverride.port - Port of a message queue server.
* @property {string} consulOverride.username - User name for message queue authentication.
* @property {string} consulOverride.password - User password for message queue authentication.
* @property {object} context - Optional context object to automatically extend emitted messages.
*/
EventClient.DefaultConfig = {
    serializer: JSON.stringify,
    parser: JSON.parse,
    serializerContentType: 'application/json',
    parserContentType: 'application/json',
    consumerGroupId: null,
    queueName: null,
    exchangeName: null,
    logger: new Logger(),
    consul: {
        host: 'consul',
    },
    consulOverride: {
        host: null,
        port: null,
        username: null,
        password: null
    },
    context: {
    }
};

module.exports = EventClient;
