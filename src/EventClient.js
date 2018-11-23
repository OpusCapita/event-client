const AmqpClient     = require('./clients/amqp/');
const KafkaClient    = require('./clients/kafka/');
const {NotImplError} = require('./err/');

const configService = require('@opuscapita/config');
const Logger        = require('ocbesbn-logger');

const extend   = require('extend');
const ON_DEATH = require('death'); // This is intentionally ugly

/**
 * @todo Implement config to switch publishing to kafka (defaults to rabbit)
 *
 * @todo Methods form 2x EventClient:
 *   - getMessage: Not supported by Kafka -> checked: Used by archive, blob
 *   - ackMessage: Not supported by Kafka -> checked: Used by archive, blob
 *   - nackMessage: Not supported by Kafka -> checked: Used by archive, blob
 *   - exchangeExists: Exchanges do not exist in Kafka, check if this is used public interface -> Checked: Only used internally
 *   - queueExists: Kafka does not care, implement for Rabbit, check if this is used public interface -> Checked: Used by web-init, blob
 *   - deleteQueue: Kafka does not care, implement for Rabbit, check if this is used public interface -> Checked: Only used internally
 *   - unsubscribe
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

    async publish()
    {
        throw new NotImplError(`${this.klassName}#dispose: Not implemented.`, 'ENOTIMPL');
    }

    /**
     * This method allows you to subscribe to one or more events. An event can either be an absolute name of a
     * topic to subscribe (e.g. my-service.status) or a pattern (e.g. my-servervice.#).
     *
     * TODO rewrite RabbitMQ wildcard subscriptions to Kafka RegEx
     * TODO First two elements of topic define the kafka topic, eg:
     *         topic: supplier.bank-account.created -> kafka topic: supplier.bank-account
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
            this.kafkaClient.subscribe(topic, callback, opts),
            this.amqpClient.subscribe(topic, callback, opts)
        ]);
    }

    /** *** PRIVATE *** */

}

module.exports = EventClient;
