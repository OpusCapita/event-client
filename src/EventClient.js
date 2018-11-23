const RabbitClient = require('./clients/rabbitmq/');
const KafkaClient  = require('./clients/kafka/');

const configService = require('@opuscapita/config');
const Logger        = require('ocbesbn-logger');

const extend   = require('extend');
const ON_DEATH = require('death'); // This is intentionally ugly

class EventClient {

    constructor(config = {})
    {
        this._logger = config.logger || new Logger();

        this._config                  = extend(true, {}, EventClient.DefaultConfig, config);
        this._config.serviceName      = configService.serviceName || this.config.serviceName;
        this._config.consumerGroupId  = config.consumerGroupId || this.config.serviceName;

        this._kafkaClient  = new RabbitClient(this.config);
        this._rabbitClient = new KafkaClient(this.config);

        ON_DEATH((signal, err) => {
            this.logger.info('EventClient#onDeath: Got signal: ' + signal, ' and error: ', err);
        });

        return true;
    }

    /** *** PUBLIC *** */

    get config()
    {
        return this._config;
    }

    get logger()
    {
        if (!this._logger) { this._logger = new Logger(); }

        return this._logger;
    }

    get kafkaClient()
    {
        return this._kafkaClient;
    }

    get rabbitClient()
    {
        return this._rabbitClient;
    }


    async dispose()
    {
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
        return this.publish(...args);
    }

    async init()
    {
        await this.kafkaClient.init(this.config);
        await this.rabbitClient.init(this.config);
        return true;

        // return Promise.all([this.kafkaClient.init(), this.rabbitClient.init()]);
    }

    async publish()
    {
        return true;
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
            this.rabbitClient.subscribe(topic, callback, opts)
        ]);
    }

    /** *** PRIVATE *** */

}

module.exports = EventClient;

