const ON_DEATH = require('death'); // This is intentionally ugly
const extend   = require('extend');

const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');

const Consumer = require('./Consumer');
const Producer = require('./Producer');

const KafkaHelper = require('./KafkaHelper');

/**
 * Class for simplifying access to kafka brokers. Each instance of this class
 * is capable of receiving and emitting events.
 */
class KafkaClient
{
    /**
     * @param {object} [config={}] - For a list of possible configuration values see [DefaultConfig]{@link KafkaClient.DefaultConfig}.
     */
    constructor(config = {})
    {
        this._logger = config.logger;

        this.config                  = extend(true, {}, KafkaClient.DefaultConfig, config);
        this.config.serviceName      = this.config.serviceName || configService.serviceName;
        this.config.consumerGroupId  = config.consumerGroupId || this.config.serviceName;

        this.connectionConfig = null;

        this._consumer = null;
        this._producer = null;

        this.OFF_DEATH = ON_DEATH((signal, err) => {
            this.logger.info('KafkaClient#onDeath: Got signal: ' + signal, ' and error: ', err);
            this.dispose();
        });
    }

    /** *** GETTER *** */

    get consumer()  { return this._consumer; }
    get producer()  { return this._producer; }
    get klassName() { return this.constructor.name || 'KafkaClient'; }

    get logger() {
        if (!this._logger)
            this._logger = new Logger();

        return this._logger;
    }

    /** *** PUBLIC *** */

    /**
     * Returns the health check results from the consumer and producer. Each of this
     * will align to the following structure:
     *
     * {
     *   status: <Number>,
     *   messages: [<String>, ... ]
     * }
     *
     * Meaning of the status code is as follows:
     *
     *  - DIS_ANALYTICS: -4, you have not enabled analytics
     *  - NO_ANALYTICS:  -3, no analytics result are available yet
     *  - UNKNOWN:       -2, status is unknown, internal error occured
     *  - UNCONNECTED:   -1, client is not connected yet
     *  - HEALTHY:        0, client is healthy
     *  - RISK:           1, client might be healthy, but something does not seem 100% right
     *  - WARNING:        2, client might be in trouble soon
     *  - CRITICAL:       3, something is wrong
     *
     *
     * Caution: Analytics takes some time to "warm up". It will not be immediatly accesible.
     *
     * @public
     * @async
     * @function checkHealth
     * @returns {Promise<object>}
     */
    async checkHealth()
    {
        let result = {
            consumer: null,
            producer: null
        };

        try {
            result.consumer = this.consumer && await this.consumer.checkHealth();
        } catch (e) {
            this.logger && this.logger.error('KafkaClient#checkHealth: Checking consumer health throwed an exception. ', e);
            result.consumer = null;
        }

        try {
            result.producer = this.producer && await this.producer.checkHealth();
        } catch (e) {
            this.logger && this.logger.error('KafkaClient#checkHealth: Checking producers health throwed an exception. ', e);
            result.producer = null;
        }

        return result;
    }

    /**
     * Allows adding a default context to every event emitted.
     * You may also want to construct an instance of this class by passing the context
     * parameter to the constructor. For further information have a look at {@link KafkaClient.DefaultConfig}.
     *
     * @param {object} [context={}] - Overrides the current context
     */
    contextify(context = {})
    {
        this.config.context = context;

        if (this.producer)
            this.producer.context = context;
    }

    /**
     * Closes all consumers and producers.
     *
     * @returns {Promise}
     * @fulfil {boolean} Indicates if all called dispose methods returned true
     */
    async dispose()
    {
        this.logger && this.logger.info('KafkaClient#dispose: Disposing ...');

        let disposePromises = [];

        if (this._consumer)
            disposePromises.push(this._consumer.dispose());

        if (this._producer)
            disposePromises.push(this._producer.dispose());

        const result = await Promise.all(disposePromises);

        this.OFF_DEATH();

        return !result.some((v) => v !== true); // Check if all values equal `true`
    }

    /**
     * Checks whenever the passed *topic* or pattern already has an active subscription inside the
     * current instance of KafkaClient. The *topic* can either be a full name of a
     * channel or a pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {boolean} [convertTopic=false] - Indicates if the given topic needs to be converted because it is a RabbitMQ routing key.
     * @returns {boolean} Returns true if the *topic* is already registered; otherwise false.
     */
    hasSubscription(topic, convertTopic = false)
    {
        return this.consumer.hasSubscription(topic, convertTopic);
    }

    /**
     * Makes some basic initializations like exchange creation as they are automatically done by emitting the first event.
     * This is used to create the required environment without pushing anything tho the queue.
     *
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    async init()
    {
        this.logger.info(this.klassName, '#init: Initialisation of Kafka event-client instance called.');

        const result = await Promise.all([
            this._initConsumer(),
            this._initProducer()
        ]);

        return result;
    }

    /**
     * Publishes an event to a certain topic by passing a message and an optional context.
     *
     * The passed *topic* has to be a string and identify the raised event as exact as possible.
     * The passed *message* can consist of all data types that can be serialized into a string.
     * The optional *context* paraemter adds additional meta data to the event. It has to be an event
     * and will extend a possibly existing global context defined by the config object passed
     * to the constructor (see {@link KafkaClient.DefaultConfig}).
     *
     * @param {string} topic - Domain model part of the subject. (Subject is usually the original rabbit-mq routingkey)
     * @param {string} subject - Subject will be attached to the message and is used to identify callbacks of subscribers.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @param {boolean} convertTopic - Indicates if the given topic needs to be converted because it is a RabbitMQ routing key.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     * @fulfil {object} Result of message producing. Should be {key, partition}
     * @reject {Error}
     */
    async publish(topic, message, context = null, opts = {}, convertTopic = false)
    {
        const subject = topic;

        if (convertTopic)
            topic = KafkaHelper.getTopicFromSubjectForPublish(subject);

        return this._publishToTopic(topic, subject, message,  context, opts);
    }

    /**
     * For a description @see KafkaClient#publish.
     *
     * @param {string} subject - Full subject of the message. Topic will be extracted from this and subject will be added to the message header.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     * @fulfil {object} Result of message producing. Should be {key, partition}
     * @reject {Error}
     */
    async publishToSubject(subject, message, context = null, opts = {})
    {
        const topic = KafkaHelper.getTopicFromSubjectForPublish(subject);
        return this._publishToTopic(topic, subject, message, context, opts);
    }

    async _publishToTopic(topic, subject, message, context = null, opts = {})
    {
        if (typeof opts !== 'object')
            throw new TypeError('Invalid argument opts. Opts has to be an object.');

        if (!this._producer)
            await this._initProducer();

        opts.subject = subject;

        return this._producer.publish(topic, message,  context, opts);
    }

    /**
     * This method allows you to subscribe to one or more events. An event can either be an absolute name of a
     * topic to subscribe (e.g. my-service.status) or a pattern (e.g. my-servervice.#).
     *
     * @async
     * @function subscribe
     * @param {string|RegExp} subject - Full name of a subject or a pattern. The kafka topic will be parsed from this.
     * @param {function} callback - Optional function to be called when a message for a topic or a pattern arrives.
     * @param {SubscribeOpts} opts - Additional options to set for the subscription.
     * @param {boolean} [convertTopic=false] - Indicates if the given topic needs to be converted because it is a RabbitMQ routing key.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    async subscribe(subject, callback = null, opts = {}, convertTopic = false)
    {
        if (!this._consumer)
            await this._initConsumer();

        return this.consumer.subscribe(subject, callback, opts, convertTopic);
    }

    /**
     * This method allows you to unsubscribe from a previous subscribed *topic* or pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the topic existed in the subscriptions.
     */
    unsubscribe(topic)
    {
        if (!this._consumer) {
            this.logger.error('KafkaClient#unsubscribe: Trying to unsubscribe but consumer was not initialized.');
            return false;
        }

        return this._consumer.unsubscribe(topic);
    }

    /** *** PRIVATE METHODS *** */

    async _doReconnect()
    {
        this.logger.error(this.klassName, '#_doReconnect: NOT IMPLEMENTED!');
        return false;
    }

    /**
     * Get this instance's configuration from consul or - if set  - from consulOverride.
     *
     * @private
     * @async
     * @function _getConfig
     * @returns {object} Config object
     */
    async _getMqConfig()
    {
        const isConsulOverride = this.config.consulOverride && this.config.consulOverride.kafkaHost && true;

        if (isConsulOverride) {
            const config = this.config.consulOverride;

            return {
                host: config.kafkaHost,
                port: config.kafkaPort
            };
        }
        else {
            const config = this.config.consul;
            const consul = await configService.init();

            const {host, port} = await consul.getEndPoint(config.mqServiceName);

            this.onEndpointChanged = async (serviceName) =>
            {
                if (serviceName === config.mqServiceName)
                {
                    this.logger.info(`Got on onEndpointChange event for service ${serviceName}.`);
                    await this._doReconnect();
                }
            };

            this.onPropertyChanged = async (key) => {
                if (key === config.mqUserKey || key === config.mqPasswordKey) {
                    this.logger.info(`Got onPropertyChanged event for key ${key}.`);
                    await this._doReconnect();
                }
            };

            consul.on('endpointChanged', this.onEndpointChanged);
            consul.on('propertyChanged', this.onPropertyChanged);

            return {
                host,
                port
            };
        }
    }

    /**
     * Initialize the consumer.
     *
     * @async
     * @function _initConsumer
     * @returns {boolean}
     */
    async _initConsumer()
    {
        this._consumer = new Consumer(this.config);

        let connectionConfig = await this._getMqConfig();
        const connectResult = await this._consumer.connect(connectionConfig);

        this.consumer.on('dlqmessage', this._onConsumerDlqMessage.bind(this));

        this.logger && this.logger.info('KafkaClient#_initConsumer: Consumer connection setup returned: ', connectResult);

        return connectResult;
    }

    /**
     * Initialize the producer.
     *
     * @async
     * @function _initProducer
     * @returns {boolean}
     */
    async _initProducer()
    {
        this._producer = new Producer(this.config);

        let connectionConfig = await this._getMqConfig();
        await this._producer.connect(connectionConfig);

        return true;
    }

    /**
     * Requeue behaviour for kafka.
     *
     * @async
     * @function _onConsumerDlqMessage
     * @param {object} message
     * @param {string} message.key
     * @param {number} message.offset
     * @param {number} message.partition
     * @param {number} message.size - Length of the message.value
     * @param {number} message.timestamp
     * @param {string} message.topic
     * @param {string} message.value - Message payload. Should contain JSON with metadata and payload
     * @param {object} message.value.properties - Stringified JSON containing message headers maintained by KafkaClient
     * @param {string} message.value.properties.routingKey - Topic the message is produced to.
     * @param {string} message.value.content    - Stringified JSON containing the actual payload of the message. Needs to be deserialized with the parser given in the config from KafkaClient#constructor.
     */
    async _onConsumerDlqMessage(message) {
        try {
            const parsedMessage = JSON.parse(message.value);

            if (!this._producer)
                await this._initProducer();

            const dlq = `dlq__${parsedMessage.properties.routingKey}`;

            parsedMessage.properties.routingKey = dlq;
            parsedMessage.properties.subject = `dlq__${parsedMessage.properties.subject}`

            await this.producer._publish(dlq, parsedMessage);

            this.logger.log(`${this.klassName}#_onConsumerDlqMessage: Moved message to DLQ.`, message);
        } catch (e) {
            this.logger.error(`${this.klassName}#_onConsumerDlqMessage: Failed to handle message.`, message, e);
        }
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
* @property {object} logger - [Logger](https://github.com/OpusCapita/logger) object to be used for logging.
* @property {object} consul - Object for configuring consul related parameters.
* @property {string} consul.host - Hostname of a consul server.
* @property {string} consul.mqServiceName - Name of the endpoint for the message queue server in consul.
* @property {object} consulOverride - Configuraion object for manually overriding the message queue connection configuration.
* @property {string} consulOverride.host - Hostname of a message queue server.
* @property {object} context - Optional context object to automatically extend emitted messages.
*/
KafkaClient.DefaultConfig = {
    serializer: JSON.stringify,
    parser: JSON.parse,
    serializerContentType: 'application/json',
    parserContentType: 'application/json',
    consumerGroupId: null,
    enableHealthChecks: true,
    logger: new Logger(),
    consul: {
        host: 'consul',
        mqServiceName: 'kafka'
    },
    consulOverride: {
        host: null
    },
    context: {
    }
};

module.exports = KafkaClient;
