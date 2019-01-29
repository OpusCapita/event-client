const EventEmitter = require('events');
const Logger = require('ocbesbn-logger');
const {NConsumer} = require('sinek');
const {ConsumerError} = require('./err/');

const subscribedTopics = require('./TopicSubscription');

/** Consumer class - interface to the sinek/Consumer implementation. */
class Consumer extends EventEmitter
{

    constructor(config)
    {
        super();

        this.config = config;

        this._logger           = config.logger || new Logger();
        this._consumer         = null;
        this._analyticsReady   = false;
        this._subscribedTopics = new Map(); // TODO Maybe this can use the topics property of the NConsumer?
    }

    /** *** PUBLIC *** */

    get consumer()  { return this._consumer; }
    get klassName() { return this.constructor.name || 'Consumer'; }

    get logger() {
        if (!this._logger) { this._logger = new Logger(); }
        return this._logger;
    }

    /**
     * Returns the result from the checkHealth of the native consumer.
     *
     * @public
     * @async
     * @function checkHealth
     * @returns {Promise}
     * @throws {ConsumerError}
     */
    async checkHealth()
    {
        if (!this._consumer) {
            throw new ConsumerError('Initialize consumer before accessing health information.', 'ENOTINITIALIZED');
        }

        if (typeof this._consumer.checkHealth !== 'function') {
            throw new ConsumerError('Not supported.', 'ENOTSUPPORTED');
        }

        return this._consumer.checkHealth();
    }

    /**
     * Setup kafka connection.
     *
     * @public
     * @function connect
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @returns {boolean}
     */
    connect(config)
    {
        return this._connect(config);
    }

    /**
     * Dispose method that frees all known resources.
     *
     * @public
     * @async
     * @function dispose
     * @returns {boolean} success
     */
    async dispose()
    {
        let ok = false;

        /** Delete all topics from the global topics registry */
        for (const topic of this._subscribedTopics.keys()) {
            subscribedTopics.delete(topic);
        }

        /** Empty local subscriptions registry */
        this._subscribedTopics = new Map();

        try {
            if (this._consumer) {
                await this._consumer.close(true);
                this._consumer = null;
            }

            ok = true;
        } catch (e) {
            this.logger.error('Consumer#dispose: Failed to close the consumer with exception. ', e);
            ok = false;
        }

        return ok;
    }

    /**
     * Checks whenever the passed *topic* or pattern already has an active subscription inside the
     * current instance of KafkaClient. The *topic* can either be a full name of a
     * channel or a pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {boolean} Returns true if the *topic* is already subscribed; otherwise false.
     * @throws {Error}
     */
    hasSubscription(topic)
    {
        if (!topic || typeof topic !== 'string' || topic === '') {
            throw new Error('Invalid argument topic given.');
        }

        return this._subscribedTopics.has(topic);
    }

    /**
     * Subscribe to a kafka topic.
     *
     * @todo Add config flag to consume only messages that arrive AFTER the consumer group was created.
     *
     * @async
     * @function subscribe
     * @param {string} topic
     * @param {function} callback
     * @returns {boolean}
     * @throws {ConsumerError}
     */
    async subscribe(topic, callback = null)
    {
        if (subscribedTopics.has(topic)) {
            throw new ConsumerError(`The topic "${topic}" has already been subscribed.`, 'EDOUBLESUBSCRIPTION', 409);
        }

        let result = this._consumer.addSubscriptions([topic]);

        if (result && Array.isArray(result) && result.includes(topic)) {
            subscribedTopics.set(topic, callback);
            this._subscribedTopics.set(topic, true);

            this.logger.info(`Consumer#subscribe: Successfully subscribed to topic ${topic}`);
        } else {
            const errMsg = `Failed to subscribe to topic ${topic}.`;
            this.logger.error('Consumer#subscribe: ', errMsg);
            throw new ConsumerError(errMsg, 'ESUBSCRIBEFAILED', 409);
        }

        return true;
    }

    /**
     * Unsubscribe from a topic.
     *
     * @async
     * @function unsubscribe
     * @param {string} topic
     * @returns {boolean} Indicates success.
     */
    async unsubscribe(topic) {
        if (!this._subscribedTopics.has(topic)) {
            this.logger.error(`Consumer#unsubscribe: This instance is not subscribed to topic ${topic}`);
            return false;
        }

        if (!this._consumer || !this._consumer.consumer) {
            this.logger.error('Consumer#unsubscribe: Consumer not setup.');
            return false;
        }

        const consumerSubscriptionBefore = this._consumer.consumer.subscription();

        this._subscribedTopics.delete(topic);

        const changedSubscription = [...this._subscribedTopics.keys()];

        /** Workaround for bug in sinek */
        if (changedSubscription && changedSubscription.length === 0) {
            this._consumer.consumer.unsubscribe();
        } else {
            this._consumer.adjustSubscription(changedSubscription);
        }

        const consumerSubscriptionAfter = this._consumer.consumer.subscription();

        if (consumerSubscriptionAfter.length === 0 || consumerSubscriptionAfter.length === consumerSubscriptionBefore.length - 1) {
            subscribedTopics.delete(topic);
            return true;
        } else {
            return false;
        }
    }

    /** *** PRIVATE METHODS *** */

    /**
     * Setup the underlying NConsumer, connect to kafka and register event listeners
     * Currently uses ASAP mode for message consumption with auto ackknowledgement of messages.
     *
     * @private
     * @async
     * @function _connect
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @returns {boolean} true if successful or throws
     */
    async _connect(config)
    {
        if (this._consumer) {
            return this._doConnect();
        };

        this._consumer = await this._createConsumer(config);

        try {
            await this._doConnect();
        } catch (e) {
            this.logger.error('Consumer#_connect: Failed to connect with exception.', e);
            throw e;
        }

        if (this.config && this.config.enableHealthChecks) {
            this._consumer.enableAnalytics({
                analyticsInterval: 1000 * 60 * 2, //runs every 2 minutes
                lagFetchInterval: 1000 * 60 * 5 //runs every 5 minutes (dont run too often!)
            });
        }

        this._registerConsumerListeners();

        const options = {
            /** grab up to n messages per batch round */
            batchSize: 1,

            /** commit all offsets on every nth batch */
            commitEveryNBatch: 3,

            /** calls synFunction in parallel * 2 for messages in batch */
            concurrency: 1,

            /**
             * commits asynchronously (faster, but potential danger
             * of growing offline commit request queue)
             *
             * @default [true]
             */
            commitSync: true
        };

        /**
         * Consume message in asap mode, decode message to UTF8 string.
         *
         * Info:
         * - 1 by 1 mode by passing a callback to .consume() - consumes a single message and commit after callback each round
         *     - The callback provided as first parameter will receive the message and another callback. The callback
         *       needs to be executed to finish the message processing in the native consumer {@link node-sinek/NConsuer#consume:569}
         * - asap mode by passing no callback to .consume() - consumes messages as fast as possible
         */
        this._consumer.consume(null, true, false, options);

        return true;
    }

    /**
     * Encapsulates the call to the sinek Consumer.
     *
     * @private
     * @function _doConnect
     * @returns {Promise}
     */
    _doConnect()
    {
        return this._consumer.connect();
    }

    /**
     * Dispatcher method that calls creates the underlying consumer based on
     * the second parameter.
     *
     * Only native consumer is implemented right now.
     *
     * @private
     * @function _createConsumer
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal
     *                               service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @param {boolean} [useNative=true] - Switch between librdkafka and node consumer
     * @returns {object} The created consumer instance
     */
    _createConsumer(config, useNative = true)
    {
        if (useNative) {
            return this._createNativeConsumer(config);
        }
    }

    /**
     *
     * Factory method to create a kafka consumer using the native implementation.
     *
     * @private
     * @function _createNativeConsumer
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal
     *                               service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @returns {object} The created native consumer instance
     */
    _createNativeConsumer(config)
    {
        return new NConsumer([], {
            noptions: {
                'metadata.broker.list': `${config.host}:${config.port}`,
                'group.id': this.config.consumerGroupId, // Default is serviceName
                'client.id': 'event-client',
                'enable.auto.commit': true,
                //'debug': 'all',
                'event_cb': true
            },
            tconf: {
                'auto.offset.reset': 'largest'
            }
        });
    }

    /**
     * Deserialize the application level content from incoming message
     * with the given parser from this.config.parser.
     *
     * @function _deserializeMessageContent
     * @param {Buffer|String}
     * @return {object}
     * @throws {ConsumerError}
     */
    _deserializeMessageContent(serializedContent)
    {
        let result = null;

        try {
            result = this.config.parser(serializedContent);
        } catch (e) {
            throw new ConsumerError('Failed to deserialize message content with exception.', 'EDESERIALIZEFAILED');
        }

        return result;
    }

    /**
     * Eventlistener for errors emitted by the consumer.
     *
     * @private
     * @function _onConsumerError
     */
    _onConsumerError(error) {
        this.logger.error(this.klassName, '#_onConsumerError: Got error response: ', error);
    }

    /**
     * Eventlistener for incoming messages.
     *
     * @todo Implement requeue behaviour for falsy values returned from the application callback in ASAP receiving mode.
     *
     * @private
     * @function _onConsumerMessage
     * @param {object} message
     * @param {string} message.key
     * @param {number} message.offset
     * @param {number} message.partition
     * @param {number} message.size - Length of the message.value
     * @param {number} message.timestamp
     * @param {string} message.topic
     * @param {string} message.value - Message payload. Should contain JSON with metadata and payload
     * @param {string} message.value.properties - Stringified JSON containing message headers maintained by KafkaClient
     * @param {string} message.value.content    - Stringified JSON containing the actual payload of the message. Needs to be deserialized with the parser given in the config from KafkaClient#constructor.
     */
    _onConsumerMessage(message)
    {
        let payload, context, rabbitRoutingKey;
        try {
            const {routingKey, content, headers} = this._prepareIncomingMessage(message);
            rabbitRoutingKey = routingKey;
            payload = content;
            context = headers;
        } catch (e) {
            this.logger.error(this.klassName, '#_onConsumerMessage: Failed to parse incoming message with exception.', message, e);
            return; // !!!
        }

        for (const [t, cb] of subscribedTopics.entries())
        {
            let requeMessage = false;

            if (message.topic.match(new RegExp(t)))
            {
                try {
                    const result = cb(payload, context, message.topic, rabbitRoutingKey);
                    if (result !== true && !message.topic.endsWith('__dlq')) {
                        this.logger.warn(this.klassName, '#_onConsumerMessage: Application callback returned a value other than true.');
                        requeMessage = true;
                    }
                } catch (e) {
                    this.logger.error(this.klassName, '#_onConsumerMessage: Calling the registered callback for topic ', message.topic, ' failed with exception.', e);
                    requeMessage = true;
                }

                if (requeMessage) {
                    this.emit('requeue', message);
                }
            }
        }
    }

    /**
     * Event handler for 'ready' events from sinek consumer. Afaik not implemented atm.
     *
     * @function _onConsumerReady
     */
    _onConsumerReady() {
        this.logger.info('Consumer#_onConsumerReady: Ready event received.');
    }

    /**
     * Parse incoming message, check fields and config, and extract context and payload.
     *
     * TODO add unit tests
     *
     * @function _prepareIncomingMessage
     * @param {object} message
     * @param {string} message.value - Stringified JSON of message structure
     * @returns {object} Struct containing message content and headers
     * @throws {ConsumerError|ParserError}
     */
    _prepareIncomingMessage(message) {
        if (!message || !message.topic) {
            throw new ConsumerError('Invalid message received.', 'EMSGINVALID');
        }

        if (!message.value) {
            throw new ConsumerError('Message has no value.', 'EMSGEMPTY');
        }

        let parsedMessageValue =  JSON.parse(message.value); // This may throw a parser exception.

        if (!parsedMessageValue || !parsedMessageValue.properties) {
            throw new ConsumerError('Message without properties recveived.', 'EMSGINVALID');
        }

        let messageProperties = parsedMessageValue.properties;
        if (messageProperties.contentType !== this.config.serializerContentType) {
            throw new ConsumerError('Message properties has no contentType.', 'EMSGINVALID');
        }

        let headers = messageProperties.headers || {};
        headers.topic = message.topic;

        let content = this._deserializeMessageContent(parsedMessageValue.content);

        return {
            routingKey: messageProperties.routingKey,
            content,
            headers
        };
    }

    /**
     * Registers listeners to the sinke consumer.
     *
     * @private
     * @function _registerConsumerListeners
     */
    _registerConsumerListeners()
    {
        this._consumer.once('analytics', () => this._analyticsReady = true);

        this._consumer.on('error', this._onConsumerError.bind(this));
        this._consumer.on('message', this._onConsumerMessage.bind(this));
        this._consumer.on('ready', this._onConsumerReady.bind(this)); // Not implemented?
    }

    /**
     * Returns a list of subscribed topics retrieved from the sinek consumer.
     *
     * @private
     * @function subscription
     * @returns {array}
     */
    _subscription()
    {
        if (!this._consumer) {
            /** Not connected -> not subscribed -> return emtpy */
            return [];
        }

        return this._consumer.consumer.subscription();
    }
}

module.exports = Consumer;
