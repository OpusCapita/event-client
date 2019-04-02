const retry           = require('bluebird-retry');
const EventEmitter    = require('events');
const Logger          = require('ocbesbn-logger');
const {NConsumer}     = require('sinek');
const {ConsumerError} = require('./err/');
const KafkaHelper     = require('./KafkaHelper');

/** Consumer class - interface to the sinek/Consumer implementation. */
class Consumer extends EventEmitter
{

    constructor(config)
    {
        super();

        this.config = config;

        this._connectionConfig = null; // Will be set by Calling Consumer#connect

        this._logger           = config.logger || new Logger();
        this._consumer         = null;
        this._analyticsReady   = false;

        /**
         * Subject registry is a dict of kafka topic subscriptions on the first level
         * and subjects that may can be received on this topic on the second level. Subjects
         * also store the application callback, eg:
         *
         * {
         *   'service.domain': {
         *     '^service\.domain\.*\.sent': *fnPtr
         *   },
         *   ...
         * }
         *
         * @member {Map} _subjectRegistry
         */
        this._subjectRegistry  = new Map();
    }

    /** *** PUBLIC *** */

    get consumer()  { return this._consumer; }
    get klassName() { return this.constructor.name || 'Consumer'; }

    get logger() {
        if (!this._logger)
            this._logger = new Logger();

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
        if (!this._consumer)
            throw new ConsumerError('Initialize consumer before accessing health information.', 'ENOTINITIALIZED');

        if (typeof this._consumer.checkHealth !== 'function')
            throw new ConsumerError('Not supported.', 'ENOTSUPPORTED');

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
        if (this._connectionConfig === null || this._connectionConfig !== config)
            this._connectionConfig = config;

        return this._connect(this._connectionConfig);
    }

    /**
     * Create a topic and wait for its creation.
     * Workaround until sinek also exposes the node-rdkafka admin client.
     *
     * @param {string} topic - Topic name that should be created
     * @param {number} timeout - Timeout to wait for in ms
     */
    async createTopic(topic, timeout = 10000)
    {
        // Create topic
        await this._consumer.getTopicMetadata(topic);

        // await creation of topic
        return retry(async () => {
            const meta   = await this._consumer.getTopicMetadata(topic);
            const topics = (meta.raw || {}).topics || [];
            const idx    = topics.findIndex(t => t.name === topic);

            if (idx >= 0)
                Promise.resolve(true);
            else
                return Promise.reject(false);
        }, {timeout, interval: 500});
    }

    /**
     * Reconnect to kafka.
     *
     * @public
     * @function connect
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @returns {Promise}
     * @fulfil {boolean}
     * @reject {Error}
     */
    async reconnect(config)
    {
        await this.consumer.close();
        await this.connect(config || this._connectionConfig);

        return true;
    }

    /**
     * Dispose method that frees all known resources.
     *
     * FIXME Race condition in sinek's NConsumer#close method (https://github.com/nodefluent/node-sinek/issues/101)
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
        for (const topic of this._subjectRegistry.keys())
            this._subjectRegistry.delete(topic);

        try {
            if (this._consumer)
                this._consumer.close(true);

            ok = true;
        } catch (e) {
            this.logger.error('Consumer#dispose: Failed to close the consumer with exception. ', e);
            ok = false;
        } finally {
            setImmediate(() => {
                if (this.consumer && this.consumer.consumer)
                    this.consumer.consumer = null; // Workaround for sinek bug #101

                if (this.consumer)
                    this._consumer = null;
            });
        }

        return ok;
    }

    /**
     * Checks whenever the passed *topic* or pattern already has an active subscription inside the
     * current instance of KafkaClient. The *topic* can either be a full name of a
     * channel or a pattern.
     *
     * @function hasSubscription
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {boolean} [convertTopic=false] - Indicates if the given topic needs to be converted because it is a RabbitMQ routing key.
     * @returns {boolean} Returns true if the *topic* is already subscribed; otherwise false.
     * @throws {Error}
     */
    hasSubscription(subject, convertTopic)
    {
        const topicSubscription = convertTopic ? (KafkaHelper.getTopicFromRoutingKey(subject)).source : subject;
        const convertedSubject = (KafkaHelper.convertRabbitWildcard(subject)).source;

        return this._subjectRegistry.has(topicSubscription) && this._subjectRegistry.get(topicSubscription).has(convertedSubject);
    }

    /**
     * Subscribe to a kafka topic.
     *
     * TODO Add config flag to consume only messages that arrive AFTER the consumer group was created.
     *
     * @async
     * @function subscribe
     * @param {string} subject
     * @param {function} callback
     * @param {object} opts
     * @param {string} opts.subject - The original routingKey. Used to register locally for later pattern matching on incoming messages.
     * @param {boolean} convertTopic - Indicates if the given topic needs to be converted because it is a RabbitMQ routing key.
     * @returns {boolean}
     * @throws {ConsumerError}
     */
    async subscribe(subject, callback = null, opts = {}, convertTopic = false)
    {
        const topicSubscription = convertTopic ? (KafkaHelper.getTopicFromRoutingKey(subject)).source : subject;
 
        const convertedSubject = (KafkaHelper.convertRabbitWildcard(subject)).source; // FIXME This should be done in EventClient not here, check that subject is a valid subscription but do not convert

        if (this._subjectRegistry.has(topicSubscription) && this._subjectRegistry.has(topicSubscription)) {
            const subjects = this._subjectRegistry.get(topicSubscription);

            if (subjects.has(convertedSubject)) {
                // Double subject subscriptions are not allowed, otherwise unsubscribe will not work.
                throw new ConsumerError(`The subject "${subject}" on topic "${topicSubscription}" has already been subscribed.`, 'EDOUBLESUBSCRIPTION', 409);
            } else {
                // Already subscribed to topic, just add subject to registry
                subjects.set(convertedSubject, callback);
            }
        } else {
            // Consume topic and register subject

            let result;

            try {
                result = this._consumer.addSubscriptions([topicSubscription]);
            } catch (e) {
                throw new ConsumerError(`Failed to subscribe to subject ${subject}`);
            }

            if (result && Array.isArray(result) && result.includes(topicSubscription)) {
                const subjects = new Map();
                subjects.set(convertedSubject, callback);

                this._subjectRegistry.set(topicSubscription, subjects);

                this.logger.info(`Consumer#subscribe: Successfully subscribed to topic ${topicSubscription}`);
            } else {
                const errMsg = `Failed to subscribe to topic ${topicSubscription}.`;
                this.logger.error('Consumer#subscribe: ', errMsg);
                throw new ConsumerError(errMsg, 'ESUBSCRIBEFAILED', 409);
            }
        }


        return true;
    }

    /**
     * Unsubscribe from a topic.
     *
     * @async
     * @function unsubscribe
     * @param {string} subject - Subject to unsubscribe from
     * @returns {boolean} Indicates success.
     */
    async unsubscribe(subject) {
        const topicSubscription = (KafkaHelper.getTopicFromRoutingKey(subject)).source;
        const convertedSubject = (KafkaHelper.convertRabbitWildcard(subject)).source;

        if (!this._subjectRegistry.has(topicSubscription)) {
            this.logger.error(`Consumer#unsubscribe: This instance is not subscribed to subject ${subject}`);
            return false;
        }

        if (!this._consumer || !this._consumer.consumer) {
            this.logger.error('Consumer#unsubscribe: Consumer not setup, call init() first.');
            return false;
        }

        const subjects = this._subjectRegistry.get(topicSubscription);

        if (!subjects.has(convertedSubject))
            throw new Error(`This consumer is not subscribed to the subject ${subject}`);
        else
            subjects.delete(convertedSubject);

        if (subjects.size === 0) {
            this.logger.info(`Consumer#unsubscribe: Subscription on topic ${topicSubscription} is empty. Remove topic subscription.`);

            const consumerSubscriptionBefore = this._consumer.consumer.subscription();

            const changedSubscription = [...this._subjectRegistry.keys()].filter((v) => v !== topicSubscription);

            /** Workaround for bug in sinek */
            if (changedSubscription && changedSubscription.length === 0)
                this._consumer.consumer.unsubscribe();
            else
                this._consumer.adjustSubscription(changedSubscription);

            const consumerSubscriptionAfter = this._consumer.consumer.subscription();

            if (consumerSubscriptionAfter.length === 0 || consumerSubscriptionAfter.length === consumerSubscriptionBefore.length - 1) {
                this._subjectRegistry.delete(topicSubscription);
                return true;
            } else {
                return false;
            }
        }

        return true;
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
     * @param {boolean} [useAsapReceiveMode=false] - Switch reveive mode (1:1 vs. asap)
     * @returns {boolean} true if successful or throws
     */
    async _connect(config, useAsapReceiveMode = false)
    {
        if (this._consumer)
            return this._doConnect();

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
        if (useAsapReceiveMode)
            this._consumer.consume(null, true, false, options);
        else
            this._consumer.consume(this._onConsumerMessage.bind(this), true, false);

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
                // 'enable.auto.commit': true,
                //'debug': 'all',
                'event_cb': true,
                'metadata.max.age.ms': 10000,
                'topic.metadata.refresh.interval.ms': 1000
            },
            tconf: {
                'auto.offset.reset': 'latest',
                'request.required.acks': -1
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
    async _onConsumerMessage(message, doneCb)
    {
        let payload, context, rabbitRoutingKey, messageSubject;

        try {
            const {routingKey, content, headers, subject} = this._prepareIncomingMessage(message);
            rabbitRoutingKey = routingKey;
            payload = content;
            context = headers;
            messageSubject = subject;
        } catch (e) {
            this.logger.error(this.klassName, '#_onConsumerMessage: Failed to parse incoming message with exception.', message, e);
            return; // !!!
        }

        for (const t of this._subjectRegistry.keys())
        {
            let requeMessage = false;

            if (message.topic.match(new RegExp(t)))
            {
                const subjects = this._subjectRegistry.get(t);

                for (const [subject, callback] of subjects) {

                    if (messageSubject.match(new RegExp(subject)))
                    {
                        if (typeof callback !== 'function')
                            throw new Error('Application callback is not a function.'); // TODO Maybe not throw?

                        let result = null;
                        try {
                            result = await retry(async () => {

                                const result = await callback(payload, context, message.topic, rabbitRoutingKey);
                                if (result !== true)
                                    throw new Error('Application callback returned a value other than true.');
                                return result;

                            }, {'max_tries': 3});
                        } catch (e) {
                            if (result !== true && !message.topic.startsWith('dlq__')) {
                                requeMessage = true;
                            }

                            this.logger.error(this.klassName, '#_onConsumerMessage: Calling the registered callback for topic ', message.topic, ' failed with exception.', e);
                            requeMessage = true;
                        }

                        if (requeMessage) {
                            this.emit('dlqmessage', message);
                        }
                    }
                }

            }
        }

        // Commit message and continue consuming in 1:1 receive mode.
        if (typeof doneCb === 'function')
            doneCb();
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
        if (!message || !message.topic)
            throw new ConsumerError('Invalid message received.', 'EMSGINVALID');

        if (!message.value)
            throw new ConsumerError('Message has no value.', 'EMSGEMPTY');

        let parsedMessageValue =  JSON.parse(message.value); // This may throw a parser exception.

        if (!parsedMessageValue || !parsedMessageValue.properties)
            throw new ConsumerError('Message without properties recveived.', 'EMSGINVALID');

        let messageProperties = parsedMessageValue.properties;
        if (messageProperties.contentType !== this.config.serializerContentType)
            throw new ConsumerError('Message properties has no contentType.', 'EMSGINVALID');

        let headers = messageProperties.headers || {};
        headers.topic = message.topic;

        let content = this._deserializeMessageContent(parsedMessageValue.content);

        return {
            routingKey: messageProperties.routingKey,
            subject: messageProperties.subject,
            content,
            headers
        };
    }

    /**
     * Registers listeners to the sinek consumer.
     *
     * @private
     * @function _registerConsumerListeners
     */
    _registerConsumerListeners()
    {
        this._consumer.once('analytics', () => this._analyticsReady = true);

        this._consumer.on('error', this._onConsumerError.bind(this));
        this._consumer.on('ready', this._onConsumerReady.bind(this)); // Not implemented?
        // this._consumer.on('message', this._onConsumerMessage.bind(this)); // FIXME make receive mode configurable
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
