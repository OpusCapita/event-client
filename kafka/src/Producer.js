const EventEmitter = require('events');
const Logger = require('ocbesbn-logger');
const {NProducer} = require('sinek');
const extend = require('extend');
const crypto = require('crypto');
const {ProducerError} = require('./err');

/** Producer class - interface to the sinek producer implementation. */
class Producer extends EventEmitter
{

    constructor(config = {})
    {
        super();

        this.config = config;
        this.logger = config.logger || new Logger();

        this._producer = null;

        this._analyticsReady = false;
        this._knownTopics    = [];
    }

    /** *** GETTER *** */

    get knownTopics() {
        return this._knownTopics;
    }

    /** *** SETTER *** */

    set context(context) {
        this.config.context = context;
    }

    /** *** PUBLIC *** */

    /**
     * Returns the result from the checkHealth of the native producer.
     *
     * @public
     * @async
     * @function checkHealth
     * @returns {Promise}
     * @throws
     */
    async checkHealth()
    {
        if (!this._producer) {
            throw new ProducerError('Initialize producer before accessing health information.', 'ENOTINITIALIZED');
        }

        if (typeof this._producer.checkHealth !== 'function') {
            throw new ProducerError('Not supported.', 'ENOTSUPPORTED');
        }

        return this._producer.checkHealth();
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
     * @async
     * @function dispose
     * @returns {boolean} success
     * @throws
     */
    async dispose()
    {
        try {
            if (this._producer) { await this._producer.close(true); }
        } catch (e) {
            this.logger.error('Producer#dispose: Failed to close the producers with exception. ', e);
        }

        return true;
    }

    /**
     * Publish a message to the given topic
     *
     * @async
     * @function publish
     * @param {string} topic - Full name of a topic.
     * @param {object} content - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    async publish(topic, content, context = null)
    {
        if (!this._knownTopics.includes(topic)) {
            await this._producer.getTopicMetadata(topic); // Create topic on kafka
            this._knownTopics.push(topic);
        }

        const message = {};

        const localContext = extend(true, {}, this.config.context, context, {
            senderService: this.config.serviceName,
            timestamp: new Date().toString()
        });

        message.properties = {
            contentType: this.config.serializerContentType,
            contentEncoding: 'utf-8',
            correlationId: context && context.correlationId,
            appId: this.config.serviceName,
            messageId: `${this.config.serviceName}.${crypto.randomBytes(16).toString('hex')}`,
            headers: localContext
        };

        message.content = this.config.serializer(content);

        let result = null;
        try {
            result = this._producer.send(topic, Buffer.from(JSON.stringify(message)));
        } catch (e) {
            this.logger && this.logger.error('Producer#publish: Failed to send message with exception.', e, message);
            throw e;
        }

        return result;
    }

    /** *** PRIVATE METHODS *** */

    /**
     * Setup the underlying NProducer, connect to kafka and register event listeners
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
        if (this._producer) {
            return this._doConnect();
        };

        this._producer = await this._createProducer(config);

        try {
            await this._doConnect();
        } catch (e) {
            this.logger.error('Producer#_connect: Failed to connect with exception.', e);
            throw e;
        }

        if (this.config && this.config.enableHealthChecks) {
            this._producer.enableAnalytics({
                analyticsInterval: 1000 * 60 * 2, //runs every 2 minutes
                lagFetchInterval: 1000 * 60 * 5 //runs every 5 minutes (dont run too often!)
            });
        }

        this._registerProducerListeners();

        return true;
    }

    /**
     * Encapsulates the call to the sinek Producer.
     *
     * @private
     * @function _doConnect
     * @returns {Promise}
     */
    _doConnect()
    {
        return this._producer.connect();
    }


    /**
     * Dispatcher method that calls creates the underlying producer based on the second parameter.
     * Only native producer is implemented right now.
     *
     * @private
     * @function _createProducer
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @param {boolean} [useNative=true] - Switch between librdkafka and node producers
     * @returns {object} The created producer instance
     */
    _createProducer(config, useNative = true)
    {
        if (useNative) {
            return this._createNativeProducer(config);
        }
    }

    /**
     *
     * Factory method to create a kafka producer using the native implementation.
     *
     * @private
     * @function _createNativeProducer
     * @param {object} config
     * @param {string} config.host - Kafka node to connect to. Uses internal service discovery to find other kafka nodes.
     * @param {string} config.port - Kafka port.
     * @param {number|string} defaultPartitionCount  - amount of default partitions for the topics to produce to
     * @returns {object} The created native producer instance
     */
    _createNativeProducer(config, defaultPartitionCount = 10)
    {
        return new NProducer({
            options: {
                kafkaHost: `${config.host}:${config.port}`,
            },
            noptions: {
                //'debug': 'all',
                'metadata.broker.list': `${config.host}:${config.port}`,
                'client.id': 'event-client-2000',
                'event_cb': true,
                'compression.codec': 'none',
                'retry.backoff.ms': 200,
                'message.send.max.retries': 1000,
                'socket.keepalive.enable': true,
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.ms': 1000,
                'batch.num.messages': 1000000,
            },
            tconf: {
                'request.required.acks': 1
            }
        }, null, defaultPartitionCount);
    }

    /**
     * Eventlistener for errors emitted by the producer.
     *
     * @private
     * @function _onProducerError
     */
    _onProducerError(error) {
        this.logger && this.logger.error('Producer#_onProducerError: Got error response: ', error);
    }

    /**
     * @private
     * @function _registerProducerListeners
     */
    _registerProducerListeners()
    {
        this._producer.once('analytics', () => this._analyticsReady = true);

        this._producer.on('error', this._onProducerError.bind(this));
    }
}

module.exports = Producer;
