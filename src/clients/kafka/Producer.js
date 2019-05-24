const EventEmitter = require('events');
const Logger = require('ocbesbn-logger');
const extend = require('extend');
const crypto = require('crypto');
const {ProducerError} = require('./err');

/**
 * Storage for in-flight or failed messages.
 *
 * - On sending a message the producer stores it to the messageStatusTracker with a
 *   delivery status ('delivered') set to 'null'. This means the message is in flight and
 *   no delivery report was received so far.
 * - Messages that are delivered successful will be removed from the tracker.
 * - For messages that come back with an errornous delivery state the delivery state is set to 'false'
 *
 * Every message that still is in the tracker after the timeout should be considered to have failed.
 *
 * @todo Better use one messageStatusTracker per instance and not share it on module level for all instances?
 */
const messageStatusTracker = new Map();

/**
 * Producer class - interface to the sinek producer implementation.
 */
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

    get klassName() { return this.constructor.name || 'Producer'; }
    get knownTopics() { return this._knownTopics; }
    get messageStatusTracker() { return messageStatusTracker; }

    /** *** SETTER *** */

    set context(context) { this.config.context = context; }

    /** *** PUBLIC *** */

    /**
     * Returns the result from the checkHealth of the native producer.
     *
     * @public
     * @async
     * @function checkHealth
     * @returns {Promise}
     */
    async checkHealth()
    {
        if (!this._producer)
            throw new ProducerError('Initialize producer before accessing health information.', 'ENOTINITIALIZED');

        if (typeof this._producer.checkHealth !== 'function')
            throw new ProducerError('Not supported.', 'ENOTSUPPORTED');

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
     */
    async dispose()
    {
        if (messageStatusTracker.size > 0)
            this.logger && this.logger.error(`${this.klassName}#dispose: Dispose called but there are ${messageStatusTracker.size} messages in-flight or failed.`);

        try {
            if (this._producer)
                await this._producer.close(true);
        } catch (e) {
            this.logger.error('Producer#dispose: Failed to close the producers with exception. ', e);
        } finally {
            setImmediate(() => {
                if (this._producer && this._producer.producer)
                    this._producer.producer = null; // Workaround for sinek bug #101

                if (this._producer)
                    this._producer = null;
            });
        }

        return true;
    }

    /**
     * Publish a message to the given topic.
     *
     * @async
     * @function publish
     * @param {string} topic - Full name of a topic.
     * @param {object} content - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @param {string} opts.kafkaPartitionKey - Define the key that ensures inorder delivery per topic partition, eg. "tenantId".
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     * @reject {Error}
     */
    async publish(topic, content, context = null, opts = {})
    {
        let partitionKey = opts.kafkaPartitionKey;
        if (!partitionKey) {
            partitionKey = null;
            this.logger.warn(this.klassName, '#publish: Publishing a message without a partition key.');
        }

        if (!this._knownTopics.includes(topic)) {
            await this._producer.getTopicMetadata(topic); // Create topic on kafka if it does not exists
            this._knownTopics.push(topic);
        }

        const message = {};

        const localContext = extend(true, {}, this.config.context, context, {
            senderService: this.config.serviceName,
            timestamp: new Date().toString()
        });

        // topic contains the converted routing key, opts.subject is the original
        message.properties = {
            topic,
            subject: opts.subject || topic,
            routingKey: opts.subject || topic, // Used for backwards compatibillity with Event-Client v2x (rabbitmq)
            contentType: this.config.serializerContentType,
            contentEncoding: 'utf-8',
            correlationId: context && context.correlationId,
            appId: this.config.serviceName,
            messageId: `${this.config.serviceName}.${crypto.randomBytes(16).toString('hex')}`,
            headers: localContext,
            partitionKey
        };

        message.content = this.config.serializer(content);

        return this._publish(topic, message);
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
     * @reject {Error}
     */
    async _connect(config)
    {
        if (this._producer)
            return this._doConnect();

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
     * Only native producer is supported right now.
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
        if (useNative)
            return this._createNativeProducer(config);
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
                'client.id': 'event-client',
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
                'request.required.acks': -1,
                'message.timeout.ms': 300000 // This needs to be set to a value that is longer than rebalancing may take
            }
        }, null, defaultPartitionCount);
    }

    /**
     * Eventlistener for delivery reports. This is a workaround because
     * sinek does not yet provide an interface for this.
     *
     * @private
     * @param {KafkaError} error - The error from librdkafka
     * @param {object} report - Object containing structural delivery information
     */
    _onProducerDeliveryReport(error, report) {
        if (report && report.key) {
            const key = Buffer(report.key).toString();
            const status = messageStatusTracker.get(key);

            this.logger && this.logger.debug(`${this.klassName}: Delivery report for message ${key} received.`);

            if (status) {

                if (error) {
                    this.logger && this.logger.error(`${this.klassName}: Failed to deliver message ${key} - `, status.message);

                    messageStatusTracker.set(key, {
                        ...report,
                        message: status.message,
                        error: error,
                        delivered: false
                    });
                } else {
                    // Delete message from tracker
                    messageStatusTracker.delete(key);
                }

            } else {
                // Received a message that was not sent from this client - this should not happen.
                this.logger && this.logger.error(`${this.klassName}: Message ${key} not found in local outgoing messages cache!`);
            }
        }
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
     * Send message through sinek producer.
     *
     * @private
     * @function _publish
     * @param {string} topic
     * @param {object} message - @see publish
     * @return {Promise}
     */
    async _publish(topic, message) {
        let result = null;

        try {
            const payload = Buffer.from(JSON.stringify(message));
            result = await this._producer.send(topic, payload, null, null, message.properties.partitionKey, message.properties.appId);

            if (result.key) {
                messageStatusTracker.set(result.key, {
                    delivered: null,
                    message
                }); // Null indicates that message is in-flight and no delivery information was received.
            }
        } catch (e) {
            this.logger && this.logger.error('Producer#publish: Failed to send message with exception.', e, message);
            throw e;
        }

        return result;
    }

    /**
     * @private
     * @function _registerProducerListeners
     */
    _registerProducerListeners() {
        this._producer.once('analytics', () => this._analyticsReady = true);
        this._producer.on('error', this._onProducerError.bind(this));
        this._producer.producer.on('delivery-report', this._onProducerDeliveryReport.bind(this));
    }
}

module.exports = Producer;
