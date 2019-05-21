const extend = require('extend');
const configService = require('@opuscapita/config');
const Promise = require('bluebird');
const Logger = require('ocbesbn-logger');
const crypto = require('crypto');
const AmqpConnection = require('./AmqpConnection');
const AmqpChannel = require('./AmqpChannel');
const EventError = require('./EventError');

class EventClient
{
    /**
     * Class for simplifying access to message queue servers implementing the Advanced Message Queuing Protocol (amqp). Each instance of this class is capable of receiving and emitting events.
     *
     * @param {object} config - For a list of possible configuration values see [DefaultConfig]{@link EventClient.DefaultConfig}.
     * @constructor
     */
    constructor(config)
    {
        this.config = extend(true, {}, EventClient.DefaultConfig, config);
        this.serviceName = configService.serviceName;
        this.exchangeName = this.config.exchangeName || this.serviceName;
        this.queueName = this.config.queueName;

        this.publisherConnection = null;
        this.consumerConnection = null;

        this.channels = [];
        this.pubChannel = null;

        this.callbackErrorCount = {};

        this._logger = this.config.logger || new Logger();
    }

    /**
     * Gets the internal logger. Creates a new one if not
     * created before.
     *
     * @function
     * @returns {Logger}
     */
    get logger() {
        if (!this._logger)
            this._logger = new Logger();

        return this._logger;
    }

    /**
     * Makes some basic initializations like exchange creation as they are automatically done by emitting the first event.
     * This is used to create the required environment without pushing anything tho the queue.
     *
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    init()
    {
        return Promise.resolve((async () =>
        {
            if (this.publisherConnection && this.consumerConnection) {
                return true;
            }

            this.connectionConfig = await this._getConfig();
            this.connectionConfig.serviceName = this.serviceName || 'no-service';

            if (this.publisherConnection === null) {
                this.publisherConnection = new AmqpConnection(this.connectionConfig, this.logger);
            } else {
                await this.publisherConnection.dispose();
                this.publisherConnection = new AmqpConnection(this.connectionConfig, this.logger);
            }

            if (this.consumerConnection === null) {
                this.consumerConnection = new AmqpConnection(this.connectionConfig, this.logger);
            } else {
                await this.consumerConnection.dispose();
                this.consumerConnection = new AmqpConnection(this.connectionConfig, this.logger);
            }

            await Promise.all([this.publisherConnection.connect(), this.consumerConnection.connect()]);

            const channel = new AmqpChannel(
                this.publisherConnection,
                this.logger,
                this.connectionConfig,
                {
                    allowPing: true,
                    exchangeName: this.exchangeName,
                    useConfirmChannel: true
                }
            );

            await channel.createExchange(this.exchangeName);
            await channel.createDeadExchange(this.exchangeName);

            this.pubChannel = channel;

            this.getChannel = new AmqpChannel(
                this.consumerConnection,
                this.logger,
                this.connectionConfig,
                {
                    allowPing: false,
                    exchangeName: this.exchangeName
                });

            return true;
        })());
    }

    /**
     * Raises an event for a certain topic by passing a message and an optional context.
     *
     * The passed *topic* has to be a string and identify the raised event as exact as possible.
     * The passed *message* can consis of all data types that can be serialized into a string.
     * The optional *context* paraemter adds additional meta data to the event. It has to be an event
     * and will extend a possibly existing global context defined by the config object passed
     * to the constructor (see {@link EventClient.DefaultConfig}).
     *
     * @param {string} topic - Full name of a topic.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    emit(topic, message, context = null, opts = { })
    {
        return Promise.resolve((async () =>
        {
            if (!this.pubChannel) {
                await this.init();
            }

            let localContext = {
                senderService: this.serviceName,
                timestamp: new Date().toString()
            };

            localContext = extend(true, { }, this.config.context, context, localContext);
            const messageId = `${this.serviceName}.${crypto.randomBytes(16).toString('hex')}`;

            const options = {
                ...opts,
                persistent: true,
                contentType: this.config.serializerContentType,
                contentEncoding: 'utf-8',
                timestamp: Math.floor(Date.now() / 1000),
                correlationId: context && context.correlationId,
                appId: this.serviceName,
                messageId: messageId,
                headers: localContext
            };

            const logger = this.logger.clone();

            logger.contextify(extend(true, { }, localContext, options));
            logger.info(`Emitting event "${topic}"`);

            const exchangeName = this.exchangeName;
            const messageBuffer = Buffer.from(this.config.serializer(message));

            let messageToPublish = {exchangeName, topic, messageBuffer, options};
            let result = false;
            try {
                result = await this.pubChannel.publish(messageToPublish);
            } catch (e) {
                logger.error(`EventClient: Failed to publish message to ${exchangeName}/${topic}`, e);
            }

            if (result) {
                return result;
            }
            else {
                logger.error(`EventClient: Failed to publish message to ${exchangeName}/${topic}`, messageToPublish);
                throw new EventError('Unkown error: Event could not be published.', 500);
            }
        })());
    }

    /**
     * This method allows you to subscribe to one or more events. An event can either be an absolute name of a
     * topic to subscribe (e.g. my-service.status) or a pattern (e.g. my-servervice.#).
     *
     * The optional callback function to be passed will be called, once a message arrives. Its definition should look like
     * **(payload, context, topic) : true|false|Promise**. In case false or Promise.resolve(false) is returned
     * from this callback or an error is thrown, the subscribe method will reschedule the event for redelivery.
     * If nothing or anything !== false or Promise.resolve(false) is returned, the event will be marked as delivered.
     *
     * If no callback function is passed, this mehtod will just create a queue for the passed topic and connect it to the right exchange.
     * This is meant to be used with the **getMessage()** method to actively fetch single messages.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {function} callback - Optional function to be called when a message for a topic or a pattern arrives.
     * @param {SubscribeOpts} opts - Additional options to set for the subscription.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    subscribe(topic, callback = null, opts = { })
    {
        return Promise.resolve((async () =>
        {
            const channel = await this._getNewChannel();
            const exchangeName = topic.substr(0, topic.indexOf('.'));
            const queueName = this.getQueueName(topic);

            if (callback) {
                const prefetchCount = opts && opts.messageLimit;

                const localCallback = async (message) =>
                {
                    const logger = this.logger.clone();
                    const {routingKey} = message.fields;
                    const {contentType, headers} = message.properties;

                    logger.info(`Receiving event for registered topic "${topic}" with routing key "${routingKey}".`);

                    if (contentType === this.config.parserContentType)
                    {
                        let payload;
                        try {
                            payload = this.config.parser(message.content);
                        } catch (parserException) {
                            let msg = 'EventClient#subscribe#localCallback: Failed to parse incoming message';
                            this.logger.error(msg, Buffer.from(JSON.stringify(message)).toString('base64'));
                            throw new EventError(msg, 500);
                        }

                        logger.contextify(extend(true, { }, headers, message.properties));
                        logger.info(`Passing event "${routingKey}" to application.`);

                        try
                        {
                            const result = await callback(payload, headers, routingKey);

                            if (result === false) {
                                throw new Error('Callback rejected to process event.');
                            }

                            return result;
                        }
                        catch (e)
                        {
                            this.callbackErrorCount[topic] = this.callbackErrorCount[topic] + 1 || 1;

                            if (this.callbackErrorCount[topic] >= 32) {
                                throw new EventError(`Processing the topic "${topic}" did not succeed after several tries.`, 500);
                            }
                            else {
                                throw e;
                            }
                        }
                    }
                    else
                    {
                        this.callbackErrorCount[topic] = this.callbackErrorCount[topic] + 1 || 1;

                        if (this.callbackErrorCount[topic] >= 32) {
                            throw new EventError(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${contentType}. Sending event to Nirvana.`, 500);
                        }
                        else {
                            throw new EventError(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${contentType}.`, 406);
                        }
                    }
                };

                return await channel.registerConsumer({
                    exchangeName,
                    queueName,
                    topic,
                    prefetchCount,
                    callback: localCallback
                });

            }
            else
            {
                await channel.createAndBindQueue({queueName, exchangeName, topic});
            }
        })());
    }

    /**
     * Gets a single message object from a queue associated to the passed topic.
     * The returned message object looks like this: **{ payload, context, topic }**.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {boolean} autoAck - Optional auto ack flag for messages. If set to false, a message has to be acked or nacked with ackMessage() or nackMessage().
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) containing an object if a message was fetched,false if there was no message available or an error.
     */
    getMessage(topic, autoAck = true)
    {
        return Promise.resolve((async () =>
        {
            if (!this.getChannel)
            {
                await this.init();
            }

            const message = await this.getChannel.getMessage(this.getQueueName(topic), autoAck);

            if (message)
            {
                const {routingKey, deliveryTag} = message.fields;
                const {contentType, headers} = message.properties;

                if (contentType === this.config.parserContentType)
                {
                    const payload = this.config.parser(message.content);

                    return {
                        payload: payload,
                        context: headers,
                        topic: routingKey,
                        tag: deliveryTag
                    };
                }
                else
                {
                    throw new EventError(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${contentType}. Sending event to Nirvana.`, 500);
                }
            }
            else
            {
                return false;
            }
        })());
    }

    /**
     * Sends a positive acknowledgement for a message retrieved via the getMessage() method.
     *
     * @param {object} message - A message retrieved via the getMessage() method.
     * @returns {Promise} Empty [Promise](http://bluebirdjs.com/docs/api-reference.html).
     */
    ackMessage(message)
    {
        return Promise.resolve((async () =>
        {
            if (!this.getChannel)
            {
                await this.init();
            }

            await this.getChannel.ackMessage({fields: {deliveryTag: message.tag}});
        })());
    }

    /**
     * Sends a negative acknowledgement for a message retrieved via the getMessage() method.
     *
     * @param {object} message - A message retrieved via the getMessage() method.
     * @returns {Promise} Empty [Promise](http://bluebirdjs.com/docs/api-reference.html).
     */
    nackMessage(message, allUpTo = false, requeue = true)
    {
        return Promise.resolve((async () =>
        {
            if (!this.getChannel)
            {
                await this.init();
            }

            await this.getChannel.nackMessage({fields: {deliveryTag: message.tag}}, allUpTo, requeue);
        })());
    }

    /**
     * Checks whenever an exchange exists or not. As checking for non existing exchnages provokes server errors (404) that will destroy the communication channel and log an error, this method should
     * not be called excessively.
     * @param {string} exchangeName - The name of the exchange to find.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the exchange exists.
     */
    exchangeExists(exchangeName)
    {
        return Promise.resolve((async () =>
        {
            if (!this.pubChannel)
            {
                await this.init();
            }

            return this.pubChannel.exchangeExists(exchangeName);
        })());
    }

    /**
     * Checks whenever a queue exists or not. As checking for non existing queues provokes server errors (404) that will destroy the communication channel and log an error, this method should
     * not be called excessively.
     * @param {string} queueName - The name of the queue to find.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the queue exists.
     */
    queueExists(queueName)
    {
        return Promise.resolve((async () =>
        {
            if (!this.pubChannel)
            {
                await this.init();
            }

            return this.pubChannel.queueExists(queueName);
        })());
    }

    /**
     * Removes a queue if it exists.
     * @param {string} queueName - The name of the queue to remove.
     * @param {DeleteQueueOpts} opts - Additional options to set for delition.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) always resolving to null. It only gets rejected in case the server connection could not be estabished.
     */
    deleteQueue(queueName, opts = { })
    {
        return Promise.resolve((async () =>
        {
            if (!this.pubChannel) {
                await this.init();
            }

            return this.pubChannel.deleteQueue(queueName, opts);
        })());
    }

    /**
     * This method allows you to unsubscribe from a previous subscribed *topic* or pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the topic existed in the subscriptions.
     */
    unsubscribe(topic)
    {
        const channel = this._getChannelByTopic(topic);
        return channel && Promise.resolve(channel.removeConsumer(topic));
    }

    /**
     * FIXME remove, this does not work as intended
     *
     * Method for releasing the publishing channel.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false if there was no active connection.
     */
    disposePublisher()
    {
        return this.dispose();
    }

    /**
     * FIXME remove, this does not work as intended
     *
     * Method for releasing all subscriptions and close the subscription channel.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false if there was no active connection.
     */
    disposeSubscriber()
    {
        return this.dispose();
    }

    /**
     * Allows adding a default context to every event emitted.
     * You may also want to construct an instance of this class by passing the context
     * parameter to the constructor. For further information have a look at {@link EventClient.DefaultConfig}.
     */
    contextify(context)
    {
        this.config.context = context || {};
    }

    /**
     * Checks whenever the passed *topic* or pattern already has an active subscription inside the
     * current instance of EventClient. The *topic* can either be a full name of a
     * channel or a pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {boolean} Returns true if the *topic* is already registered; otherwise false.
     */
    hasSubscription(topic)
    {
        const channel = this._getChannelByTopic(topic);
        return Promise.resolve(channel !== null);
    }

    /**
     * Depending on the configuration of the EventClient object, this method returns a configured queue name or a constructed name for the passed topic.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {string} Name of a queue.
     */
    getQueueName(topic = null)
    {
        return this.queueName ? this.queueName : `${this.serviceName}/${topic}`;
    }

    /**
     * Closes all publisher and subscriber channels and connections.
     *
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or rejecting with an error.
     */
    dispose()
    {
        return Promise.resolve((async () =>
        {
            const hadChannelOrConnection = (this.channels.length || this.publisherConnection || this.consumerConnection) ? true : false;

            this._onEndpointChanged && configService.removeListener('endpointChanged', this._onEndpointChanged);
            this._onPropertyChanged && configService.removeListener('propertyChanged', this._onPropertyChanged);

            let channelClosePromises = this.channels.map((c) => {
                return c.close().catch((e) => this.logger.error(`EventClient#dispose: Failed to close channel ${c.ch}`, e));
            });
            await Promise.all(channelClosePromises);

            if (this.pubChannel) {
                // TODO why is this flushWaitQueue deactivated?
                // await this.pubChannel.flushWaitQueue();

                await this.pubChannel.close();
            }

            if (this.getChannel) {
                await this.getChannel.close();
            }

            if (this.publisherConnection) {
                await this.publisherConnection.dispose();
            }

            if (this.consumerConnection) {
                await this.consumerConnection.dispose();
            }

            this.channels = [];
            this.pubChannel = null;

            this.publisherConnection = null;
            this.consumerConnection = null;

            return hadChannelOrConnection;
        })());
    }

    async _getConfig()
    {
        const isConsulOverride = this.config.consulOverride && this.config.consulOverride.host && true;

        if (isConsulOverride)
        {
            const config = this.config.consulOverride;

            return {
                host: config.host,
                port: config.port,
                username: config.username,
                password: config.password,
                maxConnectTryCount: 5,
                httpPort: 15672
            };
        }
        else
        {
            const config = this.config.consul;
            const consul = await configService.init();

            const {host, port} = await consul.getEndPoint(config.mqServiceName);
            const [username, password] = await consul.get([config.mqUserKey, config.mqPasswordKey]);

            if (!consul.listeners('endpointChanged').some((fn) => fn === this._onEndpointChanged))
                consul.on('endpointChanged', this._onEndpointChanged);

            if (!consul.listeners('propertyChanged').some((fn) => fn === this._onPropertyChanged))
                consul.on('propertyChanged', this._onPropertyChanged);

            return {
                host,
                port,
                username,
                password,
                maxConnectTryCount: 10,
                httpPort: 15672
            };
        }
    }

    async _getNewChannel()
    {
        if (!this.consumerConnection)
            await this.init();

        const channel = new AmqpChannel(this.consumerConnection, this.logger, this.connectionConfig, {exchangeName: this.exchangeName});
        this.channels.push(channel);

        return channel;
    }

    _getChannelByTopic(topic)
    {
        const channels = this.channels.filter(c => c.hasConsumer(topic));
        return channels.length ? channels[0] : null;
    }


    /**
     * FIXME this just sets the config on the connection not doing any reconnect ... ?
     *
     */
    async _doReconnect()
    {
        const config = this.config.consul;

        const {host, port} = await configService.getEndPoint(config.mqServiceName);
        const [username, password] = await configService.get([config.mqUserKey, config.mqPasswordKey]);

        if (this.publisherConnection)
            this.publisherConnection.setConfig({host, port, username, password});

        if (this.consumerConnection)
            this.consumerConnection.setConfig({host, port, username, password});

        return true;
    }

    _onEndpointChanged(serviceName)
    {
        const config = this.config.consul;

        if (serviceName === config.mqServiceName)
        {
            this.logger.info(`EventClient#_onEndpointChanged: Got on onEndpointChange event for service ${serviceName}.`);
            try {
                this._doReconnect();
            } catch (e) {
                this.logger.error('EventClient#_onEndpointChanged: Failed to call _doReconnect with exception. ', e);
            }
        }
    }

    _onPropertyChanged(key)
    {
        const config = this.config.consul;

        if (key === config.mqUserKey || key === config.mqPasswordKey)
        {
            this.logger.info(`EventClient#_onPropertyChanged: Got on onPropertyChanged event for key ${key}.`);
            try {
                this._doReconnect();
            } catch (e) {
                this.logger.error('EventClient#_onPropertyChanged: Failed to call _doReconnect with exception. ', e);
            }
        }
    }

}

module.exports = EventClient;

/**
* Static object representing a default configuration set.
*
* @property {object} serializer - Function to use for serializing messages in order to send them.
* @property {object} parser - Function to use for deserializing messages received.
* @property {string} serializerContentType - Content type of the serialized message added as a meta data field to each event emitted.
* @property {string} parserContentType - Content type for which events should be received and parsed using the configured parser.
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
    queueName: null,
    exchangeName: null,
    logger: new Logger(),
    consul: {
        host: 'consul',
        mqServiceName: 'rabbitmq-amqp',
        mqUserKey: 'mq/user',
        mqPasswordKey: 'mq/password'
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
