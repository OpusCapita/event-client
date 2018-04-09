const extend = require('extend');
const configService = require('@opuscapita/config');
const Promise = require('bluebird');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const Logger = require('ocbesbn-logger');
const crypto = require('crypto');

const cachedInstances = { };

/**
 * Options object for event emmitting.
 * @typedef {Object} EmitOpts
 * @property {boolean} mandatory If true, the event will be returned if it is not routed to a queue.
 * @property {number} expiration Time in milliseconds for an event to last in a queue before it gets removed.
 */

/**
 * Options object for event subscriptions.
 * @typedef {Object} SubscribeOpts
 * @property {number} messageLimit The maximum amount of unacknowleged messages a single subscription will get at once.
 */

/**
* Class for simplifying access to message queue servers implementing the Advanced Message Queuing Protocol (amqp).
* Each instance of this class is capable of receiving and emitting events.
*/
class EventClient
{
    /**
     * Creates a new instance of EventClient.
     * @param {object} config - For a list of possible configuration values see {@link EventClient.DefaultConfig}.
     */
    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);

        const cacheKey = crypto.createHash('md5').update(JSON.stringify(this.config)).digest("hex");

        if(cachedInstances[cacheKey])
            return cachedInstances[cacheKey];

        this.connection = null;
        this.pubChannel = null;
        this.subChannels = { };
        this.subscriptions = { };
        this.callbacks = { };
        this.serviceName = configService.serviceName;
        this.exchangeName = this.config.exchangeName || this.serviceName;
        this.queueName = this.config.queueName || this.serviceName;
        this.logger = new Logger({ context : { serviceName : this.serviceName } });
        this.callbackErrorCount = { };

        cachedInstances[cacheKey] = this;
    }

    /**
     * Makes some basic initializations like exchange creation as they are automatically done by emitting the first event.
     * This is used to create the required environment without pushing anything tho the queue.
     *
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    init()
    {
        return Promise.resolve(this._getPubChannel()).then(() => true);
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
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    emit(topic, message, context = null, opts = { })
    {
        return Promise.resolve(this._emit(topic, message, context, opts).catch(e => { this.disposePublisher(); return this._emit(topic, message, context, opts); }));
    }

    async _emit(topic, message, context, opts)
    {
        const logger = new Logger({ context : { serviceName : this.serviceName } });

        const localContext = {
            senderService : this.serviceName,
            timestamp : new Date().toString()
        };

        const transportObj = {
            topic : topic,
            context : extend(true, { }, this.config.context, context, localContext),
            payload : message
        };

        const messageId = `${this.serviceName}.${crypto.randomBytes(16).toString('hex')}`;

        const options = {
            ...opts,
            persistent : true,
            contentType : this.config.serializerContentType,
            contentEncoding : 'utf-8',
            timestamp : Math.floor(Date.now() / 1000),
            correlationId : context && context.correlationId,
            appId : this.serviceName,
            messageId : messageId,
            headers : transportObj.context
        }

        logger.contextify(extend(true, { }, transportObj.context, options));
        logger.info(`Emitting event "${topic}"`);

        const messageBuffer = Buffer.from(this.config.serializer(transportObj));
        const result = await (await this._getPubChannel()).publish(this.exchangeName, topic, messageBuffer, options);

        if(result)
            return null;
        else
            throw new Error('Unkown error: Event could not be published.');
    }

    /**
     * This method allows you to subscribe to one or more events. An event can either be an absolute name of a
     * topic to subscribe (e.g. my-service.status) or a pattern (e.g. my-servervice.#).
     *
     * The callback function to be passed will be called, once a message arrives. Its definition should look like
     * **(payload, context, topic) : true|false|Promise**. In case false or Promise.resolve(false) is returned
     * from this callback or an error is thrown, the subscribe method will reschedule the event for redelivery.
     * If nothing or anything !== false or Promise.resolve(false) is returned, the event will be marked as delivered.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @param {function} callback - Function to be called when a message for a topic or a pattern arrives.
     * @param {SubscribeOpts} opts - Additional options to set for the subscription.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    subscribe(topic, callback, opts = { })
    {
        if(this.hasSubscription(topic))
            Promise.reject(new Error(`The topic "${topic}" is already registered.`));

        const channel = Promise.resolve(this._getNewChannel());
        this.subChannels[topic] = channel;

        return channel.then(async channel =>
        {
            if(opts && opts.messageLimit)
                await channel.prefetch(opts.messageLimit);

            this._addCallback(topic, callback);

            const exchangeName = topic.substr(0, topic.indexOf('.'));

            const consumer = await this._registerConsumner(channel, exchangeName, this.queueName, topic, message =>
            {
                const routingKey = message.fields.routingKey;
                const callback = this._findCallback(routingKey);
                const logger = new Logger({ context : { serviceName : this.serviceName } });

                logger.info(`Receiving event for registered topic "${topic}" with routing key "${routingKey}".`);

                if(callback)
                {
                    if(message.properties.contentType === this.config.parserContentType)
                    {
                        const result = this.config.parser(message.content);

                        if(message.properties.headers)
                            result.context = message.properties.headers;

                        logger.contextify(extend(true, { }, result.context, message.properties));
                        logger.info(`Passing event "${result.topic}" to application.`);

                        return callback(result.payload, result.context, result.topic);
                    }
                    else
                    {
                        this._incrementCallbackErrors(topic);

                        if(this.callbackErrorCount[topic] >= 30)
                        {
                            logger.error(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${message.contentType}. Sending event to nirvana.`);
                        }
                        else
                        {
                            logger.error(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${message.contentType}.`);
                            throw new Error(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${message.contentType}.`);
                        }
                    }
                }
                else
                {
                    this._incrementCallbackErrors(topic);

                    if(this.callbackErrorCount[topic] >= 30)
                    {
                        logger.info(`There is no subscriber for topic "${topic}". Sending event to nirvana.`);
                    }
                    else
                    {
                        logger.info(`There is no subscriber for topic "${topic}".`);
                        throw new Error(`There is no subscriber for topic "${topic}".`);
                    }
                }
            });

            this.subscriptions[topic] = consumer.consumerTag;

            return null;
        });
    }

    /**
     * Checks whenever an exchange exists or not. As checking for non existing exchnages provokes server errors (404) that will destroy the communication channel and log an error, this method should
     * not be called excessively.
     * @param {string} exchangeName - The name of the exchange to find.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true or false depending on whenever the exchange exists.
     */
    exchangeExists(exchangeName)
    {
        return Promise.resolve(this._getPubChannel().then(channel => channel.checkQueue(queueName)).then(() => true).catch(() => this.disposePublisher().then(() => false)));
    }

    /**
     * Checks whenever a queue exists or not. As checking for non existing queues provokes server errors (404) that will destroy the communication channel and log an error, this method should
     * not be called excessively.
     * @param {string} queueName - The name of the queue to find.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true or false depending on whenever the queue exists.
     */
    queueExists(queueName)
    {
        return Promise.resolve(this._getPubChannel().then(channel => channel.checkQueue(queueName)).then(() => true).catch(() => this.disposePublisher().then(() => false)));
    }

    /**
     * This method allows you to unsubscribe from a previous subscribed *topic* or pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true or false depending on whenever the topic existed in the subscriptions.
     */
    unsubscribe(topic)
    {
        const channel = this.subChannels[topic];
        const subscription = this.subscriptions[topic];

        if(channel && subscription)
        {
            return channel.then(async channel =>
            {
                await channel.cancel(subscription).catch(() => null);

                delete this.subscriptions[topic];
                delete this.callbacks[this._findCallbackKey(topic)];
                delete this.callbackErrorCount[topic];

                return true;
            });
        }

        return Promise.resolve(false);
    }

    /**
     * Method for releasing the publishing channel.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true or false if there was no active connection.
     */
    disposePublisher()
    {
        if(this.pubChannel)
            return Promise.resolve(this.pubChannel).then(channel => channel.close()).catch(() => null).finally(() => this.pubChannel = null).then(() => true);

        return Promise.resolve(false);
    }

    /**
     * Method for releasing all subscriptions and close the subscription channel.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving with true or false if there was no active connection.
     */
    disposeSubscriber()
    {
        const all = [ ];

        for(const key in this.subChannels)
            all.push(this.subChannels[key].then(channel => channel.close().catch(() => null)));

        if(all.length)
            return Promise.all(all).then(() => { this.subChannels = { }; this.callbacks = { }; this.callbackErrorCount = { } }).then(() => true);

        return Promise.resolve(false);
    }

    /**
     * Allows adding a default context to every event emitted.
     * You may also want to construct an instance of this class by passing the context
     * parameter to the constructor. For further information have a look at {@link EventClient.DefaultConfig}.
     */
    contextify(context)
    {
        this.config.context = context || { };
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
        return typeof this.subscriptions[topic] !== 'undefined';
    }

    async _connect()
    {
        if(this.connection)
            return this.connection;

        try
        {
            const isConsulOverride = this.config.consulOverride && this.config.consulOverride.host && true;
            let props;

            if(isConsulOverride)
            {
                const config = this.config.consulOverride;

                props = {
                    endpoint : {
                        host : config.host,
                        port : config.port
                    },
                    username : config.username,
                    password : config.password
                };
            }
            else
            {
                const config = this.config.consul;
                const consul = await configService.init({ retryCount : 60, retryTimeout : 500 });

                props = await Promise.props({
                    endpoint : consul.getEndPoint(config.mqServiceName),
                    username : config.mqUserKey && consul.get(config.mqUserKey),
                    password : config.mqPasswordKey && consul.get(config.mqPasswordKey)
                });
            }

            this.connection = await retry(() =>
            {
                return amqp.connect({
                    protocol : 'amqp',
                    hostname : props.endpoint.host,
                    port : props.endpoint.port,
                    username : props.username,
                    password : props.password,
                    heartbeat : 60
                });
            }, { max_tries: 60, interval: 500, timeout : 120000, backoff : 1.5 });

            this.connection.on('error', err => this.logger.warn('Error on connection.', err));
            this.connection.on('blocked', err => this.logger.warn('Blocked connection.', err));
            this.connection.on('unblocked', () => this.logger.warn('Unblocked connection.'));
            this.connection.on('close', () => this.logger.warn('Closed connection.'));

            return this.connection;
        }
        catch(e)
        {
            this.logger.error('Could not connect to amqp.', e);
            throw e;
        }
    }

    async _getNewChannel(onError = () => null)
    {
        const connection = await this._connect();
        const channel = await connection.createChannel();

        channel.on('error', (err) => { this.logger.error(`A channel has been unexpectedly closed: ${err}`); onError(err); });

        return channel;
    }

    async _createExchange(exchangeName, channel)
    {
        return channel.assertExchange(exchangeName, 'topic', { durable: true, autoDelete: false });
    }

    async _getPubChannel()
    {
        if(!this.pubChannel)
        {
            const channel = await this._getNewChannel();
            await this._createExchange(this.exchangeName, channel);

            this.pubChannel = channel;
        }

        return this.pubChannel;
    }

    async _registerConsumner(channel, exchangeName, queueName, topic, callback)
    {
        await channel.assertQueue(queueName, { durable: true, autoDelete: false });
        await retry(() => channel.bindQueue(queueName, exchangeName, topic), { max_tries : 60, interval : 500, timeout : 120000, backoff : 1.5 });

        return await channel.consume(queueName, async message =>
        {
            try
            {
                const result = await Promise.resolve(callback(message));
                await (result === false ? channel.nack(message) : channel.ack(message));
            }
            catch(e)
            {
                this.logger.warn(e);

                try
                {
                    await channel.nack(message);
                }
                catch(e)
                {}
            }
        });
    }

    _addCallback(topic, callback)
    {
        this.callbacks[topic] = callback;

        const keys = Object.keys(this.callbacks).sort().reverse();
        const temp = { };

        keys.forEach(k => temp[k] = this.callbacks[k]);
        this.callbacks = temp;
    }

    _findCallbackKey(topic)
    {
        if(this.callbacks[topic])
            return topic

        for(const key in this.callbacks)
        {
            const exp = new RegExp('^' + key.replace('*', '[^\.]+$').replace('#', '([^\.]+)+$'));

            if(topic.match(exp))
                return key;
        }
    }

    _findCallback(topic)
    {
        const key = this._findCallbackKey(topic);
        return key && this.callbacks[key];
    }

    _incrementCallbackErrors(topic)
    {
        if(this.callbackErrorCount[topic])
            this.callbackErrorCount[topic]++;
        else
            this.callbackErrorCount[topic] = 1;
    }
}

/**
* Static object representing a default configuration set.
*
* @property {object} serializer - Function to use for serializing messages in order to send them.
* @property {object} parser - Function to use for deserializing messages received.
* @property {string} serializerContentType - Content type of the serialized message added as a meta data field to each event emitted.
* @property {string} parserContentType - Content type for which events should be received and parsed using the configured parser.
* @property {string} queueName - Name of the queue to connect to. By default this is the service name as of [@opuscapita/config](https://github.com/OpusCapita/config/wiki#serviceName).
* @property {string} exchangeName - The name of the exchnage to emit events to. By default this is the name of the service as from [@opuscapita/config](https://github.com/OpusCapita/config/wiki#serviceName).
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
    serializer : JSON.stringify,
    parser : JSON.parse,
    serializerContentType : 'application/json',
    parserContentType : 'application/json',
    queueName : null,
    exchangeName : null,
    consul : {
        host : 'consul',
        mqServiceName  : 'rabbitmq-amqp',
        mqUserKey: 'mq/user',
        mqPasswordKey : 'mq/password'
    },
    consulOverride : {
        host : null,
        port : null,
        username : null,
        password : null
    },
    context : {
    }
}

module.exports = EventClient;
