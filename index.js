const extend = require('extend');
const configService = require('ocbesbn-config');
const Promise = require('bluebird');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const Logger = require('ocbesbn-logger');
const crypto = require('crypto');

class EventClient
{
    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);
        this.connection = null;
        this.pubChannel = null;
        this.subChannels = { };
        this.subscriptions = { };
        this.callbacks = { };
        this.serviceName = configService.serviceName;
        this.exchangeName = this.config.exchangeName || this.serviceName;
        this.logger = new Logger({ context : { serviceName : this.serviceName } });
    }

    init()
    {
        return Promise.resolve(this._getNewChannel()).then(channel => channel.close()).then(() => true);
    }

    emit(topic, message, context = null)
    {
        const logger = new Logger({ context : { serviceName : this.serviceName } });

        if(!this.pubChannel)
            this.pubChannel = Promise.resolve(this._getNewChannel());

        return this.pubChannel.then(channel =>
        {
            const localContext = {
                senderService : this.serviceName,
                timestamp : new Date()
            };

            const transportObj = {
                topic : topic,
                context : extend(true, { }, this.config.context, context, localContext),
                payload : message
            };

            const messageId = `${this.serviceName}.${crypto.randomBytes(16).toString('hex')}`;

            const options = {
                persistent : true,
                contentType : this.config.serializerContentType,
                contentEncoding : 'utf-8',
                timestamp : Math.floor(Date.now() / 1000),
                correlationId : context && context.correlationId,
                appId : this.serviceName,
                messageId : messageId
            }

            logger.contextify(extend(true, { }, transportObj.context, options));
            logger.info(`Emitting event "${topic}"`);

            const messageBuffer = Buffer.from(this.config.serializer(transportObj));
            const result = channel.publish(this.exchangeName, topic, messageBuffer, options);

            if(result)
                return Promise.resolve();
            else
                return Promise.reject(new Error('Unkown error: Event could not be published.'));
        });
    }

    subscribe(topic, callback)
    {
        if(this.hasSubscription(topic))
            Promise.reject('The topic is already registered. A topic can only be registered once per instance.');

        const channel = Promise.resolve(this._getNewChannel());
        this.subChannels[topic] = channel;

        return channel.then(channel =>
        {
            this._addCallback(topic, callback);

            const exchangeName = topic.substr(0, topic.indexOf('.'));
            const queueName = this.serviceName;

            const consumer = this._registerConsumner(channel, exchangeName, queueName, topic, message =>
            {
                const routingKey = message.fields.routingKey;
                const callback = this._findCallback(routingKey);
                const logger = new Logger({ context : { serviceName : this.serviceName } });

                logger.info(`Receiving event for topic "${topic}"`);

                if(callback)
                {
                    if(message.properties.contentType === this.config.parserContentType)
                    {
                        const result = this.config.parser(message.content);

                        logger.contextify(extend(true, { }, result.context, message.properties));
                        logger.info(`Passing event "${result.topic}" to application.`);

                        return callback(result.payload, result.context, result.topic);
                    }
                    else
                    {
                        logger.error(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${message.contentType}.`);
                        throw new Error(`Cannot parse incoming message due to an incompatible content type. Expected: ${this.config.parserContentType} - Actual: ${message.contentType}.`);
                    }
                }
                else
                {
                    logger.info(`There is no subscriber for topic "${topic}"`);
                }
            });

            return Promise.resolve(consumer).then(result =>
            {
                this.subscriptions[topic] = result.consumerTag;
            });
        });
    }

    unsubscribe(topic)
    {
        const channel = this.subChannels[topic];
        const subscription = this.subscriptions[topic];

        if(channel && subscription)
        {
            return channel.then(channel =>
            {
                return channel.cancel(subscription).then(() =>
                {
                    delete this.subscriptions[topic];
                    delete this.callbacks[this._findCallbackKey(topic)];

                    return true;
                })
            });
        }

        return Promise.resolve(false);
    }

    disposePublisher()
    {
        if(this.pubChannel)
            return this.pubChannel.then(channel => channel.close()).then(() => this.pubChannel = null).then(() => true);

        return Promise.resolve(false);
    }

    disposeSubscriber()
    {
        const all = [ ];

        for(const key in this.subChannels)
            all.push(this.subChannels[key].then(channel => channel.close()));

        if(all.length)
            return Promise.all(all).then(() => { this.subChannels = { }; this.callbacks = { } }).then(() => true);

        return Promise.resolve(false);
    }

    contextify(context)
    {
        this.config.context = context || { };
    }

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

    async _getNewChannel()
    {
        const connection = await this._connect();
        const channel = await connection.createChannel();
        await channel.assertExchange(this.exchangeName, 'topic', { durable: true, autoDelete: false });

        return channel;
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
}

EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    serializerContentType : 'application/json',
    parserContentType : 'application/json',
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
