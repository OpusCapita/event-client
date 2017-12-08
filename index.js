'use strict'

const extend = require('extend');
const configService = require('ocbesbn-config');
const Promise = require('bluebird');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const Logger = require('ocbesbn-logger');

class EventClient
{
    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);
        this.pubChannel = null;
        this.subChannel = null;
        this.connection = null;
        this.subscriptions = { };
        this.exchangeName = 'Service_Client_Exchange';
        this.logger = new Logger({ context : { serviceName : configService.serviceName } });
    }

    emit(topic, message, context = null)
    {
        if(!this.pubChannel)
            this.pubChannel = Promise.resolve(this._getNewChannel());

        return this.pubChannel.then(channel =>
        {
            const transportObj = {
                topic : topic,
                context : extend(true, { }, this.config.context, context),
                payload : message
            };

            const messageBuffer = Buffer.from(this.config.serializer(transportObj));
            const result = channel.publish(this.exchangeName, topic, messageBuffer, { persistent: true });

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

        if(!this.subChannel)
            this.subChannel = Promise.resolve(this._getNewChannel());

        return this.subChannel.then(channel =>
        {
            const consumer = this._registerConsumner(channel, this.config.queueName, topic, result =>
            {
                return callback(result.payload, result.context, result.topic);
            });

            return Promise.resolve(consumer).then(result =>
            {
                this.subscriptions[topic] = result.consumerTag;
                return null;
            });
        });
    }

    unsubscribe(topic)
    {
        if(this.subChannel && this.subscriptions[topic])
        {
            return this.subChannel.then(channel =>
            {
                return channel.cancel(this.subscriptions[topic]).then(() =>
                {
                    delete this.subscriptions[topic];
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
        if(this.subChannel)
            return this.subChannel.then(channel => channel.close()).then(() => this.subChannel = null).then(() => true);

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
                const consul = await configService.init();
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
            }, { max_tries: 15, interval: 500 });

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

    async _registerConsumner(channel, queueName, topic, callback)
    {
        await channel.assertQueue(queueName, { durable: true, autoDelete: false });
        await channel.bindQueue(queueName, this.exchangeName, topic);

        return await channel.consume(queueName, async message =>
        {
            try
            {
                const result = await Promise.resolve(callback(this.config.parser(message.content.toString())));
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
}

EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    queueName: configService.serviceName,
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
