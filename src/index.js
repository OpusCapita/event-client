'use strict'

const extend = require('extend');
const configService = require('ocbesbn-config');
const Promise = require('bluebird');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const Logger = require('ocbesbn-logger');

/**
 * Module simplifying access to the publish/subscribe system provided by Redis.
 * @requires amqp
 * @requires extend
 * @requires ocbesbn-config
 * @requires bluebird
 */

/**
 * event clients, helps in connecting to external Message Queue tool and
 * trigger an event or subscribe to an event
 */

var EventClient = function(config)
{
    this.config = extend(true, { }, EventClient.DefaultConfig, config);
    this.subscribers = {};
    this.channel = null;
    this.exchangeName = 'Service_Client_Exchange';
    this.retryExchangeName = 'Retry_Service_Client_Exchange';
    this.logger = new Logger({ context : { serviceName : configService.serviceName } });

    /**
     * Private method to create exchange, channel
     * @return {Promise}
     */
    this._getNewChannel = function()
    {
        const config = this.config;
        const logger = this.logger;
        const exchangeName = this.exchangeName;
        const retryExchangeName = this.retryExchangeName;

        return configService.init({ host : config.consul.host }).then(consul =>
        {
            return Promise.props({
                endpoint : consul.getEndPoint(config.consul.mqServiceName),
                password : config.consul.mqPasswordKey && consul.get(config.consul.mqPasswordKey),
                username: config.consul.mqUserKey && consul.get(config.consul.mqUserKey)
            });
        })
        .then((props) =>
        {
            logger.info(`Recieved consul properties`)
            return retry(() => {
                return amqp.connect({
                    protocol: 'amqp',
                    hostname: props.endpoint.host,
                    port: props.endpoint.port,
                    username: props.username,
                    password: props.password
                })
            }, {max_tries: 15, interval: 500});
        })
        .then((mqConn) =>
        {
            logger.info(`Connection established..`);
            return mqConn.createChannel();
        })
        .then((ch) =>
        {
            logger.info(`Channel created..`);
            return Promise.all([ch, ch.assertExchange(exchangeName, 'topic'), ch.assertExchange(retryExchangeName, 'direct')])
        })
        .then(ch =>
        {
            logger.info(`Exchange '${exchangeName}' defined..`)
            return Promise.resolve(ch[0]);
        })
        .catch(err =>
        {
            logger.error('An error occured in Event client connection: %j', err.message);
            throw err;
        });
    }
}

/**
 * This method allows to emit an event/message
 * @param {String} key - routing key for the message
 * @param {Object} message - message to be communicated
 * @return {Promise}
 */
EventClient.prototype.emit = function(key, message)
{
    /**
     * This method will take care of emitting the message to the exchange
     * @return {Boolean}
     */
    const emitEvent = () =>
    {
        var messageString = '';

        if(typeof message === 'object' && this.config.context)
            messageString = this.config.serializer(extend(true, { }, this.config.context, message));
        else
            messageString = this.config.serializer(message);

        const emitted = this.channel.publish(this.exchangeName, key, Buffer.from(messageString));

        if (emitted)
        {
            this.logger.info(`Emitted event via Exchange ${this.exchangeName} and Key '${key}' and message %j`, message);
            return Promise.resolve();
        }

        this.logger.warn(`Failed to Emit event with key '${key}' and message %j`, message);
        return Promise.reject(new Error('Failed to Emit to Queue'));
    }

    if (!this.channel)
    {
        return this._getNewChannel(this.config)
        .then((mqChannel) =>
        {
            this.logger.info(`mq connection established`);
            this.channel = mqChannel;
            return emitEvent();
        })
        .catch((err) =>
        {
            this.logger.error(err);
        })
    }

    return emitEvent();
}

/**
 * This method helps to subscribe for an event using routing key
 * @param {Function} callback - callback function
 * @param {String} key - routing key for the message
 * @param {Boolean} noAck - routing key for the message
 * @return {Promise}
 */
EventClient.prototype.subscribe = function(callback, key, noAck)
{
    if (this.subscribers[key])
    {
        this.subscribers[key].push({callback: callback, noAck: noAck});
    }
    else
    {
        this.subscribers[key] = [].concat({callback: callback, noAck: noAck});
    }

    const reQueue = (key, msg) =>
    {
        setTimeout(() =>
        {
            this.emit(key, msg);
        }, 1000);

        this.logger.warn(`No return statement in callback to acknowledge message for key ${key}`);
    }

    const messageCallback = (msg, rawMsg) =>
    {
        let routingKey = key;

        if (this.subscribers[routingKey])
        {
            console.log('------->', routingKey, JSON.stringify(this.subscribers[routingKey]));

            for (let i = 0; i < this.subscribers[routingKey].length; i++)
            {
                let ack = this.subscribers[routingKey][i].noAck;
                let result = this.subscribers[routingKey][i].callback(msg, rawMsg);

                if (!ack && (typeof result == 'undefined' || !result.then))
                {
                    reQueue(key, msg);
                }
                else if (result && result.catch)
                {
                    result.catch(() =>
                    {
                        reQueue(key, msg);
                    });
                }
            }
        }
        else
        {
            this.logger.info(`No Registered callbacks for the provided key ${key}`);
        }
    }

    const bindQueue = () =>
    {
        return this.channel.assertQueue(this.config.queueName)
        .then(() =>
        {
            return this.channel.bindQueue(this.config.queueName, this.exchangeName, key)
        })
        .then(() =>
        {
            return this.channel.consume(this.config.queueName, (msg) =>
            {
                let message = this.config.parser(msg.content.toString());
                this.logger.info(`Recieved message %j for key '${key}' which doesn't require ack`, message, msg);
                return messageCallback(message, msg);
            }, {noAck: true});
        })
        .then(() =>
        {
            this.logger.info(`Subscribed to Key '${key}' and queue '${this.config.queueName}'`);
            return Promise.resolve();
        })
    }

    if (!this.channel)
    {
        return this._getNewChannel()
        .then((mqChannel) =>
        {
            this.channel = mqChannel;
            return bindQueue();
        })
    }

    return bindQueue();
}

/**
 * function to un-subscribe the routing key from queue
 * @param {String} key - name of the key
 * @return {Promise}
 */
EventClient.prototype.unsubscribe = function(key)
{
    this.subscribers[key] = [];

    return new Promise((resolve, reject) =>
    {
        this.channel.unbindQueue(this.config.queueName, this.exchangeName, key)
        .then(() =>
        {
            this.logger.info(`Successfully unsubscribed pattern/key '${key}' for queue '${this.config.queueName}'`);
            resolve();
        })
        .catch((err) =>
        {
            this.logger.warn(`Failed to unsubscribe pattern/key '${key}' for queue '${this.config.queueName}'`, err);
            reject(err);
        });
    })

}

/**
 * Allows adding to default context, helps in defining the additional information about the message
 * @param {Object} context
 */
EventClient.prototype.contextify = function(context)
{
    this.config.context = context || { };
}

/**
 * Allows to reset the channel and all its subscription lists
 */
EventClient.prototype.disposeSubscriber = function()
{
    this.subscribers = {};

    if (this.channel)
    {
        return this.channel.close()
        .then(() =>
        {
            this.logger.info('Disposed subscribed keys and channel connection...');
        })
        .catch((err) =>
        {
            this.logger.info('Failed to dispose subscribed keys and channel connection...', err);
        })
    }

    this.logger.info('No Channel found to dispose....');
    return Promise.resolve();
}

/**
 * Static object represents the event client
 *
 * @property {Function} serializer - An function takes the message in any form, but serialize the message
 * @property {Function} parser - An function takes the serialized message and parse them to a readable form
 * @property {String} queueName - name of the queue, default to serviceName
 * @property {object} consul - Object for configuring consul related parameters.
 * @property {String} consul.host - Hostname of a consul server.
 * @property {String} consul.mqServiceName - Name of the enpoint for the Message Queue tool in consul.
 * @property {String} consul.mqPasswordKey - Consul configuration key for Message Queue tool authorisation. Might be null or false if not desired to be used.
 * @property {object} context - Optional context object to automatically extend emitted messages.
 */
EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    queueName: configService.serviceName,
    consul : {
        host : 'consul',
        mqServiceName  : 'amqp',
        mqUserKey: 'mq/user',
        mqPasswordKey : 'mq/password'
    },
    context : {
    }
}

module.exports = EventClient;
