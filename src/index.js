'use strict'

const extend = require('extend');
const configService = require('ocbesbn-config');
const Promise = require('bluebird');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const AMQPRequeue = require('amqplib-retry')
const Logger = require('ocbesbn-logger');

/**
 * Module simplifying access to the publish/subscribe system provided by Redis.
 * @requires amqp
 * @requires extend
 * @requires ocbesbn-config
 * @requires bluebird
 */

/**
 * event clients, helps in connecting to external AMQP tool and
 * trigger an event or subscribe to an event
 */

var EventClient = function(config)
{
    this.config = extend(true, { }, EventClient.DefaultConfig, config);
    this.subscriber = null;
    this.emitter = null;
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
                endpoint : consul.getEndPoint(config.consul.AMQPServiceName),
                password : config.consul.AMQPPasswordKey && consul.get(config.consul.AMQPPasswordKey),
                username: config.consul.AMQPUserKey && consul.get(config.consul.AMQPUserKey)
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
        .then((AMQPConn) =>
        {
            logger.info(`Connection established..`);
            return AMQPConn.createChannel();
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
 * @return {Boolean}
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
            this.logger.info(`Emitted event with key '${key}' and message %j`, message);
            return Promise.resolve();
        }

        this.logger.warn(`Failed to Emit event with key '${key}' and message %j`, message);
        return Promise.reject(new Error('Failed to Emit to Queue'));
    }

    if (!this.channel)
    {
        return this._getNewChannel(this.config)
        .then((AMQPChannel) =>
        {
            this.logger.info(`AMQP connection established`);
            this.channel = AMQPChannel;
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
 * This method helps to acknowledge the message
 * @param {Object} message - the recieved message
 */
EventClient.prototype.ack = function(message)
{
    const acknowledge = () =>
    {
        return this.channel.ack(message)
    }

    if (!this.channel)
    {
        return _getNewChannel(this.config)
        .then((AMQPChannel) =>
        {
            this.channel = AMQPChannel;
            return acknowledge();
        })
    }

    return acknowledge();
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
    const messageCallback = (msg, rawMsg) =>
    {
        var result = callback(msg, rawMsg);

        if (!noAck && (typeof result == 'undefined' || !result.then))
        {
            return Promise.reject(new Error(`No return statement in callback to acknowledge message for key ${key}`));
        }

        return result;
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
            this.logger.info(`Subscribed to Key '${key}' and queue '${this.config.queueName}'`);

            this.channel.consume(this.config.queueName, AMQPRequeue({
                channel: this.channel,
                consumerQueue: this.config.queueName,
                failureQueue: this.config.queueName,
                delay: (attempt) => {
                    this.logger.info(`Adding to failure queue '${key}'`);
                    return attempt * 1000;
                },
                handler: (msg) => {
                    let message = this.config.parser(msg.content.toString());
                    this.logger.info(`Recieved message %j for key '${key}'`, message, msg);
                    return messageCallback(message, msg);
                }
            }));

            return Promise.resolve();
        })
    }

    if (!this.channel)
    {
        return this._getNewChannel()
        .then((AMQPChannel) =>
        {
            this.channel = AMQPChannel;
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
 * Static object represents the event client
 *
 * @property {Function} serializer - An function takes the message in any form, but serialize the message
 * @property {Function} parser - An function takes the serialized message and parse them to a readable form
 * @property {String} queueName - name of the queue, default to serviceName
 * @property {object} consul - Object for configuring consul related parameters.
 * @property {String} consul.host - Hostname of a consul server.
 * @property {String} consul.AMQPServiceName - Name of the enpoint for the AMQP tool in consul.
 * @property {String} consul.AMQPPasswordKey - Consul configuration key for AMQP tool authorisation. Might be null or false if not desired to be used.
 * @property {object} context - Optional context object to automatically extend emitted messages.
 */
EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    queueName: configService.serviceName,
    consul : {
        host : 'consul',
        AMQPServiceName  : 'amqp',
        AMQPUserKey: 'amqp/user',
        AMQPPasswordKey : 'amqp/password'
    },
    context : {
    }
}

module.exports = EventClient;
