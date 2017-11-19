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
    this.subChannel = null;
    this.pubChannel = null;
    this.exchangeName = 'Service_Client_Exchange';
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
                    password: props.password,
                    heartbeat: 60
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
            return Promise.all([ch, ch.assertExchange(exchangeName, 'topic', {durable: true})]);
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

        const emitted = this.pubChannel.publish(this.exchangeName, key, Buffer.from(messageString), {mandatory: true, persistent: true});

        if (emitted)
        {
            this.logger.info(`Emitted event with key '${key}' and message %j`, message);
            return Promise.resolve();
        }

        this.logger.warn(`Failed to Emit event with key '${key}' and message %j`, message);
        return Promise.reject(new Error('Failed to Emit to Queue'));
    }

    if (!this.pubChannel)
    {
        return this._getNewChannel(this.config)
        .then((mqChannel) =>
        {
            this.logger.info(`mq connection established`);
            this.pubChannel = mqChannel;

            // testing
            this.pubChannel.on('error', (err) =>
            {
                console.log('---->Pub Channel Error', err);
            });

            this.pubChannel.on('return', (msg) =>
            {
                console.log('---->Pub Channel return', msg);
            });

            this.pubChannel.on('close', () =>
            {
                console.log('---->Pub Channel close');
            });

            this.pubChannel.on('drain', () =>
            {
                console.log('---->Pub Channel drain');
            });
            // testing

            return emitEvent();
        })
        .catch((err) =>
        {
            this.logger.error(err);
        })
    }

    return emitEvent();
}

EventClient.prototype.reQueue = function(key, msg)
{
    setTimeout(() =>
    {
        this.logger.info('Requeuing message %j for key %s', msg, key)
        this.emit(key, msg);
    }, 1000);
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
            this.logger.warn(`Failed on acknowledgement by consumer function for key %s`, key);
            this.reQueue(key, msg);
        }
        else if (result && result.catch)
        {
            result.catch((err) =>
            {
                this.logger.warn(`Failed on acknowledgement by consumer function for key %s with error %j`, key, err);
                this.reQueue(key, msg);
            });
        }
    }

    // testing
    const testQueue = () =>
    {
        // return Promise.resolve();
        const testQueueName = "testQueue"
        return this.subChannel.assertQueue('testQueue', {durable: true})
        .then(() =>
        {
            return this.subChannel.bindQueue(testQueueName, this.exchangeName, key)
        })
        .then(() =>
        {
            return this.subChannel.consume(testQueueName, (msg) =>
            {
                let message = this.config.parser(msg.content.toString());
                this.logger.info(`***TESTRecieved message %j for key '${msg.fields.routingKey}' ${!noAck ? "which requires ack" : "which doesn't require ack"}`, message, msg);
            })
        })
        .then((consumer) =>
        {
            this.logger.info(`Subscribed to Key '${key}' and queue '${testQueueName}'`, consumer);
        })
        .catch((err) =>
        {
            this.logger.warn(err);
        })
    }
    // testing

    const bindQueue = () =>
    {
        return this.subChannel.assertQueue(this.config.queueName, {durable: true})
        .then(() =>
        {
            return this.subChannel.bindQueue(this.config.queueName, this.exchangeName, key)
        })
        .then(() =>
        {
            return this.subChannel.consume(this.config.queueName, (msg) =>
            {
                let message = this.config.parser(msg.content.toString());
                this.logger.info(`Recieved message %j for key '${msg.fields.routingKey}' ${!noAck ? "which requires ack" : "which doesn't require ack"}`, message, msg);
                try
                {
                    messageCallback(message, msg);
                }
                catch(e)
                {
                    this.logger.warn(err);
                }
            }, {noAck: true});
        })
        .then((consumer) =>
        {
            this.logger.info(`Subscribed to Key '${key}' and queue '${this.config.queueName}'`, consumer);
        });
    }

    if (!this.subChannel)
    {
        return this._getNewChannel()
        .then((mqChannel) =>
        {
            this.subChannel = mqChannel;

            // testing
            this.subChannel.on('error', (err) =>
            {
                console.log('---->Sub Channel Error', err);
            });

            this.subChannel.on('return', (msg) =>
            {
                console.log('---->Sub Channel return', msg);
            });

            this.subChannel.on('close', () =>
            {
                console.log('---->Sub Channel close');
            });

            this.subChannel.on('drain', () =>
            {
                console.log('---->Sub Channel drain');
            });
            // testing

            return Promise.all([testQueue(), bindQueue()]);
        });
    }

    return Promise.all([testQueue(), bindQueue()]);
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
        this.subChannel.unbindQueue(this.config.queueName, this.exchangeName, key)
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
    if (this.subChannel)
    {
        return this.subChannel.close()
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
