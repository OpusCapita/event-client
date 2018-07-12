const crypto = require('crypto');

const Logger = require('ocbesbn-logger');
const EventError = require('./EventError');
const EventEmitter = require('events');
const retry = require('bluebird-retry');
const superagent = require('superagent');

const overallConsumers = { };

const STATES = {
    NONE: 0,
    READY: 1,
    ERROR: 2
};

class AmqpChannel
{
    constructor(connection, logger, connectionConfig, config)
    {
        this.connection = connection;
        this.logger = logger || new Logger();
        this.connectionConfig = connectionConfig;
        this.config = config || {};

        this.channel = null;
        this.events = new EventEmitter();
        this.consumers = {};

        this.state = STATES.NONE;

        this.unackedMsgs = new Set();

        if (this.connection && this.connection.connectionState >= 1 && this.connection.connectionState <= 4) {
            // TODO this still smells like a race condition.
            this.initAmqpChannel();
        }

        this._registerConnectionListeners();
    }

    async initAmqpChannel()
    {
        try {
            this.channel = await this.getNewChannel();
            this.state = STATES.READY;

            this.events.emit('channel_ready');
        } catch (e) {
            this.logger.error('AmqpChannel: failed to create a new channel after connected.');
            this.state = STATES.ERROR;
        }
    }

    async close()
    {
        this.logger.info('Closing channel...');

        clearInterval(this.missingConsumersWorker);

        if (this.channel) {
            await this.clearConsumers().catch(e => this.logger.error(e));
            await this.channel.close().catch(e => this.logger.error(e));
        }

        this.channel = null;
        this.consumers = { };
    }

    async registerConsumer({exchangeName, queueName, topic, callback, prefetchCount})
    {
        if (this.consumerExists(topic)) {
            throw new EventError(`The topic "${topic}" has already been registered.`, 409);
        }

        const channel = await this.getChannel();

        if (prefetchCount) {
            channel.prefetch(prefetchCount, false);
        }

        await this.createAndBindQueue({queueName, exchangeName, topic});

        this.logger.info(`Trying to consume channel on queue "${queueName}" for topic "${topic}".`);

        const consumer = await channel.consume(queueName, async (message) =>
        {
            try
            {
                if (message) {
                    await callback(message);
                    await channel.ack(message);
                } else {
                    // TODO when message = null -> queue was deleted -> try to rebind
                    this.logger.warn('Received an empty message. Sending consumer to retry list...');
                }
            } catch (e) {
                const requeue = e.code !== 500;

                if (requeue) {
                    this.logger.warn(`An error occured while processing an incomming event on queue "${queueName}".`, e);
                } else {
                    this.logger.warn(`Sending event from queue "${queueName}" to dead queue.`, e);
                }

                if (message) {
                    await channel.nack(message, false, requeue);
                }
            }
        });

        if (!consumer) {
            this.logger.warn('Could not register consumer. Empty result.');
            throw new Error('Could not register consumer.');
        }

        this.consumers[topic] = {
            consumerTag: consumer.consumerTag,
            exchangeName,
            queueName,
            topic,
            callback,
            prefetchCount
        };

        overallConsumers[topic] = true;
    }

    async getMessage(queueName, autoAck)
    {
        const channel = await this.getChannel();
        return channel.get(queueName, {noAck: autoAck});
    }

    async ackMessage(message)
    {
        const channel = await this.getChannel();
        await channel.ack(message);
    }

    async nackMessage(message)
    {
        const channel = await this.getChannel();
        await channel.nack(message);
    }

    async createAndBindQueue({queueName, exchangeName, topic})
    {
        const channel = await this.getChannel();

        await channel.assertQueue(queueName, {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-dead-letter-exchange': `${exchangeName}.dead`
            }
        });

        await retry(() => channel.bindQueue(queueName, exchangeName, topic), {
            'max_tries': 10000,
            interval: 500,
            timeout: 120000,
            backoff: 1.2
        });
    }

    async removeConsumer(topic)
    {
        this.logger.info(`Trying to remove consumer for topic "${topic}."`);

        const consumer = this.consumers[topic];

        if (consumer)
        {
            this.channel && await this.channel.cancel(consumer.consumerTag).catch(() => null);

            delete this.consumers[topic];
            delete overallConsumers[topic];

            return true;
        }

        return false;
    }

    async publish({exchangeName, topic, messageBuffer, options})
    {
        try
        {
            let channel = await this.getChannel();
            let result = null;

            const hash = crypto.createHash('sha256');
            hash.update(messageBuffer);

            let cacheEntry = {
                key: hash.digest('base64'),
                ttl: null,
                msg: messageBuffer
            };

            if (this.config && this.config.useConfirmChannel) {
                this.unackedMsgs.add(messageBuffer);

                result = channel.publish(exchangeName, topic, messageBuffer, options, (err) => {
                    if (err) {
                        this.logger.error('AmqpChannel: message got n-acked by rabbitmq.');
                    } else {
                        this.unackedMsgs.has(cacheEntry) && this.unackedMsgs.delete(cacheEntry);
                    }
                });
            } else {
                result = channel.publish(exchangeName, topic, messageBuffer, options);
            }

            return result;
        }
        catch (channelClosedException)
        {
            // Register eventlistener to the channel_ready event so we can resend the message
            // once connected again.
            return new Promise((resolve, reject) => {
                this.events.once('channel_ready', async () => {
                    try
                    {
                        let channel = await this.getChannel();
                        let result = channel.publish(exchangeName, topic, messageBuffer, options);

                        resolve(result);
                    }
                    catch (e)
                    {
                        this.logger.error('Could not publish event: ', e, {exchangeName, topic, messageBuffer, options});
                        reject(e);
                    }
                });
            });
        };
    }

    async createExchange(exchangeName, type = 'topic')
    {
        let channel = await this.getChannel();
        return await channel.assertExchange(exchangeName, type, {durable: true, autoDelete: false});
    }

    async createDeadExchange(exchangeName)
    {
        const channel = await this.getChannel();

        await this.createExchange(`${exchangeName}.dead`, 'fanout');
        await channel.assertQueue(`${exchangeName}.dead`, {durable: true, autoDelete: false});
        await channel.bindQueue(`${exchangeName}.dead`, `${exchangeName}.dead`, '');
    }

    async deleteQueue(queueName, opts = { })
    {
        opts = {
            ifUnused: opts.unusedOnly,
            ifEmpty: opts.emptyOnly
        };

        let channel;

        try
        {
            channel = await this.connection.createChannel();
            await channel.deleteQueue(queueName, opts);
        }
        finally
        {
            channel && channel.close().catch((e) => this.logger.error(e));
        }

        return true;
    }

    async exchangeExists(exchangeName)
    {
        let channel;

        try
        {
            channel = await this.connection.createChannel();
            await channel.checkExchange(exchangeName);
        }
        finally
        {
            channel && channel.close().catch((e) => this.logger.error(e));
        }

        return true;
    }

    async queueExists(queueName)
    {
        let channel;

        try
        {
            channel = await this.connection.createChannel();
            await channel.checkQueue(queueName);
        }
        catch (e)
        {
            this.logger.error(e);
            return false;
        }
        finally
        {
            channel && channel.close().catch(e => this.logger.error(e));
        }

        return true;
    }

    consumerExists(topic)
    {
        return overallConsumers[topic] === true;
    }

    hasConsumer(topic)
    {
        return this.consumers[topic] !== undefined;
    }

    async clearConsumers()
    {
        this.logger.info('Trying to clear consumers...');

        const consumers = {...this.consumers};
        await Promise.all(Object.keys(consumers).map(topic => this.removeConsumer(topic)));

        this.logger.info('Consumers cleared.');

        this.consumers = { };
    }

    async getChannel()
    {
        if (!this.channel) {
            return new Promise((resolve) => {
                this.events.once('channel_ready', async () => {
                    resolve(this.channel);
                });
            });
        }

        return this.channel;
    }

    async getNewChannel()
    {
        this.logger.info('Creating new channel...');

        let channel = null;

        if (this.config && this.config.useConfirmChannel) {
            channel = await this.connection.createConfirmChannel();
        } else {
            channel = await this.connection.createChannel();
        }

        channel.on('error', async err => {
            this.logger.error(`A channel has been unexpectedly closed: ${err}`);
            // TODO what to do?
            this.state = STATES.ERROR;
        });

        channel.on('close', err => {
            this.logger.info('A channel has been closed.', err || '');
            // TODO what to do?
        });

        channel.on('drain', () => {
            this.logger.info('Channel received drain event.');
            // TODO what to do?
        });

        this.logger.info('Channel created.');

        return channel;
    }

    async _registerMissingConsumers()
    {
        const consumers = Object.values(this.consumers);

        const activeTags = [].concat(...(await Promise.all(consumers.map(c => this._getQueueData(c.queueName).then(res => res.consumer_details.map(d => d.consumer_tag)).catch(e => { this.logger.error(e); return []; })))));
        const missingConsumers = consumers.filter(c => !activeTags.includes(c.consumerTag));

        if (missingConsumers.length > 0)
        {
            this.logger.info('Registering missing consumers...');
            this.logger.info('Active consumers "%j".', activeTags);
            this.logger.info('Missing consumers "%j".', missingConsumers);
        }

        await Promise.all(missingConsumers.map(async c =>
        {
            this.logger.info('Registering "%s"...', c.topic);

            delete this.consumers[c.consumerTag];
            delete overallConsumers[c.consumerTag];

            await this.removeConsumer(c.topic).catch(console.log);
            return this.registerConsumer(c).catch(console.log);
        }));

        if (missingConsumers.length > 0) {
            this.logger.info('Missing consumers registration done.');
        }
    }

    async _getQueueData(queueName)
    {
        const auth = `${this.connectionConfig.username}:${this.connectionConfig.password}`;
        const host = `${this.connectionConfig.host}:${this.connectionConfig.httpPort}`;
        const path = `/api/queues/%2F/${encodeURIComponent(queueName)}`;

        return superagent.get(`http://${auth}@${host}${path}`).then(res => res && res.body);
    }

    _registerConnectionListeners()
    {
        this.connection.events.on('error', () => {
            this.logger.warn('AmqpChannel: Error on connection.');
            this.state = STATES.ERROR;
        });

        this.connection.events.on('close', () => {
            // This will implicitly close this.channel, so we need to create a new
            // one after 'connected' event was received

            this.logger.warn('AmqpChannel: Connection closed.');
            this.state = STATES.ERROR;
        });

        this.connection.events.on('connected', async () => {
            this.logger.info('AmqpChannel: Connection connected successfully.');

            try {
                this.channel = await this.getNewChannel();
                this.state = STATES.READY;

                this.events.emit('channel_ready');
            } catch (e) {
                this.logger.error('AmqpChannel: failed to create a new channel after connected.');
                this.logger.error(e);
                this.state = STATES.ERROR;
            }


            // TODO Check if this.channel is ok before registering missing consumers
            // this.logger.info('Trying to re-register consumers...');
            await this._registerMissingConsumers();
        });
    }
}

module.exports = AmqpChannel;
