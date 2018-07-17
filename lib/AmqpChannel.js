const crypto = require('crypto');

const Logger = require('ocbesbn-logger');
const EventError = require('./EventError');
const EventEmitter = require('events');
const retry = require('bluebird-retry');
const superagent = require('superagent');

const overallConsumers = new Map();

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
        this.consumers = new Map(); // TODO As every subscriber gets its own channel -> why use a Map instead of saving the single object?

        this.state = AmqpChannel.CS_NONE;
        this.useWaitQueue = false;

        this.unackedMsgs = new Map();
        this.waitQueue   = new Map();

        if (this.connection && this.connection.connectionState >= 1 && this.connection.connectionState <= 4) {
            this.initAmqpChannel();
        }

        this._registerConnectionListeners();
    }

    /* Channel states */
    static get CS_NONE()  { return 0; }
    static get CS_READY() { return 1; }
    static get CS_ERROR() { return 2; }

    /* Message states */
    static get MSG_CREATED() { return 0; }
    static get MSG_SENT()    { return 1; }
    static get MSG_NACKED()  { return 2; }
    static get MSG_BLOCKED() { return 3; }
    static get MSG_FAILED()  { return 4; }

    async initAmqpChannel()
    {
        try {
            this.channel = await this.getNewChannel();
            this.state = AmqpChannel.CS_READY;

            this.events.emit('channel_ready');
        } catch (e) {
            this.logger.error('AmqpChannel: failed to create a new channel after connected.');
            this.state = AmqpChannel.CS_ERROR;
        }
    }

    async close()
    {
        this.logger.info('Closing channel...');

        if (this.channel) {
            await this.clearConsumers().catch(e => this.logger.error(e));
            await this.channel.close().catch(e => this.logger.error(e));
        }

        this.consumers.clear();

        this.channel = null;
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

        // TODO what to do when this fails?
        await this.createAndBindQueue({queueName, exchangeName, topic});

        this.logger.info(`Trying to consume channel on queue "${queueName}" for topic "${topic}".`);

        const consumer = await channel.consume(queueName, async (message) =>
        {
            try
            {
                if (message) {
                    await callback(message);
                    await channel.ack(message);
                } else if (message === null) {
                    // Queue deleted
                    //
                    // As the the information on the MQ node will be lost after queue deletion
                    // we can not use _registerMissingConsumers to handle this error -> do it manually

                    this.logger.warn('Received an empty message. Reregistering consumer ...');

                    this.consumers.delete(topic);
                    overallConsumers.delete(topic);

                    this.state = AmqpChannel.CS_ERROR;

                    // Using setImmediate to avoid stack overflow of recursive call
                    setImmediate(this.registerConsumer.bind(this, {exchangeName, queueName, topic, callback, prefetchCount}));
                } else {
                    this.logger.warn('Received an empty message. Not implemented!');
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

        this.consumers.set(topic, {
            consumerTag: consumer.consumerTag,
            exchangeName,
            queueName,
            topic,
            callback,
            prefetchCount
        });

        overallConsumers.set(topic, true);
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

        const channel = await this.getChannel();
        const consumer = this.consumers.get(topic);

        if (channel && consumer)
        {
            await this.channel.cancel(consumer.consumerTag).catch((e) => this.logger.error(e));

            this.consumers.delete(topic);
            overallConsumers.delete(topic);

            return true;
        }

        return false;
    }

    /**
     * @function publish
     * @async
     *
     * Delivers a message to the given topic. Depending on the channel configuration it will use
     * switch between normal and confirmation mode for the delivery itself. In both cases, messages
     * are cached in memory until delivered.
     *
     * Possible critical scenarios:
     *   * Connection closed
     *      -> Channel is deleted, callers will queue up on awaiting on this.getChannel()
     *   * Queue deleted
     *      -> Channel tries to create and rebind queue
     *          TODO error handling
     *   * Connection blocked
     *     -> New messages are sent to the socket buffer and will be delevired as soon as the
     *        connection is unblocked
     *     -> Messages are put into the cache
     *
     *
     * @param {object} config
     * @param {string} config.exchangeName
     * @param {string} config.topic
     * @param {Buffer} config.messageBuffer
     * @param {object} config.options
     * @param {Integer} config.options.ttl - Time in miliseconds we will wait for message delivery before it gets discarded.
     *
     * @returns {Promise}
     */
    async publish(config)
    {
        let result = null;

        if (this.useWaitQueue) {
            // Channel is blocked -> do not send, just put to waitQueue for later delivery
            result = this.doPublishWait(config);
        } else {
            // Channel is ready -> send
            result = this.doPublish(config);
        }

        return result;
    }

    doPublishWait(message) {
        let {exchangeName, topic, messageBuffer, options} = message;

        /*
         * Place the message into cache before awaiting on the channel to connect.
         * In case the connection is reconnecting the call to this method will only return
         * after the channel was created (await #getChannel).
         */
        let cachedMsg = this._createCacheEntry(exchangeName, topic, messageBuffer, options);
        let [msgKey, cacheEntry] = cachedMsg;
        this.waitQueue.set(msgKey, cacheEntry);

        return true;
    }

    async doPublish(message) {
        let result = null;
        let {exchangeName, topic, messageBuffer, options} = message;

        /*
         * Place the message into cache before awaiting on the channel to connect.
         * In case the connection is reconnecting the call to this method will only return
         * after the channel was created (await #getChannel).
         */
        let cachedMsg = this._createCacheEntry(exchangeName, topic, messageBuffer, options);
        let [msgKey, cacheEntry] = cachedMsg;
        this.unackedMsgs.set(msgKey, cacheEntry);

        if (this.config && this.config.useConfirmChannel) {
            result = this.doPublishConfirm(message, cachedMsg);

            /*
             * Update the state to 'sent' - this does not necessarily mean they have been sent to the MQ
             * but they are in the Socket Buffer of the amqplib connection.
             */
            this.unackedMsgs.set(msgKey, {...this.unackedMsgs.get(msgKey), state: AmqpChannel.MSG_SENT});
        } else {
            /*
             * In case the connection is blocked messages will pile up in the socket buffer but will
             * be lost on channel close as they removed from cache as soon as the channel.publish() call
             * returns.
             */

            this.unackedMsgs.delete(msgKey);

            result = this.doPublishNoConfirm;
        }

        return result;
    }

    async doPublishConfirm(message, [msgKey, cacheEntry]) {
        let {exchangeName, topic, messageBuffer, options} = message;

        let deliveryTimeout = setTimeout(async () => {
            await this.logger.error(
                `AmqpChannel: Failed to deliver message in ${cacheEntry.ttl}ms.`,
                Buffer.from(JSON.stringify(cacheEntry)).toString('base64')
            );

            this.unackedMsgs.delete(msgKey);
        }, cacheEntry.ttl);

        return new Promise(async (resolve, reject) => {
            /*
             * This will create an event listener for every emitted message on the 'channel_ready' event and
             * might cause a problem when we are trying to send a lot of messages.
             * https://nodejs.org/api/events.html#events_eventemitter_defaultmaxlisteners
             *
             * TODO Maybe it would be better to queue the messages and send them as soon as the channel is back.
             */
            let channel = await this.getChannel();

            if (this.unackedMsgs.has(msgKey) === false) {
                reject(false);
            } else {
                channel.publish(exchangeName, topic, messageBuffer, options, (err) => {
                    /* Confirm callback to be able to access msg in the closure. */

                    clearTimeout(deliveryTimeout);

                    if (err) {
                        this.logger.error(
                            'AmqpChannel: message got n-acked by message broker.',
                            Buffer.from(JSON.stringify(cacheEntry)).toString('base64')
                        );

                        /* Update the message state to 'failed' */
                        if (this.unackedMsgs.has(msgKey)) {
                            this.unackedMsgs.set(msgKey, {...this.unackedMsgs.get(msgKey), state: AmqpChannel.MSG_NACKED});
                        }

                        // TODO expose failed message through EventClient to the application?
                        // TODO cleanup failed messages

                        reject(err);
                    } else {
                        this.unackedMsgs.delete(msgKey);
                        resolve(true);
                    }
                });
            }
        }).catch((e) => {
            this.unackedMsgs.has(msgKey) && this.unackedMsgs.set(msgKey, {...this.unackedMsgs.get(msgKey), state: AmqpChannel.MSG_FAILED}); // Update the state to 'failed'
            this.logger.error(e);
        });

    }

    async doPublishNoConfirm({exchangeName, topic, messageBuffer, options}) {
        let channel = await this.getChannel();
        return channel.publish(exchangeName, topic, messageBuffer, options);
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
        return overallConsumers.has(topic);
    }

    hasConsumer(topic)
    {
        return this.consumers.has(topic);
    }

    async clearConsumers()
    {
        this.logger.info('Trying to clear consumers...');

        await Promise.all([...this.consumers.keys()].map((topic) => this.removeConsumer(topic)));

        this.consumers.clear();

        this.logger.info('Consumers cleared.');
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
            this.state = AmqpChannel.CS_ERROR;
        });

        channel.on('close', err => {
            this.logger.info('A channel has been closed.', err || '');

            // Not using AmqpChannel#close() here because this would also clear the subscriptions.
            this.channel = null;
            this.state = AmqpChannel.CS_ERROR;
        });

        channel.on('drain', () => {
            this.logger.info('Channel received drain event.');
            // TODO resend messages from the unackedMsgs queue
        });

        this.logger.info('Channel created.');

        return channel;
    }

    async flushWaitQueue() {
        if (this.useWaitQueue) {
            this.logger.warn('AmqpChannel: Tried to flush waitQueue on a blocked connection. Trying to write pending messages to logstas.');

            for (const v of this.waitQueue.values()) {
                this.logger.error(
                    'AmqpChannel: Saving message to logstash - ',
                    Buffer.from(JSON.stringify(v)).toString('base64')
                );
            }
        } else {
            if (this.waitQueue > 0) {
                this.logger.info('AmqpChannel: Flushing waitQueue.');
            }

            for (const [k, v] of this.waitQueue.entries()) {
                try {
                    let result = await this.publish(v);

                    if (result === true) {
                        this.waitQueue.delete(k);
                    } else {
                        throw new Error('Failed to publish message from waitQueue.');
                    }
                } catch (e) {
                    this.logger.error(
                        'AmqpChannel: Failed to deliver message - ',
                        Buffer.from(JSON.stringify(v)).toString('base64')
                    );
                }
            }
        }


        return true;
    }

    async _registerMissingConsumers()
    {
        let result = false;
        const consumers = [...this.consumers.values()];

        const activeTags = [].concat(...(await Promise.all(consumers.map(c => this._getQueueData(c.queueName).then((res) => res.consumer_details.map(d => d.consumer_tag)).catch(e => { this.logger.error(e); return []; })))));
        const missingConsumers = consumers.filter(c => !activeTags.includes(c.consumerTag));

        if (missingConsumers.length > 0)
        {
            this.logger.info('Registering missing consumers...');
            this.logger.info('Active consumers "%j".', activeTags);
            this.logger.info('Missing consumers "%j".', missingConsumers);
        }

        result = await Promise.all(missingConsumers.map(async (c) =>
        {
            this.logger.info('Registering "%s"...', c.topic);

            try {
                await this.removeConsumer(c.topic);
            } catch (e) {
                this.logger.error(e);
            }

            return this.registerConsumer(c).catch(console.log);
        }));

        if (missingConsumers.length > 0) {
            this.logger.info('Missing consumers registration done.');
        }

        return result;
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
        this.connection.events.on('connection_blocked', () => {
            this.useWaitQueue = true;
        });

        this.connection.events.on('connection_unblocked', () => {
            this.useWaitQueue = false;
            this.flushWaitQueue();
        });

        this.connection.events.on('error', () => {
            this.state = AmqpChannel.CS_ERROR;
        });

        this.connection.events.on('close', () => {
            // This will implicitly close this.channel, so we need to create a new
            // one after 'connected' event was received
            this.state = AmqpChannel.CS_ERROR;
        });

        this.connection.events.on('connected', async () => {
            try {
                this.channel = await this.getNewChannel();
                this.state = AmqpChannel.CS_READY;

                this.events.emit('channel_ready');
            } catch (e) {
                this.logger.error('AmqpChannel: failed to create a new channel after connected.');
                this.logger.error(e);

                this.state = AmqpChannel.CS_ERROR;
            }

            await this._registerMissingConsumers();
        });
    }

    _createCacheEntry(exchangeName, topic, messageBuffer, options, state = AmqpChannel.MSG_CREATED)
    {
        let now = Date.now;

        const hmac = crypto.createHmac('sha256', Buffer.from(now));
        hmac.update(messageBuffer);

        let msgKey = hmac.digest('base64');
        let cacheEntry = {
            ttl: (options && options.ttl) || 30000,
            retryCnt: 0,
            state,
            exchangeName,
            topic,
            messageBuffer,
            options
        };

        return [msgKey, cacheEntry];
    }
}

module.exports = AmqpChannel;
