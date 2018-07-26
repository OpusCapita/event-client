const crypto = require('crypto');

const Logger = require('ocbesbn-logger');
const EventError = require('./EventError');
const EventEmitter = require('events');
const retry = require('bluebird-retry');
const superagent = require('superagent');

const overallConsumers = new Map();

class AmqpChannel
{
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

    /**
     * Create an AmqpChannel.
     *
     * Initializes the internal state.
     * If the given connection is active it will create a new channel.
     *
     * @param {AmqpConnection} connection
     * @param {Logger} logger
     * @param {object} connectionConfig - RabbitMQ connection config (eg. user, password, ...)
     * @param {object} config
     * @param {Boolean} config.useConfirmChannel - Indicates what kind of channel should be used for message delivery
     *
     */
    constructor(connection, logger, connectionConfig, config)
    {
        this.connection = connection;
        this.channel    = null;

        this.logger           = logger || new Logger();
        this.config           = config || {};
        this.connectionConfig = connectionConfig;

        this.unackedMsgs        = new Map();
        this.waitQueue          = new Map();
        this.confirmFailedQueue = new Map();

        this.consumers = new Map(); // TODO As every subscriber gets its own channel -> why use a Map instead of saving the single object?
        this.events    = new EventEmitter();

        this.state        = AmqpChannel.CS_NONE;
        this.useWaitQueue = false;

        if (this.connection && this.connection.connectionState >= 1 && this.connection.connectionState <= 4) {
            this.initAmqpChannel();
        }

        this._registerConnectionListeners();
    }

    async initAmqpChannel()
    {
        try {
            this.channel = await this.getNewChannel();
            this.state = AmqpChannel.CS_READY;

            this.events.emit('channel_ready');
        } catch (e) {
            this.logger.error('AmqpChannel#initAmqpChannel: failed to create a new channel after connected.');
            this.state = AmqpChannel.CS_ERROR;
        }
    }

    async close()
    {
        this.logger.info('Closing channel...');

        if (this.channel) {
            await this.clearConsumers()
                .catch(e => this.logger.error(e));

            await this.timeoutPromise(() => this.channel.close());
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
                    /*
                     * Queue deleted
                     * As the the information on the MQ node will be lost after queue deletion
                     * we can not use _registerMissingConsumers to handle this error -> do it manually
                     */

                    this.logger.warn('AmqpChannel#registerConsumer#consumeCallback: Received an empty message. Reregistering consumer ...');

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

        /* FIXME Do not do this on consumer only connections! */
        return await this.sendPing(this.channel)
            .catch(() => this.logger.warn('AmqpChannel#registerConsumer: Connection was established but got immediatly blocked.'));
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
            await this.timeoutPromise(() => this.channel.cancel(consumer.consumerTag), 1500)
                .catch((e) => this.logger.error('AmqpChannel#removeConsumer: Failed to remove consumer gracefully. ', e));

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
     *   * Connection gets BLOCKED than CLOSED than CONNECTED
     *     * Connection does not receive 'unblocked' event
     *
     * @param {object} message
     * @param {string} message.exchangeName
     * @param {string} message.topic
     * @param {Buffer} message.messageBuffer
     * @param {object} message.options
     * @param {Integer} message.options.ttl - Time in miliseconds we will wait for message delivery before it gets discarded.
     *
     * @returns {Promise}
     */
    async publish(message)
    {
        if (this.useWaitQueue) {
          /* Channel is blocked -> do not send, just put to waitQueue for later delivery */
            return this.doPublishWait(message);
        } else {
          /* Channel is ready -> send */
            return this.doPublish(message);
        }
    }

  /**
   * @function doPublishWait
   * @private
   *
   * Does not actually try to send the message. The message is put on hold in the local cache.
   *
   * @param {object} message
   * @param {string} message.exchangeName
   * @param {string} message.topic
   * @param {Buffer} message.messageBuffer
   * @param {object} message.options
   * @param {Integer} message.options.ttl - Time in miliseconds we will wait for message delivery before it gets discarded.
   *
   * @return {Boolean} Always returns true as we are only using the cache
   */
    doPublishWait(message) {
        let cachedMsg = this._createCacheEntry(message);
        let [msgKey, cacheEntry] = cachedMsg;
        this.waitQueue.set(msgKey, cacheEntry);

        return true;
    }

    /**
     * @function doPublish
     * @async
     * @private
     *
     * This method serves as a switch for channels using confirm
     * channels and those who do not. It receives a message and
     * calls the appropriate method.
     *
     * @param {object} message
     * @param {string} message.exchangeName
     * @param {string} message.topic
     * @param {Buffer} message.messageBuffer
     * @param {object} message.options
     * @param {Integer} message.options.ttl - Time in miliseconds we will wait for message delivery before it gets discarded.
     *
     * @returns {Promise} The resulting promise from the publish method.
     */
    async doPublish(message) {
        let result = null;

        /*
         * Place the message into cache before awaiting on the channel to connect.
         * In case the connection is reconnecting the call to this method will only return
         * after the channel was created (await #getChannel).
         */
        let cachedMsg = this._createCacheEntry(message);
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

            result = this.doPublishNoConfirm(message);
        }

        return result;
    }

    async doPublishConfirm(message, [msgKey, cacheEntry]) {
        let {exchangeName, topic, messageBuffer, options} = message;

        return new Promise(async (resolve, reject) => {
            this.waitingCounter = this.waitingCounter ? this.waitingCounter + 1 : 1;

            /**
             * @function
             * @async
             *
             * Timeout for message delivery. Used to resolv or reject the
             * enclosing Promise after the given timeout.
             *
             */
            let deliveryTimeoutCallback = async () => {
                this.unackedMsgs.delete(msgKey);

                if (this.useWaitQueue === true) {
                    /* If connection changed to blocked, move message to waitQueue */

                    this.waitQueue.set(msgKey, cacheEntry);
                    this.confirmFailedQueue.set(msgKey, cacheEntry);

                    this.logger.info(`AmqpChannel#doPublishConfirm: Moving message  from unackedMsgs to waitQueue. (id: ${message.options.messageId})`);

                    this.waitingCounter--;
                    resolve(true);
                } else {
                    this.confirmFailedQueue.set(msgKey, cacheEntry);

                    let msg = `AmqpChannel#doPublishConfirm: Failed to deliver message in ${cacheEntry.ttl}ms. (id: ${message.options.messageId})`;

                    await this.logger.error(
                        msg,
                        Buffer.from(JSON.stringify(cacheEntry)).toString('base64')
                    );

                    reject(msg);
                }
            };
            let deliveryTimeout = setTimeout(deliveryTimeoutCallback, cacheEntry.ttl);

            /**
             * Wait for the channel to be available
             *
             * This will create an event listener on the 'channel_ready' event for every emitted message and
             * might cause a problem when we are trying to send a lot of messages.
             * https://nodejs.org/api/events.html#events_eventemitter_defaultmaxlisteners
             *
             * TODO Maybe it would be better to queue the messages and send them as soon as the channel is back.
             */
            let channel = await this.getChannel();

            if (this.unackedMsgs.has(msgKey) === false) {
                this.logger.info(`AmqpChannel:doPublishConfirm:confirmCallback: message deleted from unackedMsgs -> rejecting (id: ${message.options.messageId})`);
                this.waitingCounter--;
                reject(false);
            } else {
                let result = channel.publish(exchangeName, topic, messageBuffer, {...options, mandatory: true}, (err) => {

                    /* Using the confirm callback to capture message, cache, etc. in the closure. */

                    clearTimeout(deliveryTimeout);
                    this.waitingCounter--;

                    if (err) {
                        this.logger.error(
                            `AmqpChannel:doPublishConfirm:confirmCallback message got n-acked by message broker. (id: ${message.options.messageId})`,
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
                        this.logger.info(`AmqpChannel:doPublishConfirm:confirmCallback: Got ACK from message broker  (id: ${message.options.messageId})`);

                        if (this.confirmFailedQueue.has(msgKey) && this.waitQueue.has(msgKey)) {
                            this.waitQueue.delete(msgKey);
                            this.confirmFailedQueue.delete(msgKey);

                            this.logger.warn(
                                `AmqpChannel#doPublishConfirm#confirmCallback: Possible double sending detected. (id: ${message.options.messageId})`,
                                Buffer.from(JSON.stringify(cacheEntry)).toString('base64')
                            );
                        }

                        this.unackedMsgs.delete(msgKey);
                        resolve(true);
                    }
                });

                if (result === false) {
                    // TODO Use waitQueue until drain event
                    // this.useWaitQueue = true;
                }
            }
        }).catch((e) => {
            this.waitingCounter--;

            this.unackedMsgs.has(msgKey) && this.unackedMsgs.set(msgKey, {...this.unackedMsgs.get(msgKey), state: AmqpChannel.MSG_FAILED}); // Update the state to 'failed'
            this.logger.error('AmqpChannel#publish: failed with (id: ${message.options.messageId}): ' + e);

            return false;
        });

    }

    /**
     * @function doPublishNoConfirm
     * @async
     * @private
     *
     * Publishes a message without waiting for confirmation from the broker.
     *
     * @returns {Promise} The result from amqplib's channel#publish method.
     *
     */
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

            // Not using AmqpChannel#close() here because this would also clear the subscriptions.
            this.channel = null;
            this.state = AmqpChannel.CS_ERROR;
        });

        channel.on('close', err => {
            this.logger.info('AmqpChannel#getNewChannel: onChannelClose Handler triggered ... a channel has been closed.', err || '');

            this.channel = null; // Not using AmqpChannel#close() here because this would also clear the subscriptions.

            this.waitingCounter = 0; // Pending messages have been deleted

            // TODO Trigger creation of new channel if not called by this#close()

            this.state = AmqpChannel.CS_ERROR;
        });

        channel.on('drain', this.onChannelDrain.bind(this));
        channel.on('return', this.onChannelReturn.bind(this));

        this.logger.info('Channel created.');

        return channel;
    }

    async flushWaitQueue() {
        if (this.useWaitQueue) {
            this.logger.warn('AmqpChannel#flushWaitQueue: Tried to flush waitQueue on a blocked connection. Trying to write pending messages to logstash.');

            for (const v of this.waitQueue.values()) {
                this.logger.error(
                    'AmqpChannel#flushWaitQueue: Saving message to logstash - ',
                    Buffer.from(JSON.stringify(v)).toString('base64')
                );
            }
        } else {
            let size = this.waitQueue.size;

            if (this.waitQueue.size > 0) {
                this.logger.info('AmqpChannel#flushWaitQueue: Flushing waitQueue.');
            }

            for (const [k, v] of this.waitQueue.entries()) {
                try {
                    let result = await this.publish(v);

                    if (result === true) {
                        this.waitQueue.delete(k);
                    } else {
                        throw new Error('AmqpChannel#flushWaitQueue: Failed to publish message from waitQueue.');
                    }
                } catch (e) {
                    this.logger.error(
                        'AmqpChannel#flushWaitQueue: Failed to deliver message - ',
                        Buffer.from(JSON.stringify(v)).toString('base64')
                    );
                }
            }

            this.events.emit('waitqueue_flushed', size);
        }

        return true;
    }

    /* *** Channel event handler *** */

    onChannelDrain() {
        this.logger.info('AmpChannel#onChannelDrain: Received drain event on channel #' + this.channel.ch);
        // TODO resend messages from the unackedMsgs queue
    }

    onChannelReturn(message) {
        if (message.fields.routingKey === 'ping') return;
        this.logger.error(
            'AmqpChannel#onChannelReturn: Failed to deliver message - routing failure',
            Buffer.from(JSON.stringify(message)).toString('base64')
        );
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
            setTimeout(this.flushWaitQueue.bind(this), this.confirmFailedQueue.size > 0 ? 2000 : 1);
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

                this.waitingCounter = 0; // Pending messages have been deleted

                if (this.consumers.size === 0) {
                    /* Send ping on pubChannel */
                    await this.sendPing(this.channel)
                        .then(() => this.useWaitQueue = false)
                        .catch(() => this.logger.warn('AmqpChannel#registerConnectinListener: Connection was established but got immediatly blocked.'));
                }

                if (this.waitQueue.size > 0 && this.useWaitQueue === false) {
                    setTimeout(this.flushWaitQueue.bind(this), this.confirmFailedQueue.size > 0 ? 2000 : 1);
                }

                this.events.emit('channel_ready');
            } catch (e) {
                this.logger.error('AmqpChannel: failed to create a new channel after connected.');
                this.logger.error(e);

                this.state = AmqpChannel.CS_ERROR;
            }

            await this._registerMissingConsumers();
        });
    }

    /**
     * @function
     *
     * This method can be used to identify the current blocked state of the underlying
     * connection. It should be used AFTER every consumer registration on the connection
     * has happenend and BEFORE the first message is send. That is because it will
     * trigger the 'blocked' event on the connection if the broker is in blocking mode.
     * @see https://github.com/OpusCapita/event-client/issues/8
     *
     * TODO this may be deprecated as soon as the requeue behavior in case of unacked
     * messages because of BLOCKED state is solved. @see https://github.com/OpusCapita/event-client/issues/10
     *
     * @param {AmqpChannel} channel - the channel we want to send the ping on
     *
     * @returns{Promise}
     */
    sendPing(channel) {
        return new Promise((resolve, reject) => {
            let timeout = null;

            if (!this.config || !this.config.exchangeName || typeof this.config.exchangeName !== 'string') {
                reject(new Error('AmqpChannel#sendPing: Faild to send ping, exchangeName is missing.'));
            }

            if (channel.useConfirmChannel === true) {
                timeout = setTimeout(() => reject(new Error('Ping timeout reached.')), 5000);
            }

            let result = channel.publish(this.config.exchangeName, 'ping', Buffer.from(JSON.stringify({cmd: 'ping'})), {mandatory: true}, (err) => {
                timeout && clearTimeout(timeout);

                this.logger.info('AmqpChannel#sendPing: Confirm for ping received on channel #' + channel.ch);

                if (err) {
                    reject(err);
                } else {
                    resolve(true);
                }
            });

            if (channel.useConfirmChannel !== true) {
                // Don't wait for response
                if (result === true) {
                    resolve(true);
                } else {
                    reject(new Error(`AmqpChannel#sendPing: Channel #${channel.ch} write buffer is full`));
                }
            }
        });
    }

    _createCacheEntry({exchangeName, topic, messageBuffer, options}, state = AmqpChannel.MSG_CREATED)
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

    timeoutPromise(fn, ms = 1500)
    {
        let timeout = new Promise((resolve) => {
            setTimeout(resolve, ms, false);
        });

        let cancelationPromise = new Promise(async (resolve, reject) => {
            return fn()
                .then(resolve)
                .catch((e) => {
                    this.logger.error(e);
                    reject(e);
                });
        }).catch((e) => this.logger.error(e));

        return Promise.race([cancelationPromise, timeout]);
    }

}

module.exports = AmqpChannel;
