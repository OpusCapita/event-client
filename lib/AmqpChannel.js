const Logger = require('ocbesbn-logger');
const EventError = require('./EventError');
const retry = require('bluebird-retry');

const overallConsumers = { };

class AmqpChannel
{
    constructor(connection, logger)
    {
        this.connection = connection;
        this.channel = null;
        this.logger = logger || new Logger();
        this.consumers = { };
        this.badConsumers = { };
        this.badConsumersWorker = setInterval(() => this.processBadConsumers(), 15000);

        this.connection.events.on('error', async () =>
        {
            this.logger.warn('Error on connection. Trying to reconnect...');

            await this.connection.reconnect().catch(e => { this.logger.error('Reconnect failed: ', e); throw e; });

            this.logger.info('Trying to re-register consumers...');

            this.badConsumers = this.consumers;
            this.consumers = { };

            //return this.reRegisterConsumers();
        });
    }

    async close()
    {
        this.logger.info('Closing channel...');

        if(this.channel)
        {
            await this.clearConsumers().catch(e => null);
            await this.channel.close().catch(e => null);
        }

        this.channel = null;
        this.consumers = { };
    }

    async registerConsumer({ exchangeName, queueName, topic, callback, prefetchCount })
    {
        if(this.consumerExists(topic))
            throw new EventError(`The topic "${topic}" has already been registered.`, 409);

        const channel = await this.getChannel();

        if(prefetchCount)
            channel.prefetch(prefetchCount, false);

        await this.createAndBindQueue({ queueName, exchangeName, topic });

        this.logger.info(`Trying to consume channel on queue "${queueName}" for topic "${topic}".`);

        const consumer = await channel.consume(queueName, async message =>
        {
            try
            {
                if(message)
                {
                    await callback(message);
                    await channel.ack(message);
                }
                else
                {
                    this.logger.warn(`Received an empty message. Sending consumer to retry list...`);
                    this.badConsumers[topic] = this.consumers[topic];

                    await this.removeConsumer(topic);

                    // await this.registerConsumer({ exchangeName, queueName, topic, callback, prefetchCount });
                }
            }
            catch(e)
            {
                const requeue = e.code != 500;

                if(requeue)
                    this.logger.warn(`An error occured while processing an incomming event on queue "${queueName}".`, e);
                else
                    this.logger.warn(`Sending event from queue "${queueName}" to dead queue.`, e);

                if(message)
                    await channel.nack(message, false, requeue);
            }
        });

        if(!consumer)
        {
            this.logger.warn('Could not register consumer. Empty result.');
            throw new Error('Could not register consumer.');
        }

        this.consumers[topic] = {
            consumerTag : consumer.consumerTag,
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
        return channel.get(queueName,  { noAck : autoAck });
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

    async createAndBindQueue({ queueName, exchangeName, topic })
    {
        const channel = await this.getChannel();

        await channel.assertQueue(queueName, { durable: true, autoDelete: false, arguments : { 'x-dead-letter-exchange' : `${exchangeName}.dead` } });
        await retry(() => channel.bindQueue(queueName, exchangeName, topic), { max_tries: 10000, interval: 500, timeout : 120000, backoff : 1.2 });
    }

    async removeConsumer(topic)
    {
        this.logger.info(`Trying to remove consumer for topic "${topic}."`);

        const consumer = this.consumers[topic];

        if(consumer)
        {
            this.channel && await this.channel.cancel(consumer.consumerTag).catch(() => null);

            delete this.consumers[topic];
            delete overallConsumers[topic];

            return true;
        }

        return false;
    }

    async publish({ exchangeName, topic, messageBuffer, options })
    {
        return (await this.getChannel()).publish(exchangeName, topic, messageBuffer, options);
    }

    async createExchange(exchangeName, type = 'topic')
    {
        await (await this.getChannel()).assertExchange(exchangeName, type, { durable: true, autoDelete: false });
    }

    async createDeadExchange(exchangeName)
    {
        const channel = await this.getChannel();

        await this.createExchange(`${exchangeName}.dead`, 'fanout');
        await channel.assertQueue(`${exchangeName}.dead`, { durable: true, autoDelete: false });
        await channel.bindQueue(`${exchangeName}.dead`, `${exchangeName}.dead`, '')
    }

    async deleteQueue(queueName, opts = { })
    {
        opts = { ifUnused : opts.unusedOnly, ifEmpty : opts.emptyOnly };
        let channel;

        try
        {
            channel = await this.getNewChannel();
            await channel.deleteQueue(queueName, opts);
        }
        finally
        {
            channel && channel.close().catch(e => null);
        }

        return true;
    }

    async exchangeExists(exchangeName)
    {
        let channel;

        try
        {
            channel = await this.getNewChannel();
            await channel.checkExchange(exchangeName);
        }
        finally
        {
            channel && channel.close().catch(e => null);
        }

        return true;
    }

    async queueExists(queueName)
    {
        let channel;

        try
        {
            channel = await this.getNewChannel();
            await channel.checkQueue(queueName);
        }
        catch(e)
        {
            return false;
        }
        finally
        {
            channel && channel.close().catch(e => null);
        }

        return true;
    }

    consumerExists(topic)
    {
        return overallConsumers[topic] === true;
    }

    async clearConsumers()
    {
        this.logger.info('Trying to clear consumers...');

        const consumers = { ...this.consumers };
        await Promise.all(Object.keys(consumers).map(topic => this.removeConsumer(topic)));

        this.logger.info('Consumers cleared.');

        this.consumers = { };
    }

    async getChannel()
    {
        if(!this.channel)
            this.channel = await this.getNewChannel(true);

        return this.channel;
    }

    async getNewChannel(safeMode)
    {
        this.logger.info('Creating new channel...');

        const channel = await this.connection.createChannel();

        channel.on('error', err =>
        {
            this.logger.error(`A channel has been unexpectedly closed: ${err}`);

            // if(safeMode && err.message.includes('404 (NOT-FOUND)'))
            //     this.reRegisterConsumers();
        });

        channel.on('close', err => { this.logger.info('A channel has been closed.', err || ''); });

        this.logger.info('Channel created.');

        return channel;
    }

    async reRegisterConsumers()
    {
        try
        {
            if(this.connection)
            {
                this.logger.info('Starting re-registration of consumers...', Object.keys(this.consumers));

                const consumers = { ...this.consumers };
                this.consumers = { };

                Object.keys(consumers).map(key => { delete overallConsumers[key]; });

                this.channel = null;

                for(const topic in consumers)
                    await this.registerConsumer(consumers[topic]);

                this.logger.info('Consumer re-registration done.');
            }
        }
        catch(e)
        {
            this.logger.error(e);
        }
    }

    async processBadConsumers()
    {
        const topics = Object.keys(this.badConsumers);

        if(topics.length === 0)
            return;

        this.logger.info('Processing bad consumers...');

        const badConsumers = { ...this.badConsumers };

        this.badConsumers = { };

        for(const topic in badConsumers)
        {
            this.logger.info(`Processing bad consumer "${topic}".`);

            await this.registerConsumer(badConsumers[topic]).catch(e =>
            {
                this.badConsumers[topics] = badConsumers[topic];
                this.logger.warn(`Failed to register bad consumer "${topic}".`, e);
            });
        }

        this.logger.info('Processing consumers done.');
    }
}

module.exports = AmqpChannel;
