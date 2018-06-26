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

        this.connection.events.on('error', () => this.reRegisterConsumers().catch(e => null));
    }

    async close()
    {
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

        const consumer = await channel.consume(queueName, async message =>
        {
            try
            {
                if(!message)
                    throw new EventError(`Received an empty message with content ${message}`, 500);

                await callback(message);
                channel.ack(message);
            }
            catch(e)
            {
                const requeue = e.code != 500;

                if(requeue)
                    this.logger.warn('An error occured while processing an incomming event.', e);
                else
                    this.logger.warn('Sending event to dead queue.', e);

                await channel.nack(message, false, requeue);
            }
        });

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
        const consumers = { ...this.consumers };
        await Promise.all(Object.keys(consumers).map(topic => this.removeConsumer(topic)));

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
        const channel = await this.connection.createChannel();

        channel.on('error', err => { this.logger.error(`A channel has been unexpectedly closed: ${err}`); safeMode && this.reRegisterConsumers(); });
        channel.on('close', err => { this.logger.info('A channel has been closed.', err || ''); err && safeMode && this.reRegisterConsumers(); });

        return channel;
    }

    async reRegisterConsumers()
    {
        try
        {
            if(this.connection)
            {
                const consumers = { ...this.consumers };
                this.clearConsumers();
                this.channel = null;

                await this.connection.reconnect();

                for(const topic in consumers)
                    await this.registerConsumer(consumers[topic]);
            }
        }
        catch(e)
        {
            this.logger.error(e);
        }
    }
}

module.exports = AmqpChannel;
