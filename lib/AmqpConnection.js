const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const EventEmitter = require('events');

class AmqpConnection
{
    constructor(config, logger)
    {
        this.config = config;
        this.connection = null;
        this.logger = logger || new Logger();
        this.events = new EventEmitter();
        this.connectTryCount = 0;
    }

    async connect()
    {
        if(this.connection)
            return;

        const doConnect = () => amqp.connect({
            protocol : 'amqp',
            hostname : this.config.host,
            port : this.config.port,
            username : this.config.username,
            password : this.config.password
        });

        this.connectTryCount++;

        if(this.connectTryCount < this.config.maxConnectTryCount)
        {
            this.connection = await retry(doConnect, { max_tries: 10000, interval: 500, timeout : 120000, backoff : 1.2 }).catch(e => { this.events.emit('error', e); throw e; });

            if(this.connection)
            {
                this.connection.on('error', err => { this.logger.warn('Error on connection.', err); this.events.emit('error', err); });
                this.connection.on('blocked', err => { this.logger.warn('Blocked connection.', err); this.events.emit('error', err); });
                this.connection.on('unblocked', err => { this.logger.warn('Unblocked connection.', err); this.events.emit('error', err); });
                this.connection.on('close', err => { this.logger.warn('Connection closed.', err || ''); (err && this.events.emit('error', err)) || this.events.emit('close'); });
            }
            else
            {
                this.connection = null;
                throw new Error();
            }
        }
    }

    async close()
    {
        if(this.connection)
            await this.connection.close().catch(e => null);

        this.connection = null;
    }

    async reconnect()
    {
        await this.close();
        return this.connect();
    }

    async createChannel()
    {
        await this.connect();
        return this.connection && this.connection.createChannel();
    }
}

module.exports = AmqpConnection;
