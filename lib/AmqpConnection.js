const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const EventEmitter = require('events');
const extend = require('extend');

class AmqpConnection
{
    constructor(config, logger)
    {
        this.config = config;
        this.connection = null;
        this.connectionState = 0; // 0 = Empty, 1 = Connecting, 2 = Connected, 3 = Error, 4 = Closed
        this.logger = logger || new Logger();
        this.events = new EventEmitter();
        this.connectTryCount = 0;
        this.uniqueId = require('crypto').createHash('md5').update(Math.random() + '').digest("hex");

        logger.contextify({ uniqueId : this.uniqueId });
    }

    setConfig(config)
    {
        this.config = extend(true, { }, this.config, config);
    }

    async connect()
    {
        if(this.connectionState === 1)
        {
            this.logger.info('Skipping connect (already connecting).');
            return;
        }
        else if(this.connectionState === 2)
        {
            this.logger.info('Skipping connect (already connected).');
            return;
        }

        this.connectionState = 1;

        const doConnect = () =>
        {
            this.logger.info('Trying to connect to %s:%s...', this.config.host, this.config.port);

            return amqp.connect({
                protocol : 'amqp',
                hostname : this.config.host,
                port : this.config.port,
                username : this.config.username,
                password : this.config.password,
                heartbeat : 15
            }, {
                clientProperties : {
                    product : 'EventClient',
                    version : version
                }
            });
        }

        this.connectTryCount++;

        this.logger.info('Current connectTryCount: ' + this.connectTryCount);

        if(this.connectTryCount < this.config.maxConnectTryCount)
        {
            this.connection = await retry(doConnect, { max_tries: 10000, interval: 500, timeout : 120000, backoff : 1.2 }).catch(e =>
            {
                this.connectionState = 3;
                this.logger.warn(e);
                this.events.emit('error', e);

                throw e;
            });

            if(this.connection)
            {
                this.connection.on('error', err => { this.connectionState = 3; this.logger.warn('Error on connection.', err); this.events.emit('error', err); });
                this.connection.on('blocked', err => { this.connectionState = 3; this.logger.warn('Blocked connection.', err); this.events.emit('error', err); });
                this.connection.on('unblocked', err => { this.logger.warn('Unblocked connection.', err); this.events.emit('error', err); });
                this.connection.on('close', err => { this.connectionState = 4; this.logger.warn('Connection closed.', err || ''); (err && this.events.emit('error', err)) || this.events.emit('close'); });

                this.connectionState = 2;
                this.connectTryCount = 0;

                this.logger.info('Connection to %s:%s established.', this.config.host, this.config.port);
            }
            else
            {
                this.connectionState = 3;
                this.connection = null;

                this.logger.info('Could not connect to %s:%s.', this.config.host, this.config.port);
                throw new Error('Could not connect to server.');
            }
        }
        else
        {
            this.logger.error(`Max connectTryCount exceeded: ${this.connectTryCount}.`);
            throw new Error('Max connectTryCount exceeded.');
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
