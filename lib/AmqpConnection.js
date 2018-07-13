const Logger = require('ocbesbn-logger');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const EventEmitter = require('events');
const extend = require('extend');
const version = require('../package.json').version;

// Connection states
const CS = {
    EMPTY: 0,
    CONNECTING: 1,
    CONNECTED: 2,
    BLOCKED: 3,
    ERROR: 4,
    CLOSED: 5
};

class AmqpConnection
{
    constructor(config, logger)
    {
        this.config = config;
        this.connection = null;
        this.connectionState = CS.EMPTY;
        this.logger = logger || new Logger();
        this.events = new EventEmitter();
        this.connectTryCount = 0;

        this.uniqueId = require('crypto')
          .createHash('md5')
          .update(Math.random() + '')
          .digest('hex');

        logger.contextify({uniqueId: this.uniqueId});
    }

    setConfig(config)
    {
        this.config = extend(true, { }, this.config, config);
    }

    async connect()
    {
        if (this.connectionState >= CS.CONNECTING && this.connectionState <= CS.BLOCKED) {
            this.logger.info('Skipping connect (already connecting).');
            return true;
        }

        this.connectionState = CS.CONNECTING;

        // TODO What is this good for?
        this.connectTryCount++;

        this.logger.info('Current connectTryCount: ' + this.connectTryCount);

        if (this.connectTryCount < this.config.maxConnectTryCount) {
            try {

                // Retry until connection estabished
                this.connection = await retry(this.doConnect.bind(this), {
                    'max_tries': 10000,
                    interval: 500,
                    timeout: 120000,
                    backoff: 1.2
                });
            } catch (e) {
                this.connectionState = CS.ERROR;
                this.logger.warn(e);
                this.events.emit('error', e);

                throw e;
            }

            if (this.connection) {
                this.connection.on('error', (err) => {
                    this.connectionState = CS.ERROR;
                    this.logger.warn('Error on connection.', err);
                    this.events.emit('error', err);
                });

                this.connection.on('blocked', (err) => {
                    this.connectionState = CS.BLOCKED;
                    this.logger.warn('Blocked connection.', err);
                    this.events.emit('connection_blocked', err);
                });

                this.connection.on('unblocked', err => {
                    this.connectionState = CS.CONNECTED;
                    this.logger.warn('Unblocked connection.', err);
                    this.events.emit('connection_unblocked', err);
                });

                this.connection.on('close', async (err) => {
                    this.connectionState = CS.CLOSED;
                    this.logger.warn('Connection closed.', err || '');

                    this.events.emit('close');

                    await this.reconnect();
                });

                this.events.emit('connected');

                this.connectionState = CS.CONNECTED;
                this.connectTryCount = 0;

                this.logger.info('Connection to %s:%s established.', this.config.host, this.config.port);

            } else {
                this.connectionState = CS.ERROR;
                this.connection = null;

                this.logger.info('Could not connect to %s:%s.', this.config.host, this.config.port);
                throw new Error('Could not connect to server.');
            }

        } else {
            this.logger.error(`Max connectTryCount exceeded: ${this.connectTryCount}.`);
            throw new Error('Max connectTryCount exceeded.');
        }

        return true;
    }

    doConnect() {
        this.logger.info('Trying to connect to %s:%s...', this.config.host, this.config.port);

        return amqp.connect({
            protocol: 'amqp',
            hostname: this.config.host,
            port: this.config.port,
            username: this.config.username,
            password: this.config.password,
            heartbeat: 15
        }, {
            clientProperties: {
                product: 'EventClient',
                version: version,
                service: this.config.serviceName
            }
        });
    }

    async close() {
        if (this.connection && this.connectionState <= CS.BLOCKED) {
            try {
                await this.connection.close();

                this.connection = null;
                this.connectionState = CS.EMPTY;

            } catch (e) {
                this.connectionState = CS.ERROR;
                this.logger.error(e);

            } finally {
                // TODO Double check this for possible connection leak (@see amqplib.Heartbeat)
                this.connection = null;
            }
        }

        return true;
    }

    async reconnect()
    {
        if (this.connectionState !== CS.CONNECTING) {
            await this.close();
            return this.connect();
        } else {
            return true;
        }
    }

    async createChannel()
    {
        await this.connect();
        return this.connection && this.connection.createChannel();
    }

    async createConfirmChannel(cb)
    {
        await this.connect();
        return this.connection && this.connection.createConfirmChannel(cb);
    }
}

module.exports = AmqpConnection;
