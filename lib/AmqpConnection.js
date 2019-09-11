const Logger = require('@opuscapita/logger');
const retry = require('bluebird-retry');
const amqp = require('amqplib');
const EventEmitter = require('events');
const extend = require('extend');
const version = require('../package.json').version;

class AmqpConnection
{
    /* Connection states */
    static get CS_EMPTY()       { return 0; }
    static get CS_CONNECTING()  { return 1; }
    static get CS_CONNECTED()   { return 2; }
    static get CS_BLOCKED()     { return 3; }
    static get CS_ERROR()       { return 4; }
    static get CS_CLOSED()      { return 5; }
    static get CS_DISPOSED()    { return 6; }

    constructor(config, logger)
    {
        this.config = config;
        this.connection = null;
        this.connectionState = AmqpConnection.CS_EMPTY;
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
        if (this.connectionState >= AmqpConnection.CS_CONNECTING && this.connectionState <= AmqpConnection.CS_BLOCKED) {
            this.logger.info('Skipping connect (already connecting).');
            return true;
        }

        this.connectionState = AmqpConnection.CS_CONNECTING;

        this.connectTryCount++;

        this.logger.info('Current connectTryCount: ' + this.connectTryCount);

        if (this.connectTryCount < this.config.maxConnectTryCount) {
            try {
                // Retry until connection estabished
                this.connection = await retry(this.doConnect.bind(this), {
                    'max_tries': 10000,
                    interval: 500,
                    timeout: 72 * 60 * 60 * 1000, // 72h
                    backoff: 1.2
                });
            } catch (e) {
                this.connectionState = AmqpConnection.CS_ERROR;
                this.logger.error('AmqpConnection#connect: Caught exception while retrying this#doConnect. ', e);
                this.events.emit('error', e);

                throw e;
            }

            if (this.connection) {
                this.connection.on('error', (err) => {
                    this.connectionState = AmqpConnection.CS_ERROR;
                    this.logger.warn('AmqpConnection: Connection went to ERROR state', err);
                    this.events.emit('error', err);
                });

                this.connection.on('blocked', (err) => {
                    this.connectionState = AmqpConnection.CS_BLOCKED;
                    this.logger.warn('AmqpConnection: Connection went to BLOCKED state', err);
                    this.events.emit('connection_blocked', err);
                });

                this.connection.on('unblocked', err => {
                    this.connectionState = AmqpConnection.CS_CONNECTED;
                    this.logger.info('AmqpConnection: Connection went to UNBLOCKED state', err);
                    this.events.emit('connection_unblocked', err);
                });

                this.connection.on('close', async (err) => {
                    this.connectionState = AmqpConnection.CS_CLOSED;
                    this.logger.warn('AmqpConnection: Connection went to CLOSED state', err);
                    this.events.emit('close');

                    await this.reconnect();
                });

                this.events.emit('connected');

                this.connectionState = AmqpConnection.CS_CONNECTED;
                this.connectTryCount = 0;

                this.logger.info('Connection to %s:%s established.', this.config.host, this.config.port);

            } else {
                this.connectionState = AmqpConnection.CS_ERROR;
                this.connection = null;

                const msg = `AmqpConnection#connect: Could not connect to ${this.config.host}:${this.config.port}.`;
                this.logger.error(msg);
                throw new Error(msg);
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
        if (this.connection && (this.connectionState <= AmqpConnection.CS_BLOCKED || this.connectionState === AmqpConnection.CS_DISPOSED)) {
            try {
                this.connection.removeAllListeners('error');
                this.connection.removeAllListeners('blocked');
                this.connection.removeAllListeners('unblocked');
                this.connection.removeAllListeners('close');


                let closeResult = await this.timeoutPromise(() => this.connection.close());

                if (closeResult === false) {
                    /**
                     * Burn it!!
                     *
                     * In some cases (eg. connection is set to bloocked state by the broker)
                     * the broker stops reading and will never ACK the FIN. Thus the underlying
                     * socket (amqplib) will wait indefintly to be close and not return its Promise.
                     *
                     * So we simply kill the socket after the timeout is reached.
                     */
                    try {
                        this.connection.connection.heartbeater.clear();
                        this.connection.connection.stream.destroy();
                    } catch (e) {
                        this.logger.error('AmqpConnection#close: Faild to destroy amqplib socket.', e);
                    }
                }

                this.connection = null;
                this.connectionState = AmqpConnection.CS_EMPTY;

            } catch (e) {
                this.connectionState = AmqpConnection.CS_ERROR;
                this.logger.error(e);

            } finally {
                this.connection = null;
            }
        }

        return true;
    }

    async dispose() {
        return new Promise((resolve) => {
            this.connectionState = AmqpConnection.CS_DISPOSED;
            setTimeout(async () => {
                await this.close();
                resolve(true);
            }, 100);
        });
    }

    async reconnect()
    {
        if (this.connectionState !== AmqpConnection.CS_CONNECTING && this.connectionState !== AmqpConnection.CS_DISPOSED) {
            await this.close();
        }

        /* Check state again to make sure it did not change. */
        if (this.connectionState !== AmqpConnection.CS_CONNECTING && this.connectionState !== AmqpConnection.CS_DISPOSED) {
            return this.connect();
        }

        return true;
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

    timeoutPromise(fn, ms = 1500)
    {
        let timeout = new Promise((resolve) => {
            setTimeout(resolve, ms, false);
        });

        let cancelationPromise = new Promise(async (resolve, reject) => {
            return fn()
                .then(() => resolve(true))
                .catch((e) => {
                    this.logger.error(e);
                    reject(e);
                });
        }).catch((e) => this.logger.error(e));

        return Promise.race([cancelationPromise, timeout]);
    }

}

module.exports = AmqpConnection;
