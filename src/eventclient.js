var singletonCore = null;

const extend = require('extend');
const Logger = require('ocbesbn-logger').setDebugMode(true);
/**
 * This is a thin wrapper around EventClientCore
 * that does carry contextual information
 */
class EventClient
{

    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);
        console.log("<<< EventClient config: ", this.config);
        if(!this.config.logger)
            this.config.logger = new Logger();
        this.logger = this.config.logger;
        this.core = getSingletonCore();
        this.consumers = {};
    }

    init() {
        console.log("EventClient.init()");
        return this.core.init(this.config)
        .then( () => {
            this.logger.info("Core initialization done");
        })
        .catch( (err) => {
            this.logger.error("Not able to initialize: ", err);
        });
    }

    /**
     * Allows adding a default context to every event emitted.
     * You may also want to construct an instance of this class by passing the context
     * parameter to the constructor. For further information have a look at {@link EventClient.DefaultConfig}.
     */
    contextify(context)
    {
        this.config.context = context || { };
    }
    
    subscribe(topic, callback = null, opts = { })
    {
        if(this.core.state != 3)
            return Promise.reject("Core not initialized, current state " + this.core.state);
        if(this.consumers[topic]) {
            this.logger.warn('Trying to consume topic ' + topic + ' repeatedly');
            return Promise.reject('Trying to consume topic ' + topic + ' repeatedly');
        }
        return this.core.subscribe(topic, callback, opts)
        .then( (result) => {
            this.consumers[topic] = result;
            this.logger.info("adding consumer info: ", result);
        })
        .catch( (err) => {
            this.logger.error("Subscribe failed:", err);
            return Promise.reject(err);
        });
    }
}
EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    serializerContentType : 'application/json',
    parserContentType : 'application/json',
    queueName : null,
    exchangeName : null,
    consul : {
        host : 'consul',
        mqServiceName  : 'rabbitmq-amqp',
        mqUserKey: 'mq/user',
        mqPasswordKey : 'mq/password'
    },
    consulOverride : {
        host : null,
        port : null,
        username : null,
        password : null
    },
    context : {
    }
}
module.exports = EventClient;

const net = require('net');
const configService = require('@opuscapita/config');
const bramqp = require('bramqp');
const queueSeparator = ':';

class EventClientCore
{
    constructor()
    {
        //this.connection = null;
        //this.channels = [ ];
        //this.pubChannel = null;
        //this.callbackErrorCount = { };

        this.state = 0; // 0 = new, 1 = should initialize, 2 = initializing, 3 = initialized
        this.connectionState = 0; // 0 = new, 1 = should connect, 2 = connecting, 3 = connected, 4 = comms ok
        this.consumers = {};
    }

    async init(config) 
    {
        if(this.state < 2) {
            this.state = 2;
            this.config = config;
            this.logger = this.config.logger;
            this.logger.info("Initializing EventClientCore with config ", this.config);
        }
        else 
        {
            this.logger.info("Skipping init of EventClientCore, current state is already ", this.state);
            return;
        }
        this.config = config;
        this.serviceName = configService.serviceName;
        this.exchangeName = this.config.exchangeName || this.serviceName;
//        this.queueName = this.config.queueName;
        this.connectionState = 1; 
console.log("EventClientCore.this: ", this);
        return this.initConfigService()
        .then( () => {
            this.logger.info("initialized config service");
            return this.connect();     
        })
        .then( () => {
            this.logger.info("initialized connection");
     
            return this.registerExchange();
        })
        .then( () => {
            this.logger.info("registered exchange " + this.exchangeName);
            this.state = 3;
            return Promise.resolve();
        })
        .catch( (err) => {
            this.logger.error("Error initializing EventClientCore: ", err);
            this.state = 1;
        })
    }

    async openChannel() {
//        this.amqpConnection.
    }

    async subscribe(topic, callback, opts) {
        if(this.connectionState != 4){
            this.logger.error("Trying to register consumer on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to register consumer on unconnected core, connectionState = " + this.connectionState);
        }
        this.logger.info("Declaring queue " + this.queueName(topic) + "...");
        try {
            await this.declareQueue(topic);
        }
        catch( e ) {
            this.logger.error("Error declaring queue for topic " + topic, e);
            throw e;
        }
        try {
            await this.bindQueue(topic);
        }
        catch( e ) {
            this.logger.error("Error binding queue for topic " + topic, e);
            throw e;
        }

    }

    async bindQueue(topic) {
        return new Promise((resolve, reject) => {
            let queueName = this.queueName(topic);
            let exchangeName = topic.substring(0, topic.indexOf(queueSeparator));
            let routingKey = topic.substring(topic.indexOf(queueSeparator)+1, topic.length);
            this.logger.info("binding queue with name " + queueName + " to exchange " + exchangeName + " using routing key " + routingKey);
            try {
            this.amqpConnection.queue.bind(1, queueName, exchangeName, routingKey,                                          false, {}, (error) =>
            {
                if(error)
                {
                    this.logger.error("Binding of queue " + queueName + " failed: ", error);
                    reject(error);
                }
                else {
                    this.logger.info("queue " + queueName + " bound.");
                    resolve();
                }
            }
            );
            }
            catch(e) {
                this.logger.error("Error binding queue " + queueName, e);
                reject(e);
            }
        });
    }

    async declareQueue(topic) {
         
        // channel#, queueName, passive, durable, exclusive, auto-delete, no-wait, args
        // note that args can be used to control mirroring, as per https://www.rabbitmq.com/ha.html
        // we would need to pass ha-mode: exactly and ha-params; count=1
        return new Promise((resolve, reject) => {
            this.logger.info("declaring queue with name " + this.queueName(topic));
            try {
            this.amqpConnection.queue.declare(1, this.queueName(topic), false, true, false, 
                                          false, false,{'ha-mode': {type:'Short string', data:'exactly'}, 'ha-params': {type:'Short string', data:'2'}}, (error) => 
            {
                if(error)
                {
                    this.logger.error("Declaration of queue " + this.queueName(topic) + " failed: ", error);
                    reject(error);
                }
                else {
                    this.logger.info("queue " + this.queueName(topic) + " declared.");
                    resolve();
                }
            }
            );
            }
            catch(e) {
                this.logger.error("Error declaring queue " + this.queueName(topic), e);
                reject(e);
            }
        });
    }

    queueName(topic) {
        return this.serviceName + queueSeparator + topic.replace('#',':ANY:');
    }

    async registerExchange() {
        return new Promise((resolve, reject) => {
            this.amqpConnection.exchange.declare(1, this.exchangeName, 'topic', false, true,
            false, false, false, {}, (err) => {
                if(err) {
                    this.logger.error(err);
                    reject("Failed declaring exchange " + this.exchangeName + ": " + error);
                }
                else resolve();
            });
        });
    }

    destroySocket() {
        if (this.socket)
        {
            this.logger.warn("destroying socket, current connectionState " + this.connectionState);
            try {
                this.socket.destroy("Socket destroyed");
            }
            catch(err) { this.logger.error("Error destroying socket: ",err)}
        }
    }

    async amqpConnectionErrorHandler(error) 
    {
        console.log("this3: ", this);
	logger.error("Error on amqp connection: ", error);
	this.connectionState = 1;
	this.logger.info("Resetting amqp connection");
	await new Promise( (resolve,reject) => {
	this.amqpConnection.closeAMQPCommunication( (err) =>
	    {
	        if(err) {
		    this.logger.error("Error closing amqp comms: ", err);
		    this.destroySocket();
		    this.connectionState = 1;
		    reject(err);
		}
		else {
		    this.logger.error("Closed amqp comms, destroying socket");
		    this.destroySocket();
		    this.connectionState = 1;
		    resolve();
		}
            });
	});
    }
    
    handleBramqpInit(initError, handle)
    {
        console.log("this2: ", this);
        if (initError) {
            this.logger.error("Could not initialize bramqp: ", initError);
            reject(initError);
        }
        else {
            this.logger.info("adding connection error handler");
            let logger = this.logger;
            handle.on('error', this.amqpConnectionErrorHandler.bind(this));
            handle.on('channel.close', function(channel, method, data) {
	        this.logger.error('channel ' + channel + ' closed by server');
            });
            resolve(handle);
        }
    }

    async connect()
    {
        if(this.connectionState > 1)
        {
            this.logger.warn("Skipping connect, state is ", this.connectionState);
            return false;
        }
        this.connectionState = 2;
        let connectionConfig = await this.getAmqpConnectionDetails();
        this.logger.info("using amqp connection details: ", connectionConfig);
        if (this.socket)
            destroySocket();

        this.logger.info("Connecting to " + connectionConfig.host + ":" + connectionConfig.port + "...");

        this.socket = net.connect({
            port: connectionConfig.port,
            host: connectionConfig.host
        });
        
        await new Promise((resolve, reject) => {
            console.log("this: ", this);
            bramqp.initialize(this.socket, 'rabbitmq/full/amqp0-9-1.stripped.extended', 
            this.handleBramqpInit.bind(this))
        })
        .then( (amqpConnection) => {
            this.amqpConnection = amqpConnection;
            this.logger.info("Connected to " + connectionConfig.host + ":" + connectionConfig.port);
            this.connectionState = 3;
        })
        .catch( (err) => {
            this.logger.error("Error establishing amqp connection: ", err);
            destroySocket();
            this.connectionState = 1; // set back to unconnected
        });

        await new Promise( (resolve, reject) => {
            this.amqpConnection.openAMQPCommunication(connectionConfig.username, connectionConfig.password, 10, '/', (error) => {
                if(error) {
                    reject(error);
                }
                else {
                    resolve();
                }
            });
        })
        .then( () => {
            this.logger.info("AMQP Comms started, channel 1 opened."); 
            this.connectionState = 4;
        })
        .catch( (err) => {
            this.logger.error("AMQP Comms can't be established, resetting", err);
            this.destroySocket();
            this.connectionState = 1;
        });
    }

    async getAmqpConnectionDetails() {
        const isConsulOverride = this.config.consulOverride && this.config.consulOverride.host && true;

        if(isConsulOverride)
        {
            const config = this.config.consulOverride;

            return {
                host : config.host,
                port : config.port,
                username : config.username,
                password : config.password,
                maxConnectTryCount : 5
            };
        }
        else
        {
            const config = this.config.consul;
            this.logger.info("reading config from consul..., this=", this);
            const { host, port } = await configService.getEndPoint(config.mqServiceName);
            this.logger.info("host: " + host + ", port: " + port);
            const [ username, password ] = await configService.get([ config.mqUserKey, config.mqPasswordKey ]);
            this.logger.info("username: " + username + ", passwprd: " + password);
            return { host, port, username, password, maxConnectTryCount : 10 };
        }

    }
    
    async initConfigService() 
    {
        const isConsulOverride = this.config.consulOverride && this.config.consulOverride.host && true;

        if(isConsulOverride)
        {
            this.logger.info("Consul override in use, skipping init of config service");
        }
        else
        {
            const consul = await configService.init();
        }
        return;
    }
}

function getSingletonCore(config) {
    if(!singletonCore) {
        console.log("Creating new EventClientCore singleton");
        singletonCore = new EventClientCore();
    }
    return singletonCore;
}


