const singletonCore = new EventClientCore();

module.exports = EventClient;

/**
 * This is a thin wrapper around EventClientCore
 * that does carry contextual information
 */
class EventClient 
{

    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);
        this.core = getSingletonCore();
    }

    function init() {
        return Promise.resolve((async () =>
        {
            await this.core.init(this.config);

        })());
    }
}

function getSingletonCore(config) {
    if(!singletonCore) {
        singletonCore = new EventClientCore();
    }
}

const net = require('net');
const configService = require('@opuscapita/config');
const bramqp = require('bramqp');

class EventClientCore
{
    constructor()
    {
        //this.connection = null;
        //this.channels = [ ];
        //this.pubChannel = null;
        //this.callbackErrorCount = { };

        this.state = 0; // 0 = uninitialized, 1 = initializing, 2 = initialized
    }

    async function init(config) 
    {
        if(this.state == 0) {
            this.state = 1;
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
        this.queueName = this.config.queueName;
        this.connectionState = 0; // 0 = unconnected, 1 = connecting, 2 = connected
        await initConfigService();
        
        await connect();     
    }

    async function destroySocket() {
        if (this.socket)
        {
            this.logger.warn("Found lingering socket while in state " + this.connectionState);
            try {
                this.socket.destroy("Socket destroyed by connect()");
            }
            catch(err) { this.logger.error("Error destroying socket: ",err)}
        }
    }

    async function connect()
    {
        if(this.connectionState > 0)
        {
            this.logger.warn("Skipping connect, state is ", this.connectionState);
            return false;
        }
        this.connectionState = 1;
        let connectionConfig = getAmqpConnectionDetails();
        if (this.socket)
            await destroySocket();

        this.logger.info("Connecting to " + connectionConfig.host + ":" + connectionConfig.port + "...");

        this.socket = net.connect({
            port: connectionConfig.port,
            host: connectionConfig.host
        });
        
        new Promise((resolve, reject) => {
            bramqp.initialize(this.socket, 'rabbitmq/full/amqp0-9-1.stripped.extended', function(initError, handle)
            {
                if (initError) {
	            this.logger.error("Could not initialize bramqp: ", initError);
                    reject(initError);
                }
                else {
                    resolve(handle);
                }
            })
        })
        .then( (amqpConnection) => {
            this.amqpConnection = amqpConnection;
            this.logger.info("Connected to " + connectionConfig.host + ":" + connectionConfig.port);
            this.connectionState = 2;
        })
        .catch( (err) => {
            this.logger.error("Error establishing amqp connection: ", err);
            destroySocket();
            this.connectionState = 0; // set back to unconnected
        })
    }

    async function getAmqpConnectionDetails() {
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

            const { host, port } = await consul.getEndPoint(config.mqServiceName);
            const [ username, password ] = await consul.get([ config.mqUserKey, config.mqPasswordKey ]);

            return { host, port, username, password, maxConnectTryCount : 10 };
        }

    }
    
    async function initConfigService() 
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
    }
}

EventClient.DefaultConfig = {
    serializer : JSON.stringify,
    parser : JSON.parse,
    serializerContentType : 'application/json',
    parserContentType : 'application/json',
    queueName : null,
    exchangeName : null,
    logger : new Logger(),
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

