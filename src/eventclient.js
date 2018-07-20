var singletonCore = null;

const extend = require('extend');
const util = require('util');
const Logger = require('ocbesbn-logger').setDebugMode(true);
const ChannelPool = require('./ChannelPool');
const Promise = require('bluebird');
const uuidv4 = require('uuid/v4');

/**
 * This is a thin wrapper around EventClientCore
 * that does carry contextual information
 */
class EventClient
{

    constructor(config)
    {
        this.config = extend(true, { }, EventClient.DefaultConfig, config);
        //console.log("<<< EventClient config: ", this.config);
        if(!this.config.logger)
            this.config.logger = new Logger();
        this.logger = this.config.logger;
        this.core = getSingletonCore();
        this.consumers = {}; // map of topic -> consumer channel descriptor
        this.instanceId = uuidv4();
    }

    init() {
        console.log("EventClient.init()");
        return this.core.init(this)
        .then( () => {
            this.logger.info("Core initialization done\n\n");
        })
        .catch( (err) => {
            this.logger.error("Not able to initialize: ", err);
        });
    }

    /**
     * Closes all publisher and subscriber channels held by this instance. If this is the last event client instance, it will also tear
     * down the shared core.
     *
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or rejecting with an error.
     */
    dispose()
    {
        this.logger.info("unregistering client with id " + this.instanceId);
        return this.core.unregisterClient(this.instanceId);
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
    
    /**
     * This method allows you to unsubscribe from a previous subscribed *topic* or pattern.
     *
     * @param {string} topic - Full name of a topic or a pattern.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the topic existed in the subscriptions.
     */
    unsubscribe(topic)
    {
        if(this.core.state != 3)
            return Promise.reject("Can't subscribe " + topic + ": Core not initialized, current state " + this.core.state);
        let consumer = this.consumers[topic];
        let existing = false;
        if(!consumer) {
            this.logger.warn("cannot unsubscribe from topic: " + topic + ", no consumer registerd.");
            //throw new Error("failed to unsubscribe, topic " + topic + " not registered");
        }
        else existing = true;
        try {
            return this.core.unsubscribe(topic)
            .then( (channel) => {
                delete this.consumers[topic];
                return Promise.resolve(existing);
            })
            .catch( (err) => {
                this.logger.error("error in unsubscribe " +  topic + ": ", err);
                throw err;
            });
        }
        catch(e) {
            this.logger.error("Unsubscription of topic " + topic + " failed: ", e);
            throw e;
        }
        
    }

    subscribe(topic, callback = null, opts = { })
    {
        if(this.core.state != 3)
            return Promise.reject("Can't subscribe " + topic + ": Core not initialized, current state " + this.core.state);
        if(this.consumers[topic]) {
            this.logger.warn('Trying to consume topic ' + topic + ' repeatedly');
            return Promise.reject('Trying to consume topic ' + topic + ' repeatedly');
        }
        return this.core.subscribe(topic, callback, opts)
        .then( (result) => {
            this.consumers[topic] = result;
            //this.logger.info("adding consumer info: ", result);
        })
        .catch( (err) => {
            this.logger.error("Subscribe failed:", err);
            return Promise.reject(err);
        });
    }
    
    /**
     * Raises an event for a certain topic by passing a message and an optional context.
     *
     * The passed *topic* has to be a string and identify the raised event as exact as possible.
     * The passed *message* can consis of all data types that can be serialized into a string.
     * The optional *context* paraemter adds additional meta data to the event. It has to be an event
     * and will extend a possibly existing global context defined by the config object passed
     * to the constructor (see {@link EventClient.DefaultConfig}).
     *
     * @param {string} topic - Full name of a topic.
     * @param {object} message - Payload to be sent to a receiver.
     * @param {object} context - Optional context containing meta data for the receiver of an event.
     * @param {EmitOpts} opts - Additional options to be set for emmiting an event.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    emit(topic, message, context = null, opts = { })
    {
        if(this.core.state != 3)
            return Promise.reject("Can't publish event Core not initialized, current state " + this.core.state);

        let localContext = {
            //senderService : this.core.serviceName,
            //timestamp : new Date().toString()
        };

        localContext = extend(true, { }, this.config.context, context, localContext);
        //console.log("extend result: " + JSON.stringify(localContext));
        const messageId = `${this.core.serviceName}.${crypto.randomBytes(16).toString('hex')}`;
        let payload = this.config.serializer(message);
        
        const options = {
            ...opts,
            persistent : true,
            contentType : this.config.serializerContentType,
            contentEncoding : 'utf-8',
            timestamp : new Date(),
            correlationId : context && context.correlationId,
            appId : this.core.serviceName,
            messageId : messageId,
            headers : localContext
        };

        const logger = this.logger.clone();

        logger.contextify(extend(true, { }, localContext, options));
        logger.info(`requested to emit event with topic "${topic}", options = `, options);
        
        return this.core.emit(topic, payload, options)
        .catch( (err) => {
            let eventRep = {options: options, payload: payload};
            this.logger.error("Publish failed with error " + err + " on topic " + topic + ", options " + JSON.stringify(options) + ", payload = ", eventRep);
            return Promise.reject(err);
        });
    }

    /**
     * Checks whenever an exchange exists or not. As checking for non existing exchnages provokes server errors (404) that will destroy the communication channel and log an error, this method should
     * not be called excessively.
     * @param {string} exchangeName - The name of the exchange to find.
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to true or false depending on whenever the exchange exists.
     */
    exchangeExists(exchangeName)
    {
        if(this.core.state != 3)
            return Promise.reject("Can't query exchange: Core not initialized, current state " + this.core.state);
        
        return this.core.exchangeExists(exchangeName)
        .then( (result) => {
            //this.logger.info("exchangeExists(" + exchangeName + "): ", result);
            return true;
        })
        .catch( (err) => {
            this.logger.error("exchange doesnt exist: " , err);
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
const crypto = require('crypto');
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

        this.state = 0; // 0 = new, 1 = should initialize, 2 = initializing, 3 = initialized, 4 - disposing, 
        this.connectionState = 0; // 0 = new, 1 = should connect, 2 = connecting, 3 = connected, 4 = comms ok, 5 = closing
        this.consumers = {};
        this.clients = {};
    }

    async unregisterClient(instanceid) {
        let instance = this.clients[instanceid];
        if(!instance) throw new Error("core doesn't know an instance of eventclient with id " + instanceid);
        let cancelPromises = [];
        for(let consumer in instance.consumers) {
            console.log("selecting consumer ", consumer, " for unregistering due to dispose()...");
            // we need to cancel consumer then return channel
            cancelPromises.push(
                this.unsubscribe(consumer.consumerTopic)
            );
        }
        return Promise.all(cancelPromises)
        .then( () => {
            delete this.clients[instanceid];
            // now check whether the lasdt client instance was taken down
            if(Object.keys(this.clients).length <1) {
                this.logger.warn("last instance has been unregistered from event client core, shutting down.");
                return this.dispose();
            }
            else {
                this.logger.info("core staying up, " + Object.keys(this.clients).length + " instances remaining...", this.clients);
            }
            return Promise.resolve(true);
        });
    }
    
    async dispose() {
        if(this.state == 4) {
            throw new Error("can't dispose as already in state closing");
        }
        
        this.connectionState = 5;
        
        return Promise.fromCallback( (cb) => {
            //this.amqpConnection.removeAllListeners();
            this.socket.removeAllListeners('error');
            this.socket.on('close', () => {
                this.logger.info("Socket has been closed");
                this.socket.removeAllListeners();
                this.socket = null;
                this.connectionState = 0;
            });
            this.logger.info("shutting down amqp comms...");
            this.amqpConnection.closeAMQPCommunication( cb );
            
        })
        .then( () => {
            this.logger.info("destroying socket");
            return this.destroySocket();
        })
        .then( () => {this.state = 0; this.logger.info("core disposed.");});
    }
    
    async init(eventclientinst) 
    {
        let config = eventclientinst.config;
        if(this.clients[eventclientinst.instanceId]) {
            throw new Error("This client instance " + eventclientinst.instanceId + " has already called init on core.");
        }
        this.clients[eventclientinst.instanceId] = eventclientinst;
        if(this.state < 2) {
            this.state = 2;
            this.config = config;
            this.logger = this.config.logger;
            this.logger.info("Initializing EventClientCore"); // with config ", this.config);
        }
        else 
        {
            this.logger.info("Skipping init of EventClientCore, current state is already ", this.state);
            return;
        }
        this.config = config;
        this.serviceName = configService.serviceName;
        this.exchangeName = this.config.exchangeName || this.serviceName;
        this.channelPool = new ChannelPool(this.config, this);
//        this.queueName = this.config.queueName;
        this.connectionState = 1; 
// console.log("EventClientCore.this: ", this);
        return this.initConfigService()
        .then( () => {
            this.logger.info("initialized config service");
            return this.connect();     
        })
        .then( (amqpConnection) => {
            this.logger.info("initialized connection, connectionState " + this.connectionState + "\n\n");
            
            this.logger.info("initializing channel pool, config: " + this.config.pool);
            return this.channelPool.init();
        })
        .then( () => {
            this.logger.info("initialized channel pool\n\n");
            
            this.logger.info("registering exchange " + this.exchangeName + "...");
            return this.createExchange(); 
        })
        .then( () => {
            this.logger.info("registered exchange " + this.exchangeName + "\n\n");
            this.state = 3;
            return Promise.resolve();
        })
        .catch( (err) => {
            this.logger.error("Error initializing EventClientCore: ", err);
            this.state = 1;
            throw new Error(err);
        })
    }

    async exchangeExists(exchangeName) 
    {
        if(this.connectionState != 4){
            this.logger.error("Trying to query exchange on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to query exchange on unconnected core, connectionState = " + this.connectionState);
        }

        let channel = null;
        try {
            channel = await this.channelPool.leaseChannel("publish");
        }
        catch(e) {
            this.logger.error("Error leasing channel to query exchange: ", e);
            throw e; // maybe handle this transparently ?
        }
        
        try {
            await this.declareExchange(channel, exchangeName, true);
        }
        catch(e) {
            this.logger.error("Error querying exchange on channel " + channel.channelNo + ": ", e);
            throw e; // maybe handle this transparently ?
        }
        finally {
            await this.channelPool.returnChannel(channel.channelNo);
        }
        return null;
    }
    
    /**
     * Delcares the exchange owned by the enclosing service of this lib instance, named like this.exchangeName
     */
    async createExchange() 
    {
        if(this.connectionState != 4){
            this.logger.error("Trying to register service exchange on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to register service exchange on unconnected core, connectionState = " + this.connectionState);
        }

        let channel = null;
        try {
            channel = await this.channelPool.leaseChannel("mgmt");
        }
        catch(e) {
            this.logger.error("Error leasing channel to register service exchange: ", e);
            throw e; // maybe handle this transparently ?
        }
        
        try {
            await this.declareExchange(channel);
        }
        catch(e) {
            this.logger.error("Error registering service exchange on channel " + channel.channelNo + ": ", e);
            throw e; // maybe handle this transparently ?
        }
        finally {
            await this.channelPool.returnChannel(channel.channelNo);          
        }
        return null;
    }

    /**
     * Raises an event for a certain topic by passing a message and an optional context.
     *
     * The passed *topic* has to be a string and identify the raised event as exact as possible.
     * The passed *message* can consis of all data types that can be serialized into a string.
     * The optional *context* paraemter adds additional meta data to the event. It has to be an event
     * and will extend a possibly existing global context defined by the config object passed
     * to the constructor (see {@link EventClient.DefaultConfig}).
     *
     * @param {string} topic - Full name of a topic.
     * @param {object} payload - String (already serialized)
     * @param {EmitOpts} opts - message properties (application specific headers go into opts.headers)
     * @returns {Promise} [Promise](http://bluebirdjs.com/docs/api-reference.html) resolving to null if the subscription succeeded. Otherwise the promise gets rejected with an error.
     */
    async emit(topic, payload, opts)
    {
        if(this.connectionState != 4){
            this.logger.error("Trying to emit on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to emit on unconnected core, connectionState = " + this.connectionState);
        }


        const exchangeName = this.exchangeName;
        
        let channel = null;
        try {
            channel = await this.channelPool.leaseChannel("publish");
        }
        catch(e) {
            this.logger.error("Error leasing channel to publish event: ", e);
            throw e; // maybe handle this transparently ?
        }
        
        try {
          return new Promise((resolve, reject) => {
              this.logger.info("emitting event on channel " + channel.channelNo + ", opts = " + JSON.stringify(opts) + ", payload: ", payload);
              try {
                  this.amqpConnection.basic.publish(channel.channelNo, exchangeName, topic, false, false, (err) =>
                      {
                          if(err) {
                              this.logger.error("Error publishing event on channel " + channel.channelNo + ": ", err);
                              channel.state = ChannelPool.CS_ERROR;
                              this.channelPool.returnChannel(channel.channelNo);
                              reject(err);
                          }
                          else {
                              this.logger.info("Publish for channel " + channel.channelNo + " sent, sending content...");
                              
                              // only resolve once we got the handshake
                              channel.state = ChannelPool.CS_CONTENT;
                              //opts = {"content-type": "application/json", "headers": {"foo": {type:"Long string", data:"bar"}}};
                              opts = {...opts};
                              opts.headers = object2Table(opts.headers);
                              this.amqpConnection.content(channel.channelNo, 'basic', opts, payload, (contentError) => 
                                  {
                                      if(contentError){
                                          this.logger.error("error sending content on channel " + channel.channelNo + " for messageId " + options.messageId + ": ", contentError);
                                          channel.state = ChannelPool.CS_ERROR;
                                          this.channelPool.returnChannel(channel.channelNo);
                                          reject(contentError);
                                      }
                                      else {
                                          this.logger.info("sent content on Channel " + channel.channelNo + ", topic = " + topic);
                                          channel.state = ChannelPool.CS_OPEN;
                                          this.channelPool.returnChannel(channel.channelNo);
                                          resolve();
                                      }
                                  }
                              );
                          }
                      }
                  );
              }
              catch(e) {
                  this.logger.error("Error publishing message on channel " + channel.channelNo + " to exchange " + exchangeName + " with topic " + topic + " and payload = " + payload, e);
                  channel.state = ChannelPool.CS_ERROR;
                  this.channelPool.returnChannel(channel.channelNo);
                  reject(e);
              }
          });
        }
        catch(e) {
            this.logger.error("Error publishing message on channel " + channel.channelNo + ": ", e);
            this.channelPool.returnChannel(channel.channelNo);
            throw e; // maybe handle this transparently ?
        }
        return null;
    }
    
    
    /**
     * Channel needs to be a channel descriptor
     */
    async publish(channel, exchangeName, topic, logger, payload, options) {

        return new Promise((resolve, reject) => {
            this.logger.info("publishing event on channel " + channel.channelNo + ", using headers ", options);
            try {
                this.amqpConnection.basic.publish(channel.channelNo, exchangeName, topic, false, false, (err) =>
                    {
                        if(err) {
                            this.logger.error("Error publishing event on channel " + channel.channelNo + ": ", err);
                            channel.state = ChannelPool.CS_ERROR;
                            reject(err);
                        }
                        else {
                            this.logger.info("Publish for channel " + channel.channelNo + " sent, sending content...");
                            
                            // only resolve once we got the handshake
                            channel.state = ChannelPool.CS_CONTENT;
                            
                            this.amqpConnection.content(channel.channelNo, 'basic', options, payload, (contentError) => 
                                {
                                    if(contentError){
                                        this.logger.error("error sending content on channel " + channel.channelNo + " for messageId " + options.messageId + ": ", contentError);
                                        channel.state = ChannelPool.CS_ERROR;
                                        reject(contentError);
                                    }
                                    else {
                                        this.logger.info("sent content");
                                        channel.state = ChannelPool.CS_OPEN;
                                        resolve();
                                    }
                                }
                            );
                        }
                    }
                );
            }
            catch(e) {
                this.logger.error("Error publishing message on channel " + channel.channelNo + " to exchange " + exchangeName + " with topic " + topic + " and payload = " + payload, e);
                channel.state = ChannelPool.CS_ERROR;
                reject(e);
            }
        });
    }

    getConsumerChannel(topic) {
        for(let c of this.channelPool.channels) {
            //console.log("checking channel ", c);
            if(c.type == "consumer") {
                if(c.consumerTopic == topic) {
                    //console.log("returning channel ", c)
                    return c;
                }
            }
        }
        return null;
    }
    
    /**
     * Returns the channel descriptor that held the subscription after clearing the consumer.
     */
    async unsubscribe(topic) {
        if(this.connectionState != 4){
            this.logger.error("Trying to unsubscribe on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to unsubscribe on unconnected core, connectionState = " + this.connectionState);
        }
        
        
        this.logger.info("Getting channel to unsubscribe " + this.queueName(topic) + "...");
        let channel = this.getConsumerChannel(topic);
        this.logger.info("unsubscribing ", channel.channelNo);
        if(!channel) return null;
        try {
            channel = await this.cancelQueue(channel);
        }
        catch(e) {
            this.logger.error("Error canceling consumer on channel " + JSON.stringify(channel) + ": ", e);
            throw e; // maybe handle this transparently ?
        }
        finally {
            this.channelPool.returnChannel(channel.channelNo);
        }
    }

    /**
     * Channel needs to be a channel descriptor
     */
    async cancelQueue(channel) {

        let eventName = '' + channel.channelNo + ':basic.cancel-ok';
        let queueName = this.queueName(channel.consumerTopic);
        return new Promise((resolve, reject) => {
            this.logger.info("canceling queue with name " + queueName);
            
            try {
                // channel#, queueName, consumerTag, no-local, no-ack, exclusive, no-wait, args
                this.amqpConnection.once(eventName, (channelNo, method, data)  =>
                    {
                        this.logger.info("handshake for canceling queue " + queueName + " via channel " + channel.channelNo + " received, data: ", data);
                        
                        delete channel.consumerTag;
                        delete channel.consumerCallback;
                        delete channel.consumerTopic;
                        
                        this.removeEventListenersByEventName(''+channel.channelNo+':basic.deliver');
                        
                        channel.state = ChannelPool.CS_OPEN;   
                        channel.type = "idle";                        
                        resolve(channel);
                    }
                );
                this.amqpConnection.basic.cancel(channel.channelNo, channel.consumerTag, false, (err) =>
                    {
                        if(err) {
                            this.logger.error("Error canceling channel " + channel.channelNo + ": ", err);
                            channel.state = ChannelPool.CS_ERROR;
                            this.removeEventListenersByEventName(eventName);
                            reject(err);
                        }
                        else {
                            this.logger.info("Cancel for channel " + channel.channelNo + " sent, waiting for handshake...");
                            
                            // only resolve once we got the handshake
                            channel.state = ChannelPool.CS_AWAITING_REPLY;
                        }
                    }
                );
            }
            catch(e) {
                this.logger.error("Error canceling channel " + channel.channelNo, e);
                channel.state = ChannelPool.CS_ERROR;
                reject(e);
            }
        });
    }

    /**
     * Callback gets three args: payload, context, routingkey and is called for each message
     * incoming. If callback throws ro rejects, message is considered NOT processed
     * Only if callback resolves, the message will be acked on the queue.
     */
    async subscribe(topic, callback, opts) {
        if(this.connectionState != 4){
            this.logger.error("Trying to register consumer on unconnected core, connectionState = " + this.connectionState);
            throw new Error("Trying to register consumer on unconnected core, connectionState = " + this.connectionState);
        }
        
        
        this.logger.info("Getting channel for new subscriber on queue " + this.queueName(topic) + "...");
        let channel = null;
        try {
            channel = await this.channelPool.leaseChannel("consumer");
        }
        catch(e) {
            this.logger.error("Error leasing channel to consume queue: ", e);
            throw e; // maybe handle this transparently ?
        }
        this.logger.info("Declaring queue " + this.queueName(topic) + " on channel " + channel.channelNo);
        try {
            await this.declareQueue(channel, topic);
        }
        catch( e ) {
            this.logger.error("Error declaring queue for topic " + topic, e);
            throw e;
        }
        try {
            let exchange = topic.substring(0, topic.indexOf('.'));
            await this.bindQueue(channel, exchange, topic);
        }
        catch( e ) {
            this.logger.error("Error binding queue for topic " + topic, e);
            throw e;
        }
        let consumerChannel = null;
        try {
            consumerChannel = await this.consumeQueue(channel, this.queueName(topic), topic, callback);
        }
        catch( e ) {
            this.logger.error("Error consuming queue for topic " + topic, e);
            throw e;
        }
        return consumerChannel;
    }

    /**
     * Channel needs to be a channel descriptor
     */
    async consumeQueue(channel, queueName, topic, callback) {

        let eventName = '' + channel.channelNo + ':basic.consume-ok';
        return new Promise((resolve, reject) => {
            this.logger.info("consuming queue with name " + queueName);
            
            try {
                // channel#, queueName, consumerTag, no-local, no-ack, exclusive, no-wait, args
                this.amqpConnection.once(eventName, (channelNo, method, data)  =>
                    {
                        channel.state = ChannelPool.CS_CONSUMING;
                        
                        this.logger.info("handshake for consuming queue " + queueName + " via channel " + channel.channelNo + " received, consumer tag: ", data);
                        
                        channel.consumerTag = data['consumer-tag'];
                        channel.consumerCallback = callback;
                        channel.consumerTopic = topic;
                        
                        this.amqpConnection.on(''+channel.channelNo+':basic.deliver', (channelNo, method, deliveryInfo) =>
                            {
                                console.log("new message incoming on channel " + channel.channelNo + ", queue " + queueName + ": ", deliveryInfo);
                                if( channel.state != ChannelPool.CS_CONSUMING) {
                                    this.logger.error("Expected channel to be in consuming state, in fact it is in " + channel.state);
                                }
                                channel.deliveryTag = deliveryInfo['delivery-tag'];
                                channel.deliveryTags[channel.deliveryTag] = new Date(); // this is to verify this delivery was indeed received on the channel 
                                channel.redelivered = deliveryInfo.redelivered;
                                channel.exchange = deliveryInfo.exchange;
                                channel.routingKey = deliveryInfo['routing-key'];
                                channel.state = ChannelPool.CS_CONTENT;
                                if(!deliveryInfo) {
                                    this.logger.warn("Received null deliveryInfo on channel "  + channel.channelNo + ", consumer canceled!");
                                }
                            }
                        );
                        resolve(channel);
                    }
                );
                this.amqpConnection.basic.consume(channel.channelNo, queueName, null, false, false, false, false, {}, (err) =>
                    {
                        if(err) {
                            this.logger.error("Error consuming channel " + channel.channelNo + ": ", err);
                            channel.state = ChannelPool.CS_ERROR;
                            this.removeEventListenersByEventName(eventName);
                            reject(err);
                        }
                        else {
                            this.logger.info("Consume for channel " + channel.channelNo + " sent, waiting for handshake...");
                            
                            // only resolve once we got the handshake
                            channel.state = ChannelPool.CS_AWAITING_REPLY;
                        }
                    }
                );
            }
            catch(e) {
                this.logger.error("Error binding queue " + queueName, e);
                channel.state = ChannelPool.CS_ERROR;
                reject(e);
            }
        });
    }

    /**
     * Queues always belong to the service running event-client, but they can be bound to other services' exchanges
     */
    async bindQueue(channel, exchangeName, topic) {
        let channelNo = channel.channelNo;
        let eventName = ''+channelNo+':queue.bind-ok';
        
        return new Promise((resolve, reject) => {
            let queueName = this.queueName(topic);
            let routingKey = topic;
            this.logger.info("channel = " + channelNo + ", topic = " + topic + ", queueSeparator = " + queueSeparator + ", binding queue with name " + queueName + " to exchange " + exchangeName + " using routing key " + routingKey);
            try {
                this.amqpConnection.once(eventName, (channelNum, method, data) =>
                    {
                        this.logger.info("bind of queue " + queueName + " requested on channel " + channelNo + " confirmed by server on channel " + channelNum);
                        channel.state = ChannelPool.CS_OPEN;
                        resolve(data);
                    }
                );
                this.amqpConnection.queue.bind(channelNo, queueName, exchangeName, routingKey, false, {}, (error) =>
                    {
                        if(error)
                        {
                            this.logger.error("Binding of queue " + queueName + " failed: ", error);
                            channel.state = ChannelPool.CS_ERROR;
                            reject(error);
                        }
                        else {
                            this.logger.info("queue " + queueName + " bound.");
                            channel.state = ChannelPool.CS_AWAITING_REPLY;
                            resolve();
                        }
                    }
                );
            }
            catch(e) {
                this.logger.error("Error binding queue " + queueName, e);
                channel.state = ChannelPool.CS_ERROR;
                reject(e);
            }
        });
    }

    async declareQueue(channel, topic) {
         
        let channelNo = channel.channelNo;
        let eventName = ''+channelNo+':queue.declare-ok';
        // channel#, queueName, passive, durable, exclusive, auto-delete, no-wait, args
        // note that args can be used to control mirroring, as per https://www.rabbitmq.com/ha.html
        // we would need to pass ha-mode: exactly and ha-params; count=1
        return new Promise((resolve, reject) => {
            this.logger.info("declaring queue with name " + this.queueName(topic) + " on channel " + channelNo);
            try {
                //{'ha-mode': {type:'Short string', data:'exactly'}, 'ha-params': {type:'Short string', data:'2'}}
                
                this.amqpConnection.once(eventName, (channelNum, method, data) =>
                    {
                        this.logger.info("declare of queue " + this.queueName(topic) + " requested on channel " + channelNo + " confirmed by server on channel " + channelNum);
                        channel.state = ChannelPool.CS_OPEN;
                        resolve(data);
                    }
                );

                this.amqpConnection.queue.declare(channelNo, this.queueName(topic), false, true, false, 
                                              false, false,{}, (error) => 
                    {
                        if(error)
                        {
                            this.logger.error("Declaration of queue " + this.queueName(topic) + " failed: ", error);
                            channel.state = ChannelPool.CS_ERROR;
                            reject(error);
                        }
                        else {
                            this.logger.info("queue " + this.queueName(topic) + " declared.");
                            channel.state = ChannelPool.CS_OPEN;
                            resolve();
                        }
                    }
                );
            }
            catch(e) {
                this.logger.error("Error declaring queue " + this.queueName(topic), e);
                channel.state = ChannelPool.CS_ERROR;
                reject(e);
            }
        });
    }

    queueName(topic) {
        return this.serviceName + queueSeparator + topic.replace('#',':ANY:');
    }

    removeEventListenersForChannel(amqpConnection, channelNo) {
        let eventNames = amqpConnection.eventNames();
        let eventPrefix = ""+channelNo+":";
        for(let ename of eventNames) {
            if(ename.substring(0, eventPrefix.length) == eventPrefix) {
                this.logger.info("removing event listener " + ename);
                let listeners = amqpConnection.listeners(ename);
                listeners.forEach( (l) => {amqpConnection.removeListener(l)});
            }
        }
    }
    
    removeEventListenersByEventName(ename) {
        let listeners = this.amqpConnection.listeners(ename);
        this.logger.info("removing " + listeners.length + " event listeners for event name " + ename);
        listeners.forEach( (l) => {
            //console.log("listener type " + (typeof l) + ", listener: ", l);
            this.amqpConnection.removeListener(ename, l);
            
        });
    }

    async declareExchange(channel, exchangeName = this.exchangeName, passive = false) {
        
        let eventName = ''+channel.channelNo+':exchange.declare-ok';
        return new Promise((resolve, reject) => {
            try {
                this.amqpConnection.once(eventName, (channelNum, method, data) => 
                    {
                        this.logger.info("declare of exchange " + this.exchangeName + " requested on channel " + channel.channelNo + " confirmed by server on channel " + channelNum);
                        channel.state = ChannelPool.CS_OPEN;
                        resolve(data);
                    }
                );
                
                // channelNo, exchangeName, exchangeType, passive, durable, auto-delete, internal, no-wait, args
                this.amqpConnection.exchange.declare(channel.channelNo, this.exchangeName, 'topic', passive, true,
                    false, false, false, {}, (err) => 
                    {
                        if(err) {
                            this.logger.error(err);
                            channel.state = ChannelPool.CS_ERROR;
                            this.removeEventListenersByEventName(eventName);
                            reject("Failed declaring exchange " + this.exchangeName + ": " + error);
                        }
                        else {
                            this.logger.info("requested exchange declare on " + channel.channelNo + ", waiting on handshake...");
                            channel.state = ChannelPool.CS_AWAITING_REPLY;
                        }
                    }
                );
            }
            catch( e ) {
                channel.state = ChannelPool.CS_ERROR;
                throw e;
            }
        });
    }

    async destroySocket() {
        if (this.socket)
        {
            if(this.socket.destroyed) {
                this.logger.info("socket already destroyed!");
                return Promise.resolve(true);
            }
            let errMsg = "Socket destroyed";
            this.logger.warn("destroying socket, current connectionState " + this.connectionState);
            return new Promise((resolve,reject) => {
                this.socket.on('error', (err) => {
                    this.logger.info("socket error: ",err)
                    if(err && err == errMsg)
                        resolve(true);
                    else reject(err);
                });
                this.socket.destroy(errMsg);
            });
        }
        else return false;
    }

    async amqpConnectionErrorHandler(error) 
    {
        //console.log("this3: ", this);
        this.logger.error("Error on amqp connection: ", error);
        if(1 == 2) 
        {
          this.connectionState = 1;
          this.logger.info("Resetting amqp connection");
          await new Promise( (resolve,reject) => {
              this.amqpConnection.closeAMQPCommunication( async function(err)
              {
                if(err) {
                  this.logger.error("Error closing amqp comms: ", err);
                  await this.destroySocket();
                  this.connectionState = 1;
                  reject(err);
                }
                else {
                  this.logger.error("Closed amqp comms, destroying socket");
                  await this.destroySocket();
                  this.connectionState = 1;
                  resolve();
                }
              });
          });
        }
        console.log("amqpConnectionErrorHandler done");
    }
    
    async reconnect() {
        // we need to establish a connection (as in init)  
        // but then we also need to emit any pending messages
        // and reopen all consumer channels
        this.logger.info("reconnnecting...");
        try {
            await this.connect();
        }
        catch(e) {
            this.logger.error("reconnect attempt failed: ", e);
            throw e;
        }
        
        this.logger.info("re establishing consumers...");
        this.logger.info("reconnect done");
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
            await this.destroySocket();

        this.logger.info("Connecting to " + connectionConfig.host + ":" + connectionConfig.port + "...");

        this.socket = net.connect({
            port: connectionConfig.port,
            host: connectionConfig.host
        });
        this.socket.on('error', async (err) => {
            console.error("Socket error handler: ", err);
            if(err.code == "ECONNRESET") {
                this.logger.error("Connection has been closed by server, destroying socket...");
                this.connectionState = 5; // set to closing
                
                // now we lost the connection, so it means all channels are down and we dont have a valid amqp connection handle
                try {
                    await this.destroySocket();
                }
                catch(e) {
                    this.logger.error("error destroying socket: ", e);
                }
                this.connectionState = 0;
                const reconTimeout = 5000;
                this.logger.info("scheduling reconnect in " + reconTimeout + " ms");
                setTimeout(reconTimeout, this.reconnect);
            }
        });
        
        await new Promise((resolve, reject) => {
            let handleBramqpInit = function(initError, handle)
            {
                //console.log("this2: ", this);
                if (initError) {
                    this.logger.error("Could not initialize bramqp: ", initError);
                    reject(initError);
                }
                else {
                    this.logger.info("adding connection error handler");
                    let logger = this.logger;
                    handle.on('error', this.amqpConnectionErrorHandler.bind(this));
                    handle.on('channel.close', (channel, method, data) => 
                        {
                            this.logger.error('channel ' + channel + ' closed by server');
                        }
                    );
                    resolve(handle);
                }
            }
            //console.log("this: ", this);
            bramqp.initialize(this.socket, 'rabbitmq/full/amqp0-9-1.stripped.extended', 
            handleBramqpInit.bind(this))
        })
        .then( (amqpConnection) => {
            this.amqpConnection = amqpConnection;
            this.logger.info("Connected to " + connectionConfig.host + ":" + connectionConfig.port);
            this.connectionState = 3;
        })
        .catch( async (err) => {
            this.logger.error("Error establishing amqp connection: ", err);
            await destroySocket();
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
            this.logger.info("AMQP Comms started, channel 1 opened, registering content listener..."); 
            this.connectionState = 4;
        })
        .catch( async (err) => {
            this.logger.error("AMQP Comms can't be established, resetting", err);
            await this.destroySocket();
            this.connectionState = 1;
        })
        .then( () => {
            this.logger.info("adding content listener to connection");
            this.registerContentListener();
        });
        if(this.amqpConnection && this.connectionState == 4) {
            this.logger.info("connect() successful");
            return this.amqpConnection;
        }
        this.logger.error("connect() returns null");
        return null;
    }

    async registerContentListener() {
        this.amqpConnection.on('content', (channelNo, classname, properties, content) =>
            {
                console.log("content received on channel " + channelNo + ", classname " + classname + ", properties: " + JSON.stringify(properties) + ", content: ", content);
                // find consumer
                let channel = this.channelPool.channels[channelNo-1];
                this.logger.debug("channel found: ", channel.channelNo);
                if(!channel.state == ChannelPool.CS_CONTENT) {
                    this.logger.error("Expected channel to be in state CS_CONTENT, in fact it is in " + channel.state);
                    throw new Error("Channel " + channelNo + " state error on content, state = " + channel.state);
                }
                let deliveryTag = channel.deliveryTag;
                channel.state = ChannelPool.CS_CONSUMING;
                if(!channel.consumerCallback) {
                    this.logger.error("no consumeCallback on channel " + channelNo);
                    channel.state = ChannelPool.CS_ERROR;
                    throw new Error("Cannot consume content on channel " + channelNo + ": channel has no consumer callback");
                }
                if(!content) {
                    this.logger.warn("Received null event on channel "  + channelNo + ", consumer canceled!");
                    this.channelPool.returnChannel(channel);
                }
                else {
                    this.logger.warn("Forwarding message received on channel "  + channel.channelNo + ", to consumer callback...");
                    try {
                        let headers = properties.headers;
                        let payload = this.config.parser(content);
                        
                        this.logger.info("using headers ", properties);
                        return channel.consumerCallback(payload, headers, channel.routingKey) // in the meantime, channel can consume
                        // more messages and switch between CS_CONSUME and CS_CONTENT
                        .then( () => {
                            this.logger.info("callback on " + channelNo + " succeeded, acking message");
                            this.amqpConnection.basic.ack(channel.channelNo, deliveryTag, (ackErr) => {
                                if(ackErr) {
                                    this.logger.error("Error acking message " + deliveryTag + ": ", ackErr);
                                }
                                else {
                                    this.logger.info("acked message " + deliveryTag + " on channel " + channel.channelNo);
                                    if(channel.deliveryTags[deliveryTag]) {
                                        delete channel.deliveryTags[deliveryTag];
                                    }
                                    else {
                                        throw new Error("Acked message with deliveryTag " + deliveryTag + ", but no such tag on channel!");
                                    }
                                }
                            });
                        })
                        .catch( (e) => {
                            this.logger.error("callback on " + channelNo + " failed");
                        });
                        
                    }
                    catch(e) {
                        this.logger.error("error in callback for channel " + channel + ": ", e);
                    }
                }
            }
        );            
    }

    /**
     * Returns bramqp handle if available
     */
    getAmqpConnection() {
        if(this.connectionState == 4)
            return this.amqpConnection;
        else {
            this.logger.error("can't return amqpConnection, as it's currently in state " + this.connectionState + ", connection: ", this.amqpConnection);
            return null;
        }
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
            this.logger.info("reading config from consul..."); //, this=", this);
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

    const js2rabbitmqTypeMap = {
      "String": "Long string",
      "string": "Long string",
      "number": "Signed 64-bit",
      "float": "64-bit float",
      "Date": "Timestamp"
    };
    
    /**
     *
     *	'Boolean': 0x74, // t
     *	'Signed 8-bit': 0x62, // b
     *	'Signed 16-bit': 0x73, // s
     *	'Signed 32-bit': 0x49, // I
     *	'Signed 64-bit': 0x6c, // l
     *	'32-bit float': 0x66, // f
     *	'64-bit float': 0x64, // d
     *	'Decimal': 0x44, // D
     *	'Long string': 0x53, // S
     *	'Array': 0x41, // A
     *	'Timestamp': 0x54, // T
     *	'Nested Table': 0x46, // F
     *	'Void': 0x56, // V
     *	'Byte array': 0x78, // x
     */
    function object2Table(obj) {
      let results = {};
      for(let k in obj) {
        let v = obj[k];
        let ktype = typeof v;
        //console.log("processing key " + k + ", value " + v + ", type " + ktype);
        if(ktype == "number") {
          if((""+v).indexOf('.') > -1) {
            ktype = "float";
            console.log("setting type to float on " + v);
          }
        }
        else if(ktype == "object") {
          if(v instanceof Date) ktype = "Date";
        }
        let targetType = js2rabbitmqTypeMap[ktype];
        if(!targetType) throw new Error("unable to map value " + v + " for key " + k + " to rabbit mq type. Type " + ktype + " has no map entry.");
        results[k] = {type: targetType, data:v};
      }
      return results;
    }

