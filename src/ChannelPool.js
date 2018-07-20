const extend = require('extend');

class ChannelPool
{
    constructor(config, ecc) {
        this.config = extend(true, { }, ChannelPool.DefaultConfig, config);
        console.log("ChannelPool config: ", this.config);
        this.logger = this.config.logger;
        this.channels = [] // channel number is one-based, so channels[0] is channel#1 etc. 
                           // channel have channelNo = channel number
                           // state: 0 - closed, 1 - opening, 2 - opened, 3 - error, 4 - closing, 5 - reply
        this.ecc = ecc;
        this.state = 0; // 0 - down, 1 - init, 2 - running, 3 - resizing
    }
    
    /**
     * Initializes the pool by creating minIdle channels
     */
    async init() {
        if(this.state != 0) throw new Error("trying to init channel pool that is already in state " + this.state);
        this.state = 1;
        this.channels.push({channelNo: 1, state:ChannelPool.CS_OPEN, type:"mgmt"});
        for(let i = 0; i < this.config.pool.minIdle; i++) {
            let channelNo = i+2;
            try {
                await this.openChannel(channelNo, this.ecc.amqpConnection);
                this.logger.info("opened channel " + channelNo);
            }
            catch(e) {
                this.logger.error("Unable to open channel " + channelNo + ": ", e);
                this.state = 4;
                throw e; // this is happening during init, so caller should handle
            }
            
        }
        this.logger.info("channel pool initialized, current channels: ", this.channels);
        this.state = 2;
    }
    
    /**
     * Will get channel from pool or create new one
     * Returns leased channel
     */
    async leaseChannel(type) {
      let c = this.channels.find( (c) => {return c.type == "idle" && c.state == ChannelPool.CS_OPEN});
      if(c) {
          c.type = type;
          return c;
      }
      
      this.logger.info("no open idle channel found, checking openable ones...");
      c = this.channels.find( (c) => {return c.type == "idle" && (c.state == ChannelPool.CS_CLOSED || c.state == ChannelPool.CS_ERROR) });
      if(c) {
          c.type == "preparing"; // make sure while we are opening it it isnt opened also by concurrent process
          try {
              c = await this.openChannel(c.channelNo);          
          }
          catch(e) {
              this.logger.error("failed opening channel ", c, ": ", e);
              throw new Error("failed opening " + JSON.stringify(c) ,e);
          }
          c.type = type;
          return c;
      }
      
      // now we really need a new one
      c = await this.addChannel();
      c.type = type;
      this.checkAndResize();
      return c;
    }
    
    async addChannel() {
      let c = {channelNo:this.channels.length+1, type:"preparing", state:ChannelPool.CS_CLOSED};
      this.logger.info("enlarging channel pool, adding ", c);
      this.channels.push(c);
      try {
          c = await this.openChannel(c.channelNo);          
      }
      catch(e) {
          this.logger.error("failed opening channel ", c, ": ", e);
          throw new Error("failed opening " + JSON.stringify(c) ,e);
      }
      return c; 
    }
    
    checkAndResize() {
        console.log("check and resize: ", this.channels);
        let idleChannels = [];
        let preparingChannels = 0;
        let closeableChannels = [];
        for(let i = 0; i < this.channels.length; i++) {
            let c = this.channels[i];
            if( (c.type == "idle") && (c.state == ChannelPool.CS_OPEN || c.state == ChannelPool.CS_OPENING)) {
                idleChannels.push(c);
                if(c.state == ChannelPool.CS_OPEN){
                    closeableChannels.push(c);
                }
            }
            if( (c.type == "preparing") && (c.state == ChannelPool.CS_OPEN || c.state == ChannelPool.CS_OPENING)) {
                preparingChannels++;
            }
        }
        this.logger.info("current closable channels: " + closeableChannels.length + ", idleChannels: " + idleChannels.length + ", minIdle: " + this.config.pool.minIdle + ", maxIdle: " + this.config.pool.maxIdle);
        if (idleChannels.length + preparingChannels < this.config.pool.minIdle) {
            this.logger.info("pool resize found " + idleChannels.length + " idle channels, need min " + this.config.pool.minIdle + " ones, opening...");
            for(let i = 0; i < this.config.pool.minIdle - idleChannels.length;i++) {
                let nc = null;
                try {
                    c = addChannel(); // dont await, otherwise we get a race
                }
                catch(e) {
                    this.logger.error();
                }
            }  
        }
        else if (idleChannels.length > this.config.pool.maxIdle) {
            let channelsToClose = idleChannels.length - this.config.pool.maxIdle;
            this.logger.info("pool resize found " + idleChannels.length + " idle channels, may have max " + this.config.pool.maxIdle + " ones, closing...");          
            for(let i = 0; i < closeableChannels.length && i < channelsToClose; i++) {
                this.closeChannel(closeableChannels[closeableChannels.length-1-i].channelNo); // dont wait on it
            }
        }
    }
    
    /** 
     * Will set the returned channel to idle so it can be leased again.
     */
    returnChannel(channelNo) {
        this.logger.info("returning channel " + channelNo);
        let channel = this.channels[channelNo-1];
        if(!channel) {
            this.logger.info("Channels: ", this.channels);
            throw new Error("Can't return channel " + channelNo + " because it doesn't exist.");
        }
        if(channel.state == ChannelPool.CS_AWAITING_REPLY) { 
            this.logger.info("channel " + channelNo + " is pending reply on being returned, closing it.");
            this.channels[channelNo-1] = {type:"na", channelNo:channelNo, state:ChannelPool.CS_CLOSING};
            closeChannel(channelNo); // dont wait on it here
        }
        else {
            this.channels[channelNo-1] = {type:"idle", channelNo:channelNo, state:ChannelPool.CS_OPEN, deliveryTags:{}};
        }
        
        // we might need to free resources later if we introduce maxIdle config
        this.checkAndResize();
        
        return Promise.resolve(channelNo);
    }
    
    /** 
     * Immediately sets the channel state to closing, then returns a promise that resolves once the close was handshaked
     * by the server or rejects if an error occurred during the closing.
     */
    closeChannel(channelNo) {
        this.logger.info("trying to close channel " + channelNo);
        let channel = this.channels[channelNo-1];
        if(!channel)
            throw new Error("Can't close channel that doesn't exist.");
        if(!(channel.state == ChannelPool.CS_AWAITING_REPLY || channel.state == ChannelPool.CS_OPEN || channel.state == ChannelPool.CS_ERROR)) {
            this.logger.warn("close channel " + channelNo + " called while channel in unexpected state " + channel.state);
            throw new Error("Channel " + channelNo + " close rejected cause current state is " + channel.state);
        }
        
        channel.state = ChannelPool.CS_CLOSING;
        
        return new Promise( (resolve, reject) =>
            {
               
                let amqpConnection = this.ecc.amqpConnection;
                //console.log("amqpConnection: ", this.ecc.amqpConnection);

                let topic = ''+channelNo+':channel.close-ok';

                amqpConnection.once(topic, (channelNo, method, data) => 
                    {
                        this.logger.info("received " + topic + ", channel closed successfully");
                        this.channels[channelNo-1].state = ChannelPool.CS_CLOSED;
                        resolve(this.channels[channelNo-1]);
                    }
                );

                
                amqpConnection.channel.close(channelNo, "close-ok", "close accepted", null, null, (err) => 
                    {
                        if(err)
                        {
                            this.logger.error("error closing channel " + channelNo + ": ", err);
                            channel.state = ChannelPool.CS_ERROR;
                            amqpConnection.removeAllListeners(topic);
                            reject(err);
                        }
                        else this.logger.info("channel " + channelNo + " close requested");
                    }
                );
            }
        );
    }
    
    /**
     * Returns promise that resolves to opened channel or reject with error
     */
    async openChannel(channelNo) {
        this.logger.info("trying to open new channel " + channelNo);
        return new Promise( (resolve, reject) =>
            {
                let channel = this.channels[channelNo-1];

                if(!channel) {
                    this.logger.info("openChannel(" + channelNo + "), channel not in pool yet, adding...");
                    channel = this.channels[channelNo-1] = {channelNo: channelNo, state:ChannelPool.CS_OPENING, type:"idle", deliveryTags:{}};
                }
                else {
                    this.logger.info("openChannel(" + channelNo + "), channel found: ", channel);
                    if(!(channel.state == ChannelPool.CS_CLOSED || channel.state == ChannelPool.CS_ERROR)) {
                        throw new Error("Channel " + channelNo + " is not closed, can't open: ", channel);
                    }
                }
                channel.state = ChannelPool.CS_OPENING;
                
                let amqpConnection = this.ecc.amqpConnection;
//                console.log("amqpConnection: ", this.ecc.amqpConnection);

                let topic = ''+channelNo+':channel.open-ok';
                amqpConnection.once(topic, (channelNo, method, data) => 
                    {
                        this.logger.info("received " + topic + ", channel openend successfully");
                        this.channels[channelNo-1].state = ChannelPool.CS_OPEN;
                        resolve(this.channels[channelNo-1]);
                    }
                );

                amqpConnection.channel.open(channelNo, (err) => 
                    {
                        if(err)
                        {
                            this.logger.error("error opening channel " + channelNo + ": ", err);
                            amqpConnection.removeAllListeners(topic);
                            this.channels[channelNo-1].state = ChannelPool.CS_ERROR;
                            this.channels[channelNo-1].type = "idle";
                            reject(err);
                        }
                        else this.logger.info("channel " + channelNo + " open requested");                      
                    }
                );
                
                amqpConnection.on(''+channelNo+':channel.close', (channel, method, data) =>
                    {
                        this.logger.info("received channel close on channel " + channelNo);
                        this.channels[channelNo-1].state = ChannelPool.CS_CLOSING;
                        // shut down lingering consumers ?
                        
                        this.ecc.amqpConnection.channel['close-ok'](channelNo, (err) => 
                            {
                                if(err) {
                                    this.logger.error("error seding close-ok on channel " + channelNo + ": ", err);
                                    channel.state = ChannelPool.CS_ERROR;
                                }
                                else {
                                    this.logger.info("close-ok handshake sent to server on channel " + channelNo);
                                    channel.state = ChannelPool.CS_CLOSED;
                                }
                            }
                        );
                    }
                );
            }
        );
    }
}
ChannelPool.DefaultConfig = {
    pool: { minIdle: 1, maxIdle: 2 }
}
ChannelPool.CS_CLOSED = 0;
ChannelPool.CS_OPENING = 1;
ChannelPool.CS_OPEN = 2;
ChannelPool.CS_ERROR = 3;
ChannelPool.CS_CLOSING = 4;
ChannelPool.CS_AWAITING_REPLY = 5;
ChannelPool.CS_CONSUMING = 6;
ChannelPool.CS_CONTENT = 7;


module.exports = ChannelPool;