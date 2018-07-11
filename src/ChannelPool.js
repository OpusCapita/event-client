const extend = require('extend');

class ChannelPool
{
    constructor(config, ecc) {
        this.config = extend(true, { }, ChannelPool.DefaultConfig, config);
        this.logger = this.config.logger;
        this.size = 0;
        this.channels = [] // channel number is one-based, so channels[0] is channel#1 etc. 
                           // channel have channelNo = channel number
                           // state: 0 - closed, 1 - opening, 2 - opened, 3 - error, 4 - closing, 5 - reply
        this.ecc = ecc;
    }
    
    /**
     * Initializes the pool by creating minIdle channels
     */
    async init() {
        this.channels.push({channelNo: 1, state:ChannelPool.CS_OPEN, type:"mgmt"});
        for(let i = 0; i < this.config.pool.minIdle; i++) {
            let channelNo = i+2;
            try {
                await this.openChannel(channelNo, this.ecc.amqpConnection);
                this.logger.info("opened channel " + channelNo);
            }
            catch(e) {
                this.logger.error("Unable to open channel " + channelNo + ": ", e);
                throw e; // this is happening during init, so caller should handle
            }
            
        }
        this.logger.info("channel pool initialized, current channels: ", this.channels);
    }
    
    /**
     * Will get channel from pool or create new one
     * Returns leased channel
     */
    async leaseChannel(type) {
      // find next idle channel
      let leasedChannel = null;
      let channelNo = -1;
      let idleCount = 0;
      let openableChannels = [];
      for(let i = 0; i < this.channels.length; i++) {
          let channel = this.channels[i];
          if(channel.type == "idle") {
              if(channel.state == ChannelPool.CS_OPEN) {
                  if(channelNo == -1) {
                      channel.type = type;
                      channelNo = channel.channelNo;
                      leasedChannel = channel;
                  }
                  else idleCount++;
              }
          }
          if(channel.state == ChannelPool.CS_CLOSED){
              this.logger.info("found channel " + channel.channelNo + " in closed state");
              openableChannels.push[channel.channelNo];
          }
          else if (channel.state == ChannelPool.CS_ERROR) {
              this.logger.info("found channel " + channel.channelNo + " in error state");
              openableChannels.push[channel.channelNo];
          }
      }
      let channelsToOpen = this.config.pool.minIdle - idleCount;
      if(channelNo == -1) channelsToOpen++;
      
      
      let channel = null;
      this.logger.info("going to open " + channelsToOpen + " new channels...");
      let i = 0;
      while(channelsToOpen > 0) {
      //for(let i = this.channels.lenght; i < this.channels.length + channelsToOpen; i++) {
          let channelNum = -1;
          if(i < openableChannels.length)
              channelNum = openableChannels[i];
          else 
              channelNum = this.channels.length + (i-openableChannels.length) + 1;
          try {
              channel = await this.openChannel(channelNum);
              if(channelNo == -1) {
                  leasedChannel = channel;
                  channelNo = channel.channelNo;
              }
          }
          catch( e ) {
              this.logger.error("error opening new channel " + channelNum, e);
              throw e;
          }
          channelsToOpen--;
      }
      this.logger.info("channels opened");
      leasedChannel.type = type;
      leasedChannel.leasedOn = new Date();
      return leasedChannel;
    }
    
    /** 
     * Will set the returned channel to idle so it can be leased again.
     */
    async returnChannel(channelNo) {
        let channel = this.channels[channelNo-1];
        if(!channel)
            throw new Error("Can't return channel that doesn't exist.");
        if(channel.state == CS_AWAITING_REPLY) { 
            this.logger.info("channel " + channelNo + " is pending reply on close");
            this.channels[channelNo-1] = {type:"na", channelNo:channelNo, state:CS_CLOSING};
            closeChannel(channelNo); // dont wait on it here
        }
        else {
            this.channels[channelNo-1] = {type:"idle", channelNo:channelNo, state:CS_OPEN};
        }
        
        // we might need to free resources later if we introduce maxIdle config
        return Promise.resolve(channelNo);
    }
    
    async closeChannel(channelNo) {
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
                            reject(err);
                        }
                        else this.logger.info("channel " + channelNo + " close requested");
                    }
                );
            }
        );
    }
    
    /**
     * Returns promis that resolves to opened channel or reject with error
     */
    async openChannel(channelNo) {
        this.logger.info("trying to open new channel " + channelNo);
        return new Promise( (resolve, reject) =>
            {
                let channel = this.channels[channelNo-1];

                if(!channel)
                    channel = this.channels[channelNo-1] = {channelNo: channelNo, state:ChannelPool.CS_OPENING, type:"idle"};
                else if(!(channel.state == ChannelPool.CS_CLOSED || channel.state == ChannelPool.CS_ERROR)) {
                    throw new Error("Channel " + channelNo + " is not closed, can't open: ", channel);
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
                        
                        this.amqpConnection.channel['close-ok'](channelNo, (err) => 
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
    pool: { minIdle: 2 }
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