# ocbesbn-event-client

This module provides simplified access to the publish/subscribe system provided by AMQP server. It uses Consul in order to determine the required AMQP server endpoint and further configurations. To have a look at the full API, please visit the related [wiki page](https://github.com/OpusCapita/event-client/wiki).

### Minimum setup
First got to your local code directory and run:
```
npm install ocbesbn-event-client
```
To go with the minimum setup, you need to have access to a running **Consul server** to get your endpoint configuration for AMQP server. In addition, a **AMQP server** is required which has to be registered inside Consul. If Mq password **authentication** is required, Consul has to provide the configuration key **{{your-service-name}}/amqp/password** where *{{your-service-name}}* is the least name of the directory your code runs in. If authentication is not used, you can set the **consul.MqPasswordKey** to null or false when creating a new instance of MqEvents.

If all this is set up, go to you code and add the following lines:

```JS
const MqEvents = require('ocbesbn-event-client');

var events = new MqEvents({ consul : { host : '{{your-consul-host}}' } });

// Subscribe to a channel by name.
events.subscribe('my-channel', console.log).then(() => events.emit('Hello, world!', 'my-channel'));
// - OR -
// Subscribe to a channel by pattern.
events.subscribe('my-channel.#', console.log).then(() => events.emit('Hello, world!', 'my-channel.sub-channel'));

// unsubscribe from a particular key
events.unsubscribe('my-channel');

// unsubscribe from a pattern
events.unsubscribe('my-channel.#');
```

### Default configuration

The default configuration object provides hints about what the module's standard behavior is like.

```JS
{
    serializer : JSON.stringify,
    parser : JSON.parse,
    queueName: configService.serviceName, // name of the service
    consul : {
        host : 'consul',
        MqServiceName  : 'mq',
        MqUserKey: 'mq/user',
        MqPasswordKey : 'mq/password'
    },
    context : {
    }
}
```
