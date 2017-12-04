# ocbesbn-event-client

This module provides simplified access to the publish/subscribe system provided by Message Queue server. It uses Consul in order to determine the required Message Queue server endpoint and further configurations. To have a look at the full API, please visit the related [wiki page](https://github.com/OpusCapita/event-client/wiki).

### Minimum setup
First got to your local code directory and run:
```
npm install ocbesbn-event-client
```
To go with the minimum setup, you need to have access to a running **Consul server** to get your endpoint configuration for Message Queue server. In addition, a **Message Queue server** is required which has to be registered inside Consul. If Message Queue password **authentication** is required, Consul has to provide the configuration key **{{your-service-name}}/mq/password** where *{{your-service-name}}* is the least name of the directory your code runs in. If authentication is not used, you can set the **consul.mqPasswordKey** to null or false when creating a new instance of EventClient.

If all this is set up, go to you code and add the following lines:

```JS
const EventClient = require('ocbesbn-event-client');

var events = new EventClient({ consul : { host : '{{your-consul-host}}' } });

// Subscribe to a channel by name.
events.subscribe('my-channel', console.log).then(() => events.emit('my-channel', 'Hello, world!'));
// - OR -
// Subscribe to a channel by pattern.
events.subscribe('my-channel.#', console.log).then(() => events.emit('my-channel.sub-channel', 'Hello, world!'));

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
        mqServiceName  : 'mq',
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
```
