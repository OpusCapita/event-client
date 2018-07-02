# EventClient

This module provides simplified access to the publish/subscribe system provided by a Message Queue server. It uses Consul in order to determine the required server endpoint and further configuration. To have a look at the full API, please visit the related [wiki page](https://github.com/OpusCapita/event-client/wiki).

### Minimum setup
First got to your local code directory and run:
```
npm install @opuscapita/event-client
```
To go with the minimum setup, you need to have access to a running **Consul server** to get your endpoint configuration for Message Queue server. In addition, a **Message Queue server** is required which has to be registered inside Consul. If Message Queue password **authentication** is required, Consul has to provide the configuration key **{{your-service-name}}/mq/password** where *{{your-service-name}}* is the least name of the directory your code runs in. If authentication is not used, you can set the **consul.mqPasswordKey** to null or false when creating a new instance of EventClient.

> The basic implementation requirements define, that the format of an **event's name** has to be **serviceName.domainName.eventType.eventDetail** where **serviceName** defines the name of the **exchange** used. If the exchange does not exist, an error will be thrown by the *subscribe()* method. Therefor please **make sure**, the **exchange exists** by either creating it manually or bringing up the service that raises the event as the *emit()* method will create the exchange for the service it is called in.

#### Common subscription

If all this is set up, go to you code and add the following lines:

```JS
const EventClient = require('@opuscapita/event-client');

(async () =>
{
    const events = new EventClient({ consul : { host : '{{your-consul-host}}' } });

    // Subscribe to a channel by name.
    await events.subscribe('my-service.my-channel', console.log);
    await events.emit('my-service.my-channel', 'Hello, world!');

    // - OR -
    // Subscribe to a channel by pattern.
    await events.subscribe('my-service.my-channel.#', console.log);
    await events.emit('my-service.my-channel.sub-channel', 'Hello, world!');

    // unsubscribe from a particular key
    await events.unsubscribe('my-service.my-channel');

    // unsubscribe from a pattern
    await events.unsubscribe('my-service.my-channel.#');

    await events.dispose();
})();
```

#### Fetching single message

EventClient is also capable of fetching single events (messages) from a queue. This is done by subscribing to a queue without a callback and then fetching messages manually one by one.

```JS
const EventClient = require('@opuscapita/event-client');

(async () =>
{
    const events = new EventClient();

    await events.subscribe('my-service.my-channel');
    await events.emit('my-service.my-channel', 'Hello, world!');

    // With automatic acknowledgement.
    let message = await events.getMessage('my-service.my-channel');

    console.log(message);


    await events.emit('my-service.my-channel', 'Hello, world!');

    // With manual acknowledgement.
    message = await events.getMessage('my-service.my-channel', false);
    await events.ackMessage(message);

    console.log(message);

    await events.dispose();
})();
```

### Default configuration

The default configuration object provides hints about what the module's standard behavior is like.

```JS
{
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
```

### Testing

local docker-compose comes with a two node rabbitmq cluster that can be used to 
test cluster node failure conditions.
To bring down a node curl it on port 6666 for rabbit1 and 6667 for rabbit2 which will take them down...

