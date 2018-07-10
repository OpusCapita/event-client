const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');

// const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

async function main() {
    await configService.init({logger : Logger.DummyLogger});

    const config = EventClient.DefaultConfig.consul;
    const endpoint = await configService.getEndPoint(config.mqServiceName);
    const username = config.mqUserKey && await configService.get(config.mqUserKey);
    const password = config.mqPasswordKey && await configService.get(config.mqPasswordKey);

    const consulOverride = { };
    consulOverride.host = endpoint.host;
    consulOverride.port = endpoint.port;
    consulOverride.username = username;
    consulOverride.password = password;

    const client = new EventClient({logger: new Logger()});

    const routingKey = 'event-client.Test';

    await client.init();

    await client.subscribe(routingKey, async (payload, context, key) => {
        console.log(payload);

        return true;
    });

    global.ec = client;

    setInterval( async () => {
        console.log('Emitting event ...');

        let result = await client.emit(routingKey, {message: new Date()});
        // TODO result should be false or throw if connection is blocked
    }, 30000);
}

try {
    main();
} catch (e) {
    /* handle error */
    console.log(e);
}
