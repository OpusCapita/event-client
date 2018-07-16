const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');
const {EventClient} = require('../lib');

// const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

async function main() {
    await configService.init({logger: Logger.DummyLogger});

    const config = EventClient.DefaultConfig.consul;
    const endpoint = await configService.getEndPoint(config.mqServiceName);
    const username = config.mqUserKey && await configService.get(config.mqUserKey);
    const password = config.mqPasswordKey && await configService.get(config.mqPasswordKey);

    const consulOverride = {};
    consulOverride.host = endpoint.host;
    consulOverride.port = endpoint.port;
    consulOverride.username = username;
    consulOverride.password = password;

    const client = new EventClient({logger: new Logger()});
    global.ec = client;

    const routingKey1 = 'event-client.testone';
    const routingKey2 = 'event-client.testtwo';

    let rxTx = [];


    // Also works w/o call to init
    await client.init();

    await client.subscribe(routingKey1, async (payload) => {
        console.log('Received event on ' + routingKey1);
        console.log(payload);

        rxTx.pop();

        return true;
    });


    await client.subscribe(routingKey2, async (payload) => {
        console.log('Received event on ' + routingKey2);
        console.log(payload);

        rxTx.pop();

        return true;
    });

    setInterval(async () => {
        console.log('Emitting event ...');

        let result = client.emit(routingKey1, {message: new Date()});
        // TODO result should be false or throw if connection is blocked
        rxTx.push(result);

        console.log(`Emit result is: ${result}`);
        console.log(`RxTx size = ${rxTx.length}`);
    }, 10000);

    return true;
}

try {
    main();
} catch (e) {
    /* handle error */
    console.log(e);
    debugger;
}
