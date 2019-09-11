const configService = require('@opuscapita/config');
const Logger = require('@opuscapita/logger');
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
    const client2 = new EventClient({logger: new Logger()});

    global.ec = client;
    global.ec2 = client2;

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

    let cnt = 0;
    let hasThrown = false;
    let interval = setInterval(async () => {
        let result = null;
        try {
            // if (!hasThrown) await client.pubChannel.channel.checkQueue('benis' + Math.ceil(Math.random() * 1000));
            result = await client.emit(routingKey1, {message: new Date()}, null, {ttl: 10000});
        } catch (e) {
            hasThrown = true;
            console.error('Exception in main loop:');
            console.error(e);
        }
        rxTx.push(result);
        cnt++;
        console.log(`Emit result is: ${result}`);
        console.log(`RxTx size = ${rxTx.length}`);

        if (cnt >= 100) {
            console.log('Shutting down');
            clearInterval(interval);

            await client.dispose();
        }
    }, 10000);

    return true;
}

try {
    main();
    console.log('Done');
} catch (e) {
    /* handle error */
    console.log(e);
    debugger;
}
