const extend = require('extend');

const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');

const {EventClient} = require('../src');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

function main() {

    return new Promise(async (resolve) => {
        await configService.init({logger: Logger.DebugLogger});

        await sleep(2000); // Wait for remote debugger

        const config = {
            sendWith: 'kafka',
            serviceName: 'event-client',
            consumerGroupId: process.env.CONSUMER_GROUP_ID || 'event-client',
            logger: new Logger(),
            consulOverride: {
                // kafkaHost: 'kafka',
                // kafkaPort: 9092
                hosts: 'kafka1:9092,kafka2:9093,kafka3:9094'
            }
        };

        const cs1 = new EventClient(config);
        await cs1.init();
        global.ec1 = cs1;

        // cs1._kafkaClient.producer._producer.producer.on('delivery-report', (err, report) => {
        //     if (err) {
        //         console.log('> Delivery error report', err);
        //     } else {
        //         console.log('> Delivery successful.');
        //     }

        //     if (report && report.key) {
        //         const key = Buffer(report.key).toString();

        //         const idx = messagesSent.findIndex((m => m.key === key));

        //         if (idx && idx >= 0) {
        //             console.log(`> Message with key ${key} found.`);
        //             messagesSent[idx].delivered = err !== null ? false : true;
        //         } else {
        //             console.log(`> Message with key ${key} not found!`);
        //         }

        //     }

        //     deliveryReports.push({err, report});
        // })

        debugger;

        let cnt = 0;
        global.sendFn = async () => {
            console.log('Publishing #', cnt);
            await cs1.publish('event-client.kdlal.test1', `${Date.now()} - ${cnt++}`, {'custom': 'context'});
            return true;
        };

        // console.log('::DEBUG::', await cs1._kafkaClient.producer._producer.getTopicMetadata('event-client.kdlal'));

        // const subRes1 = await cs1.subscribe('event-client.kdlal.test1', (message, headers, topic, routingKey) => {
        // const subRes1 = await cs1.subscribe('test-topic', (message, headers, topic, routingKey) => {
        //     console.log('CS1: Received message: ', message, ' | ', headers, ' | RoutingKey: ', routingKey);
        //     return true;
        // }).catch(e => {
        //     console.error(e);
        // });

        // const subRes2 = await cs1.subscribe('event-client.debug.test2', (message, headers, topic, routingKey) => {
        //     console.log('CS1: Received message: ', message, ' | ', headers, ' | RoutingKey: ', routingKey);
        //     return true;
        // }).catch(console.error);

        // await cs1.subscribe('event-client.debug#', (message, headers, topic, routingKey) => {
        //     console.log('CS1: Received message: ', message, ' | ', headers, ' | RoutingKey: ', routingKey);
        //     return true;
        // }).catch(console.error);

        // await cs1.subscribe('event-client.*.*', (message, headers, topic, routingKey) => {
        //     console.log('CS1: Received message with two wildcards: ', message, ' | ', headers, ' | RoutingKey: ', routingKey);
        //     return true;
        // }).catch(console.error);

        // await cs1.subscribe('xxx.event-client.*.*', (message, headers, topic, routingKey) => {
        //     console.log('CS1: Received message with two wildcards: ', message, ' | ', headers, ' | RoutingKey: ', routingKey);
        //     return true;
        // }).catch(console.error);

        // await cs1.subscribe('beta', (message) => {
        //     console.log('Main: Received message: ', message);
        // });

        // const cs2 = new EventClient(extend(true, config, {consumerGroupId: 'beta'}));
        // await cs2.init();

        // await cs2.subscribe('event-client.#', (message, headers) => {
        //     console.log('CS2: Received message: ', message, '-', headers);
        //     return true;
        // }).catch(console.error);


        // setInterval(() => {
        //     console.log('Publishing #', cnt);
        //     cs1.publish('event-client.debug.subone', `${Date.now()} - ${cnt++}`, {'custom': 'context'});
        // }, 6000);


        // setInterval(async () => {
        //     try {
        //         cs1.checkHealth()
        //             .then(result => console.log('CS1 Health: ', result))
        //             .catch(e => console.log('Failed to check health with exception: ', e.message));
        //         cs2.checkHealth()
        //             .then(result => console.log('CS2 Health: ', result))
        //             .catch(e => console.log('Failed to check health with exception: ', e.message));
        //     } catch (e) {
        //         debugger;
        //     }
        // }, 5000);

        // setInterval(() => {
        //     // console.log('keepalive');
        //     sendFn();
        // }, 5000);

        resolve(true);
    });

}

try {
    main()
        .then(() => console.log('Setup done ...'));
} catch (e) {
    /* handle error */
    console.log(e);
}

