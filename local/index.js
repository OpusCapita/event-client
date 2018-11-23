const configService = require('@opuscapita/config');
const Logger = require('ocbesbn-logger');

const {EventClient} = require('../src');

const extend = require('extend');

const sleep = (millis) => new Promise(resolve => setTimeout(resolve, millis));

function main() {

    return new Promise(async (resolve) => {
        await configService.init({logger: Logger.DebugLogger});

        await sleep(2000); // Wait for remote debugger

        const config = {
            serviceName: 'event-client',
            consumerGroupId: process.env.CONSUMER_GROUP_ID || 'event-client',
            logger: new Logger(),
            consulOverride: {
                kafkaHost: 'kafka3',
                kafkaPort: 9094
                // hosts: 'kafka1:9092,kafka2:9093,kafka3:9094'
            }
        };

        const cs1 = new EventClient(config);
        // const cs2 = new EventClient(extend(true, config, {consumerGroupId: 'beta'}));

        await cs1.init();
        // // await cs2.init();

        global.ec1 = cs1;

        await cs1.subscribe('^pattern.*.com', (message, headers) => {
            console.log('CS1: Received message: ', message, '-', headers);
        });

        // await cs2.subscribe('^pattern.*', (message, headers) => {
        //     console.log('CS2: Received message: ', message, '-', headers);
        // });

        // // await cs1.subscribe('beta', (message) => {
        // //     console.log('Main: Received message: ', message);
        // // });

        let cnt = 0;
        setInterval(() => {
            console.log('Publishing #', cnt);
            cs1.publish('pattern.oc.com', `${Date.now()} - ${cnt++}`, {'custom': 'context'});
        }, 5000);

        // setInterval(async () => {
        //     cs1.checkHealth()
        //         .then(result => console.log('CS1 Health: ', result))
        //         .catch(e => console.log('Failed to check health with exception: ', e.message));
        //     cs2.checkHealth()
        //         .then(result => console.log('CS2 Health: ', result))
        //         .catch(e => console.log('Failed to check health with exception: ', e.message));
        // }, 5000);

        // setInterval(() => {
        //     cs1.publish('beta', `${Date.now()} - ${cnt++}`);
        // }, 60000);

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

