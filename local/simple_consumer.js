const {Kafka, logLevel} = require('kafkajs');

const kafka = new Kafka({
    logLevel: logLevel.DEBUG,
    brokers: ['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
    clientId: 'example-consumer',
});

const topic = 'topic-test';
const consumer = kafka.consumer({groupId: 'test-group'});

global.ec1 = consumer;

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({topic});
    await consumer.run({
        // eachBatch: async ({ batch }) => {
        //   console.log(batch)
        // },
        eachMessage: async ({topic, partition, message}) => {
            const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
            console.log(`- ${prefix} ${message.key}#${message.value}`);
        }
    });
};

run().catch(e => console.error(`SimpleConsumer: ${e.message}`, e));
