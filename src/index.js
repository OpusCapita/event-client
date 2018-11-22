const {RabbitClient} = require('./clients/rabbitmq/');
const {KafkaClient}  = require('./clients/kafka/');


class EventClient {
    constructor()
    {
        return true;
    }

    /** *** PUBLIC *** */

    /**
     * Delegates to publish().
     *
     * @async
     * @function emit
     */
    async emit()
    {
    }

    /** *** PRIVATE *** */

    get _rabbitClient()
    {
    }

    get _kafkaClient()
    {
    }
}

module.exports = EventClient;
