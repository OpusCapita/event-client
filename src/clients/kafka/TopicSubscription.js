/**
 * Subscribed topics registry used by all KafkaClient instances.
 */

const subscribedTopics = new Map();

module.exports = subscribedTopics;
