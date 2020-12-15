const { Kafka } = require("kafkajs");
const config = require("./config/config.json");

const kafka = new Kafka(config.KAFKA);
const admin = kafka.admin();

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group"});

// Create topics if not exists
(async () => {
    try {
        await admin.createTopics({
            topics: [
                { topic: "topic1" },
                { topic: "topic2" },
                { topic: "topic3" },
            ],
        });
    } catch (err) {
        console.warn(err);
    }
})();

module.exports = { kafka: kafka, producer: producer, consumer: consumer };