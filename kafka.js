const { Kafka } = require("kafkajs");
const config = require("./config/config.json");

const kafka = new Kafka(config.KAFKA);

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test-group"});

module.exports = { kafka: kafka, producer: producer, consumer: consumer };