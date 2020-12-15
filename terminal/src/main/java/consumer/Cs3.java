package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Cs3 {
    public static void consumeCs3(String brokers, String groupId, String topicName) {

        KafkaConsumer<String, String> consumer;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        ArrayList<String> sqlData = new ArrayList<String>();
        try {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

            }
        } finally {

            consumer.close();
        }
    }

}
