package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

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
                System.out.println(record.value());
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(record.value());
                Iterator<String> keys = json.keys();
                while(keys.hasNext()) {
                    String key = keys.next();
                    if (json.get(key) instanceof JSONObject) {

                        System.out.println(json.get(key));
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {

            consumer.close();
        }
    }

}
