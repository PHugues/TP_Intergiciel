package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;
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
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(record.value());
                if(json.get("totaldeaths") != null)System.out.println("Total morts : "+json.get("totaldeaths"));
                if(json.get("newrecovered") != null)System.out.println("Nouveaux cas remis : "+json.get("newrecovered"));
                if(json.get("totalconfirmed") != null)System.out.println("Total confirmés : "+json.get("totalconfirmed"));
                if(json.get("newconfirmed") != null)System.out.println("Nouveaux cas confirmés : "+json.get("newconfirmed"));
                if(json.get("newdeaths") != null)System.out.println("Nouveaux morts : "+json.get("newdeaths"));
                if(json.get("totalrecovered") != null)System.out.println("Total remis : "+json.get("totalrecovered"));
                if(json.get("avgdeaths") != null)System.out.println("Moyenne des cas confirmés sum(pays)/nb(pays) : "+json.get("avgdeaths"));
                if(json.get("avgconfirmed") != null)System.out.println("Moyenne des Décès sum(pays)/nb(pays) : "+json.get("avgconfirmed"));
                if(json.get("deaths_pourcent") != null)System.out.println("Pourcentage de Décès par rapport aux cas confirmés : "+json.get("deaths_pourcent"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {

            consumer.close();
        }
    }

}
