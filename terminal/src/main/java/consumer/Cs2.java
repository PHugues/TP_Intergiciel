package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Cs2 {
    public static String consumeCs2(String brokers, String groupId, String topicName) {

        KafkaConsumer<String, String> consumer;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","earliest");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        String sqlData = "";
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String[] split = record.value().split(" ");
                switch(split[0]) {
                    case "Get_global_values":
                        sqlData = "SELECT * FROM world";
                        break;
                    case "Get_country_values":
                        sqlData = "SELECT * FROM countries WHERE countrycode = "+ split[1] ;
                        break;
                    case "Get_confirmed_avg":
                        sqlData = "SELECT avg(TotalConfirmed) as avgconfirmed FROM countries";
                        break;
                    case "Get_deaths_avg":
                        sqlData = "SELECT avg(TotalDeaths) as avgdeaths FROM countries";
                        break;
                    case "Get_countries_deaths_percent":
                        sqlData = "SELECT (count(TotalDeaths) * 100 / count(TotalConfirmed)) as deaths_pourcent FROM global";
                        break;
                    default:
                        sqlData = "Erreur command";
                        break;
                }
            }
        } finally {

            consumer.close();
        }
        return sqlData;
    }
}
