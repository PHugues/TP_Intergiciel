package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class Pr2 {
    public static void producePr2(String brokers, String topicName,String command) throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try
        {
            producer.send(new ProducerRecord<String, String>(topicName, command)).get();
        }
        catch (Exception ex)
        {
            System.out.print(ex.getMessage());
            throw new IOException(ex.toString());
        }

        System.out.println("Commande envoyé au topic 2");

    }
}
