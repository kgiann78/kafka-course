package io.giannousis.me;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // Create Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", bootstrapServers);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // Create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,
                "payload");
        // Send Data - async (fire and forget)
        producer.send(producerRecord);
        // wait to finish
        producer.flush();
        producer.close();
    }
}