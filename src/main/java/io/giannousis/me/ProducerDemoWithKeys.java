package io.giannousis.me;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        // Create Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "second_topic";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int I=0; I<10; I++) {
            String message = "payload_" + I;
            String key = "id_" + Integer.toString(I);
            // Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,
                    key, message);
            logger.info("Key: " + key);
            // Send Data async
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata -> " +
                                "Topic: " + recordMetadata.topic() +
                                " Partition " + recordMetadata.partition() +
                                " Offset " + recordMetadata.offset() +
                                " Timestamp " + recordMetadata.timestamp()
                        );
                    } else {
//                    e.printStackTrace();
                        logger.error("Error while producing: " + e);
                    }
                }
            }).get();   // We want to show that same keys goes to same partition
                        // block the .send() to make it synchronous - do not do this in production
                        // his adds throws ExecutionException, InterruptedException in class
        }
        producer.flush();
        producer.close();
    }
}
// If it shows only one partition the topic has only one partition
// Same number of partitions lead to same results across machines