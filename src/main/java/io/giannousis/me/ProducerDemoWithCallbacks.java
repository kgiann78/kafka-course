package io.giannousis.me;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the Producer
        final KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);
        for (int I=0; I<10; I++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("first_topic", "payload_" + Integer.toString(I));
            // Send the data
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata -> " +
                                "Topic: " + m.topic() +
                                " Partition: " + m.partition() +
                                " Offset: " + m.offset() +
                                " Timestamp: " + m.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}