package io.giannousis.me;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        // Create Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        String group_id = "my_last_app";
        String topic = "second_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest, none
        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe Consumer to our topics
//        consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));
        try {
            // Poll for new Data
            while (true) {
                //            consumer.poll(100);
                // ALT+ENTER set language level to 8
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> rec : records) {
                    logger.info("Key: " + rec.key() + ", Value: " + rec.value() +
                            ", Partition: " + rec.partition() + ", Offset: " + rec.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
