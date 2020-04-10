package io.giannousis.me;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    // constructor
    private ConsumerDemoWithThread() {
    }
    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        // Create Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        String group_id = "my_app";
        String topic = "second_topic";
        logger.info("Creating the consumer thread");
        // Latch to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // create the consumer Runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                group_id,
                topic,
                latch);
        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();
        } catch(InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
class ConsumerRunnable implements Runnable {
    private Logger logger =
            LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    private KafkaConsumer<String, String> consumer;
    private CountDownLatch latch;
    public ConsumerRunnable(String bootstrapServers,
                          String group_id,
                          String topic,
                          CountDownLatch latch) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest, none
        // Create the Consumer
        this.consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe Consumer to our topics
        consumer.subscribe(Arrays.asList(topic));
        this.latch = latch;
    }
    @Override
    public void run() {
        try {
            // Poll for new Data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> rec : records) {
                    logger.info("Key: " + rec.key() + ", Value: " + rec.value() +
                            ", Partition: " + rec.partition() + ", Offset: " + rec.offset());
                }
            }
        }
        catch (WakeupException e) {
            // We are going to expect this exception
            // It is the way out
            logger.info("Received shutdown signal!");
        }
        finally {
            consumer.close();
            // Allow main code to understand that we are able to exit
            latch.countDown();
        }
    }
    public void shutdown() {
        // this is a special method to interrupt consumer.poll()
        // it will throw the exception WakeUpException
        consumer.wakeup();
    }
}