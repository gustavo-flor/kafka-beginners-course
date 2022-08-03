package com.github.gustavoflor.kafkabasics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "second-consumer-demo";
    private static final String TOPIC = "java-basics";

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer!");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest, none, earliest

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling kafkaConsumer.wakeup()...");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key = {}, value = {}, partition = {}, offset = {}", record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            kafkaConsumer.close();
            log.info("consumer is now gracefully closed");
        }
    }
}
