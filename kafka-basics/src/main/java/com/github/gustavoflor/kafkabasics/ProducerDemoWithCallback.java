package com.github.gustavoflor.kafkabasics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Objects.isNull;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private static final String SUCCESS_MESSAGE = "Received new metadata. topic = {}, partition = {}, offset = {}, timestamp = {}";

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int index = 0; index < 10; index++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("java-basics-callback", "Hello World " + index);
            kafkaProducer.send(producerRecord, callback());

            // to avoid StickyPartitionCache (only to learn, don't use this in production)
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        kafkaProducer.flush();

        kafkaProducer.close();
    }

    private static Callback callback() {
        return (RecordMetadata metadata, Exception exception) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (isNull(exception)) {
                log.info(SUCCESS_MESSAGE, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                log.error("Error while producing", exception);
            }
        };
    }
}
