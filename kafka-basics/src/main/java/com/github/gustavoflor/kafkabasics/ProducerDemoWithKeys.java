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

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    private static final String SUCCESS_MESSAGE = "Received new metadata. topic = {}, key = {}, partition = {}, offset = {}, timestamp = {}";

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int turns = 0; turns < 3; turns++) {
            for (int index = 0; index < 3; index++) {
                String topic = "java-basics";
                String value = "Hello World " + index;
                String key = "id_" + index;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                kafkaProducer.send(producerRecord, callback(producerRecord));
            }
        }

        kafkaProducer.flush();

        kafkaProducer.close();
    }

    private static Callback callback(ProducerRecord<String, String> producerRecord) {
        return (RecordMetadata metadata, Exception exception) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (isNull(exception)) {
                log.info(SUCCESS_MESSAGE, metadata.topic(), producerRecord.key(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                log.error("Error while producing", exception);
            }
        };
    }
}
