package com.github.gustavoflor.kafkaproducerwikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "wikimedia.recentchange";
    private static final URI WIKIMEDIA_STREAM_URI = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProperties());
        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, TOPIC);
        EventSource eventSource = new EventSource.Builder(eventHandler, WIKIMEDIA_STREAM_URI).build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(5);
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        high throughput configuration
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

//        safe producer configuration (Kafka <= 2.8)
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        return properties;
    }
}
