package com.github.gustavoflor.kafkaproducerwikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    public WikimediaChangeHandler(final KafkaProducer<String, String> kafkaProducer, final String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // DO NOTHING
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(final String event, final MessageEvent messageEvent) {
        log.info("Message received with data = {}", messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(final String comment) {
        // DO NOTHING
    }

    @Override
    public void onError(final Throwable throwable) {
        log.error("Error in Stream Reading", throwable);
    }
}
