package com.demo.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer {

    @Value("${spring.kafka.consumer.topic}")
    private String topic;

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    @KafkaListener(topics = "${spring.kafka.consumer.topic}")
    public void consume(String message) throws IOException {
        logger.info(String.format("#### -> Consumed message -> %s", message));
    }
}
