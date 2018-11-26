package com.kafkaExample.kafkaconsumerexample.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafkaExample.kafkaconsumerexample.model.User;

@Service
public class KafkaConsumer {
	
	Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "Kafka_Example", group = "group_id")
    public void consume(String message) {
        System.out.println("Consumed  message: " + message);
        log.info("Consumed  message: " + message);
    }


    @KafkaListener(topics = "Kafka_Json_Example", group = "group_json",
            containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user) {
        System.out.println("Consumed JSON Message: " + user);
        //processing
        //publish it to other topic
    }
}
