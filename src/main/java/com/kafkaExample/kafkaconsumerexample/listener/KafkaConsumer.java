package com.kafkaExample.kafkaconsumerexample.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafkaExample.kafkaconsumerexample.model.User;

@Component
public class KafkaConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
	
	@Value("${kafka.destination.topic.name}")
	private String destinationTopic;
	
	@Autowired
	private KafkaTemplate<String, User> kafkaSender;
	
    @KafkaListener(topics = "Kafka_Example", group = "group_id")
    public void consume(String message) {
        System.out.println("Consumed  message: " + message);
        log.info("Consumed  message: " + message);
    }

    /*
     * 
     * Method which consumes the JSON message from kafka topic and
     * sends it to other topic after processing
     */
    @KafkaListener(topics = "${kafka.source.topic.name}", group = "group_json",
            containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user) {
    	
    	 log.info("Consumed JSON Message: " + user);
    	 System.out.println("Consumed JSON  message: " + user);
        //processing - updating name from the consumed message
        user.setName(user.getName()+"_Updated");
        
        try {
        	 kafkaSender.send(destinationTopic, user);
             log.info("Message published successfully ");
        }
        catch(Exception e) {
        	e.printStackTrace();
        	log.error("Message publishing failed");
        }
    }
}
