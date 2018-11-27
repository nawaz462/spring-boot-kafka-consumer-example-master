package com.kafkaExample.kafkaconsumerexample.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.kafkaExample.kafkaconsumerexample.model.User;


@RestController
public class MessageController {
	
	@Value("${kafka.source.topic.name}")
	private String DESTINATION_TOPIC;
	
    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    @GetMapping("/publish/{message}")
    public String publishMessage(@PathVariable("message") final String name) {

    	kafkaTemplate.send(DESTINATION_TOPIC, new User(name,"Sample"));
//    	kafkaTemplate.send("Kafka_example", name);
        return "Published successfully";
    }
    
    @PostMapping("/publishJson")
    public String publishJsonMessage(@RequestBody User user) {
    	
    	System.out.println("Publishing JSON Message...");
    	kafkaTemplate.send(DESTINATION_TOPIC, user);
		return "Successfully pulbished message to "+ DESTINATION_TOPIC;
    	
    }
}
