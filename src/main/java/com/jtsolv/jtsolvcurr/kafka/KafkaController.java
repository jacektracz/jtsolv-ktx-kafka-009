package com.jtsolv.jtsolvcurr.kafka;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducer messageProducer;

    @PostMapping("/kafka/send-post")
    public String sendMessage(@RequestParam("message") String message) {
        messageProducer.sendMessage("jtsolv-test-topic-4", message);
        return "Message sent: " + message;
    }

    @GetMapping("/kafka/send-get")
    public String sendMessageGet(@RequestParam("message") String message) {
        messageProducer.sendMessage("t-1", message);
        return "Message sent: " + message;
    }
    
}