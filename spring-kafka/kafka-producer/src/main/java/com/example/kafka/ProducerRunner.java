package com.example.kafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ProducerRunner implements CommandLineRunner {
    
    private final KafkaProducerService producer;
    
    public ProducerRunner(KafkaProducerService producer) {
        this.producer = producer;
    }
    
    @Override
    public void run(String... args) throws Exception {
        int i = 0;
        while (true) {
            String message = "Message " + i + " at " + System.currentTimeMillis();
            producer.sendMessage(message);
            i++;
            Thread.sleep(10);
        }
    }
}
