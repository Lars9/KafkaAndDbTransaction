package com.example.kafkaanddbtransaction;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class TestListener {

    static List<String> messages = new CopyOnWriteArrayList<>();


    @KafkaListener(topics = "topic2", containerFactory = "concurrentKafkaListenerContainerFactoryTest")
    public void listen(String message) {
        messages.add(message);
    }
}
