package com.example.kafkaanddbtransaction.listener;

import lombok.AllArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@AllArgsConstructor
public class Listener {

    private static int counter = 27;

    private JdbcTemplate jdbcTemplate;

    private KafkaTemplate<String, String> kafkaTemplateReal;


    @KafkaListener(/*id = "foo",*/ topics = "topic1", containerFactory = "concurrentKafkaListenerContainerFactory"/*, errorHandler = "kafkaListenerErrorHandler"*/)
    @Transactional(transactionManager = "chainedTm")
    public void listen(String message, @Header(KafkaHeaders.OFFSET) Long offset) {

        System.out.println("YEA " + message + " from offset: " + offset);
        // jdbcTemplate.execute("INSERT INTO TEST VALUES(" + counter + ", 'Stepanikash')");
        jdbcTemplate.execute("INSERT INTO TESTTABLE VALUES(" + counter + ", 'Stepanikash')");
        counter++;

        kafkaTemplateReal.send("topic2", "key2", "transaction message baby8");

//       countDownLatch.countDown();

//       throw new RuntimeException("RUNTIME EXCEPTION MOTHER FUCKER!!!");

    }

/*
    @KafkaListener(id = "dlt", topics = "topic1.DLT")
    public void listenDeadLetterTopic(String message) {

        System.out.println("Message from dead letter topic: " + message);

    }
*/
}
