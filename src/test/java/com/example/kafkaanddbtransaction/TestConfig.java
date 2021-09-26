package com.example.kafkaanddbtransaction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Properties;

import static com.example.kafkaanddbtransaction.KafkaAndDbTransactionApplicationTests.kafka;

@TestConfiguration
public class TestConfig {

    @Bean
    public ProducerFactory<String, String> producerFactoryTest() {
        HashMap<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()/*"localhost:9092"*/);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        DefaultKafkaProducerFactory<String, String> producer = new DefaultKafkaProducerFactory<>(props);
        producer.setTransactionIdPrefix("test-id-prefix");
        return producer;
    }

    @Bean
    public Properties props() {
        Properties properties = new Properties();
        properties.setProperty("kafka.servers", kafka.getBootstrapServers());

        return properties;
    }

    @Bean
    public PropertySourcesPlaceholderConfigurer propertie() {
        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();

        Properties properties = new Properties();
        properties.setProperty("kafka.servers", kafka.getBootstrapServers());
        properties.setProperty("bootstrap.servers", kafka.getBootstrapServers());
        properties.setProperty("jdbc.url", KafkaAndDbTransactionApplicationTests.db.getJdbcUrl());

        propertySourcesPlaceholderConfigurer.setProperties(properties);
        return propertySourcesPlaceholderConfigurer;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateTest(ProducerFactory<String, String> producerFactoryTest) {
        return new KafkaTemplate<>(producerFactoryTest);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactoryTest() {

        ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.GROUP_ID_CONFIG, "test",
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
                )
        );
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);

        return concurrentKafkaListenerContainerFactory;
    }
}
