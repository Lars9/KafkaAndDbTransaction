package com.example.kafkaanddbtransaction.config;


import oracle.jdbc.pool.OracleDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class DbAndKafkaConfig {

    @Value("${kafka.servers}")
    String kafkaServers;

    @Value("${jdbc.url}")
    String jdbcUrl;

    @Bean
    public DataSource dataSource() throws SQLException {

        OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setURL(jdbcUrl);
        oracleDataSource.setUser("test_user");
        oracleDataSource.setPassword("1234");

        return oracleDataSource;
    }

    @Bean
    public DataSourceTransactionManager transactionManager() throws SQLException {
        return new DataSourceTransactionManager(dataSource());
    }

    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager(ProducerFactory<String, String> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean
    public ChainedKafkaTransactionManager<String, String> chainedTm(KafkaTransactionManager<String, String> kafkaTransactionManager,
                                                                    PlatformTransactionManager transactionManager) throws SQLException {
        return new ChainedKafkaTransactionManager<>(kafkaTransactionManager, transactionManager);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        DefaultKafkaProducerFactory<String, String> producer = new DefaultKafkaProducerFactory<>(ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        ));
        producer.setTransactionIdPrefix("tx-");
        return producer;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateReal() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<String, String>(
                new ImmutableMap.Builder<String, Object>()
                        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
                        .put(ConsumerConfig.GROUP_ID_CONFIG, "foo")
                        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true)
                        .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                        .put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                        .build(),
                new StringDeserializer(), new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory(KafkaTemplate kafkaTemplateReal,
                                                                                                           ChainedKafkaTransactionManager<String, String> chainedKafkaTransactionManager) throws SQLException {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setMissingTopicsFatal(false);
        factory.getContainerProperties().setTransactionManager(chainedKafkaTransactionManager);
//        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setPollTimeout(30000L);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplateReal),
                new FixedBackOff(0L, 1L)));
        factory.setAfterRollbackProcessor(new DefaultAfterRollbackProcessor<>(new FixedBackOff(0, 0L)));

//        factory.setErrorHandler((m, e) -> System.out.println("ERROR IN TRANSACTION BABY!!!"));

        return factory;

    }

//    @Bean
//    public NewTopic topic1() {
//        return TopicBuilder.name("topic1").build();
//    }
//
//    @Bean
//    public NewTopic topic2() {
//        return TopicBuilder.name("topic2").build();
//    }

}
