package com.example.kafkaanddbtransaction;

import com.example.kafkaanddbtransaction.listener.Listener;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.example.kafkaanddbtransaction.TestListener.messages;
import static org.mockito.ArgumentMatchers.any;


@SpringBootTest(properties = "spring-main-allow-bean-definition-overriding=true",
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = {TestConfig.class, KafkaAndDbTransactionApplication.class})
@Log
class KafkaAndDbTransactionApplicationTests {

    private static final String SCRIPT = "test_sql.sh";

    @Autowired
    KafkaTemplate<String, String> kafkaTemplateTest;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @SpyBean
    Listener listener;

    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    static OracleContainer db = new OracleContainer("wnameless/oracle-xe-11g");

    private AnswerWaitor waitor = new AnswerWaitor();

    @BeforeAll
    public static void beforeClass() throws IOException, InterruptedException {
        kafka.start();

        Locale.setDefault(Locale.ENGLISH);
        db.withCopyFileToContainer(MountableFile.forClasspathResource(SCRIPT),
                "/" + SCRIPT);
        db.start();

        Container.ExecResult result = db.execInContainer("bash", "-c",
                "chmod a+x /" + SCRIPT + ";" +
                        "./" + SCRIPT);

        log.info("Result script working: " + result.getStdout());
        log.info("ERROR script working: " + result.getStderr());

    }

    @BeforeEach
    void before() {
        Mockito.doAnswer(waitor).when(listener).listen(any(), any());
    }

    @Test
    @SneakyThrows
    void transactionIsSuccessCommittedTest() {

        kafkaTemplateTest.executeInTransaction(kt -> kt.send("topic1", 0, "key1", "Simple message1"))
                .get(10, TimeUnit.SECONDS);
        waitor.await();

        List<String> listMessages = waitMessagesNotEmpty(messages, 1000L);
        Assertions.assertEquals(1,
                listMessages.size(), "Actual size message list differs expected message list");

        List<Map<String, Object>> actualRecords = jdbcTemplate.queryForList("SELECT * FROM TESTTABLE");
        Assertions.assertEquals(1, actualRecords.size(), "Actual size TESTTABLE differs expected TESTTABLE");

        List<Map<String, Object>> result =
                jdbcTemplate.queryForList("select * from TEST_USER.TESTTABLE");
    }

    private List<String> waitMessagesNotEmpty(List<String> messages, Long milliseconds) {
        return Awaitility
                .waitAtMost(milliseconds, TimeUnit.MILLISECONDS)
                .until(() -> messages, Matchers.not(Matchers.empty()));
    }

    @AfterEach
    @SneakyThrows
    void after() {
        jdbcTemplate.execute("DELETE FROM TESTTABLE");
    }

}
