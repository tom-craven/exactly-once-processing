package com.tom.kafkasolutions;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tom.kafkasolutions.KafkaTestProperties.*;

@Slf4j
@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EmbeddedKafka
public class SolutionIT {

    private static final ArrayList<ProducerRecord<String, byte[]>> recordRepository;
    private final ArrayList<RecordMetadata> results = new ArrayList<>();
    private KafkaProducer<String, byte[]> kafkaTemplate;

    @Autowired
    private EOSKafkaProducer eosKafkaProducer;
    @Autowired
    private EOSKafkaConsumer eosKafkaConsumer;
    @Autowired
    private TransactionHandlingService transactionHandlingService;

    static {
        recordRepository = new ArrayList<>(SAMPLE_SIZE);
        for (int i = 1; i <= SAMPLE_SIZE; i++) {
            String uuid = UUID.randomUUID().toString();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC, uuid.toUpperCase(), uuid.getBytes(StandardCharsets.UTF_8));
            recordRepository.add(record);
        }
        log.info("created records");
    }

    @SneakyThrows
    @Test
    @Order(1)
    public void whenMessagesSentToInputTopicThenResultShouldHaveMetadata() {
        kafkaTemplate = new KafkaProducer<>(producerProps());

        kafkaTemplate.initTransactions();
        kafkaTemplate.beginTransaction();

        // load record from datasource, table rows perhaps
        recordRepository.forEach(record ->
                {
                    RecordMetadata result = null;
                    try {
                        result = kafkaTemplate.send(record)
                                .get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("send message failed for record {}", record);
                    }
                    results.add(result);
                }
        );

        kafkaTemplate.commitTransaction();
        //producer.flush();
        kafkaTemplate.close();
        TimeUnit.SECONDS.sleep(3);
        for (RecordMetadata result : results) {
            Assert.notNull(result, "no result for record metadata");
            Assert.isTrue(result.hasOffset(), "no offset assigned for sent message");
            Assert.isTrue(results.size() == SAMPLE_SIZE, "the count of send result does not match the sample size");
        }
    }


    @Test
    @Order(2)
    public void whenConsumingFromOutputThenCountShouldBeEqual() {
        KafkaConsumer<String, byte[]> outputConsumer = new KafkaConsumer<>(consumerProps(OUTPUT_GROUP_ID));

        outputConsumer.subscribe(Collections.singleton(OUTPUT_TOPIC));

        ConsumerRecords<String, byte[]> records = outputConsumer.poll(Duration.ofSeconds(30));

        AtomicInteger count = new AtomicInteger();

        records.records(OUTPUT_TOPIC).forEach(record -> log.info("CONSUMED {}: offset = {}, key = {}, value = {}", count.incrementAndGet(), record.offset(), record.key(), Arrays.toString(record.value())));

        outputConsumer.commitSync();

        log.info("TOTAL RECEIVED: {}", count);
        Assert.isTrue(count.get() == SAMPLE_SIZE, "The count of consumed messages does not meet the sample size");
    }

}
