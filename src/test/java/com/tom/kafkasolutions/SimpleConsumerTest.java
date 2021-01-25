package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.tom.kafkasolutions.SimpleProducerTest.INPUT_TOPIC;
import static com.tom.kafkasolutions.SimpleProducerTest.producerProps;

@Slf4j
public class SimpleConsumerTest {
    private static final String OUTPUT_TOPIC = "exactly-once-output";
    KafkaConsumer<String, byte[]> consumer;
    KafkaProducer<String, byte[]> producer;

    @BeforeEach
    public void init() {
        consumer = new KafkaConsumer<>(consumerProps());
        producer = new KafkaProducer<>(producerProps());
        producer.initTransactions();

    }

    @Test
    public void whenPerformingTransaction() {
        consumer.subscribe(Collections.singleton(INPUT_TOPIC));
        producer.beginTransaction();
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));

        records.forEach(record -> {
            log.info("CONSUMED RECORDS: offset = {}, key = {}, value = {}", record.offset(), record.key(), Arrays.toString(record.value()));
            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, record.key(), record.value()));
        });

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, byte[]>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }

        producer.sendOffsetsToTransaction(offsetsToCommit, "simple-group");
        producer.commitTransaction();
    }

    private static Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put("ssl.truststore.location", "E:\\buildSpace\\kafka-solutions\\kafka-solutions\\src\\main\\resources\\kafka.truststore.jks");
        props.put("ssl.truststore.password", "secret");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;");
        return props;
    }
}
