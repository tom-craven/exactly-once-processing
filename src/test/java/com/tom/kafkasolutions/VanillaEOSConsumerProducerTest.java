package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tom.kafkasolutions.KafkaTestProperties.*;

/**
 * No assertions jut a harness for manual test
 */
@Slf4j
@Disabled
public class VanillaEOSConsumerProducerTest {

    private KafkaConsumer<String, byte[]> consumer;
    private KafkaProducer<String, byte[]> producer;

    @BeforeEach
    public void init() {
        consumer = new KafkaConsumer<>(consumerProps(INTERNAL_GROUP_ID));
        producer = new KafkaProducer<>(producerProps());
        producer.initTransactions();
    }

    @Test
    public void whenPerformingTransaction() {
        consumer.subscribe(Collections.singleton(INPUT_TOPIC));
        producer.beginTransaction();
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(30));

        AtomicInteger count = new AtomicInteger();

        records.forEach(record -> {
            log.info("CONSUMED {}: offset = {}, key = {}, value = {}", count.getAndIncrement(), record.offset(), record.key(), Arrays.toString(record.value()));
            producer.send(new ProducerRecord<>(INTERNAL_GROUP_ID, record.key(), record.value()));
        });

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        records.partitions().forEach(topicPartition -> {
            List<ConsumerRecord<String, byte[]>> partitionedRecords = records.records(topicPartition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset + 1));
        });

        producer.sendOffsetsToTransaction(offsetsToCommit, "simple-group");
        producer.commitTransaction();
        consumer.commitSync();
    }

}
