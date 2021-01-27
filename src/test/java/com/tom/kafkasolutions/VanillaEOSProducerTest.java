package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.tom.kafkasolutions.KafkaTestProperties.INPUT_TOPIC;
import static com.tom.kafkasolutions.KafkaTestProperties.producerProps;

/**
 * No assertions jut a harness for manual test
 */
@Slf4j
@Disabled
public class VanillaEOSProducerTest {

    private KafkaProducer<String, byte[]> producer;
    private static final List<ProducerRecord<String, byte[]>> recordRepository;

    static {
        int sampleSize = 10;
        recordRepository = new ArrayList<>(sampleSize);
        for (int i = 1; i < sampleSize; i++) {
            String uuid = UUID.randomUUID().toString();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC, uuid, uuid.getBytes(StandardCharsets.UTF_8));
            recordRepository.add(record);
        }
        log.info("created records");
    }

    @BeforeEach
    public void init() {
        producer = new KafkaProducer<>(producerProps());
        producer.initTransactions();
    }

    @Test
    public void itShouldSendMessages() {
        producer.beginTransaction();
        // load record from datasource, table rows perhaps
        recordRepository.forEach(record -> producer.send(record));
        producer.commitTransaction();
        //producer.flush();
        producer.close();
    }

}
