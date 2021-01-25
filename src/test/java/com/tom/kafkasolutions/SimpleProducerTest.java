package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class SimpleProducerTest {

    KafkaProducer<String, byte[]> producer;
    private static final List<ProducerRecord<String, byte[]>> recordRepository;
    public static final String INPUT_TOPIC = "exactly-once-input-topic";

    static {
        int sampleSize = 10;
        recordRepository = new ArrayList<>(sampleSize);
        for (int i = 1; i < sampleSize; i++) {
            String uuid = UUID.randomUUID().toString();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC,uuid, uuid.getBytes(StandardCharsets.UTF_8));
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

    public static Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer-test-1");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.RETRIES_CONFIG, 7);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put("ssl.truststore.location", "E:\\buildSpace\\kafka-solutions\\kafka-solutions\\src\\main\\resources\\kafka.truststore.jks");
        props.put("ssl.truststore.password", "secret");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;");
        return props;
    }

}
