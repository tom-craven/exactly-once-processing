package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(
        topics = "${spring.kafka.template.consumer-topic}",
        containerFactory = "kafkaListenerContainerFactory")
public class EOSKafkaConsumer {

    private final TransactionHandlingService transactionHandlingService;

    @Autowired
    public EOSKafkaConsumer(TransactionHandlingService transactionHandlingService) {
        this.transactionHandlingService = transactionHandlingService;
    }

    @KafkaHandler
    public void consumeEventMessage(
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionId,
            @Header(KafkaHeaders.OFFSET) java.util.List<Long> offsets,
            @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
            @Payload byte[] payload
    ) {
        log.info("received message\nKey: {}\nPayload: {}\nOffset: {}\nPartition-id: {}\nProcessing...", key, payload, offsets, partitionId);
        transactionHandlingService.process(key, payload);
    }
}
