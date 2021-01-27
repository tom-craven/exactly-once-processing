package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class EOSKafkaProducer {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private String MESSAGE_KEY_HEADER = "MessageKey";;

    @Autowired
    public EOSKafkaProducer(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Send a plain message
     *
     *
     * @param key
     * @param payload bytes to send
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public SendResult<String, byte[]> sendMessage(String key, byte[] payload) throws ExecutionException, InterruptedException {

        return getFuture(MessageBuilder.
                withPayload(payload).setHeader(MESSAGE_KEY_HEADER, key), kafkaTemplate).get();
    }

    static CompletableFuture<SendResult<String, byte[]>> getFuture(MessageBuilder<byte[]> message, KafkaTemplate<String, byte[]> kafkaTemplate) {

        ListenableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(message.build());

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, byte[]> result) {
                log.info("Kafka Producer sent message with result, {}", result.toString());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Kafka Producer Failed to send Message because exception {}", ex.getMessage());
            }
        });
        return future.completable();
    }

}
