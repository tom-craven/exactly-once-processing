package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class TransactionHandlingService {

    private final EOSKafkaProducer eosKafkaProducer;

    @Autowired
    public TransactionHandlingService(EOSKafkaProducer eosKafkaProducer) {
        this.eosKafkaProducer = eosKafkaProducer;
    }

    @Transactional
    public void process(String key, byte[] payload) {
        try {
            eosKafkaProducer.sendMessage(key, payload);
        } catch (ExecutionException | InterruptedException e) {
            throw new ListenerExecutionFailedException(e.getMessage());
        }
    }
}
