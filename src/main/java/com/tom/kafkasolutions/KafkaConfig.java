package com.tom.kafkasolutions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.MessagingMessageConverter;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    private final ConsumerFactory<String, byte[]> consumerFactory;

    @Autowired
    public KafkaConfig(ConsumerFactory<String, byte[]> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name("input-topic")
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name("output-topic")
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(@Autowired MessagingMessageConverter messagingMessageConverter) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setErrorHandler(new SeekToCurrentErrorHandler());
        factory.setMessageConverter(messagingMessageConverter);
        return factory;
    }

    @Bean
    @Primary
    public MessagingMessageConverter simpleMapperConverter() {
        return new MessagingMessageConverter();
    }
}






