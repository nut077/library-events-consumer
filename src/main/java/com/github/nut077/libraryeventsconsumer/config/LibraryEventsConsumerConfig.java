package com.github.nut077.libraryeventsconsumer.config;

import com.github.nut077.libraryeventsconsumer.exception.BadRequestException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
@EnableKafka
public class LibraryEventsConsumerConfig {

  public DefaultErrorHandler errorHandler() {
    var exceptionToIgnoreList = List.of(BadRequestException.class);
    FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
    exceptionToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
    errorHandler.setRetryListeners(((consumerRecord, e, i) -> log.error("Failed Records in Retry Listener, Exception : {}, deliveryAttempt : {}", e.getMessage(), i)));
    return errorHandler;
  }

  // for manual
  @Bean
  ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
    ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
    ConsumerFactory<Object, Object> kafkaConsumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory);
    factory.setConcurrency(3); // recommended if you are not running your application in a cloud like environment
    //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.setCommonErrorHandler(errorHandler());
    return factory;
  }
}
