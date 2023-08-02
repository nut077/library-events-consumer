package com.github.nut077.libraryeventsconsumer.config;

import com.github.nut077.libraryeventsconsumer.exception.BadRequestException;
import com.github.nut077.libraryeventsconsumer.service.FailureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
@EnableKafka
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

  private final KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private FailureService failureService;

  public static final String RETRY = "RETRY";
  public static final String DEAD = "DEAD";
  public static final String SUCCESS = "SUCCESS";

  @Value("${topics.retry}")
  private String retryTopic;

  @Value("${topics.dlt}")
  private String deadLetterTopic;

  public DeadLetterPublishingRecoverer publishingRecoverer() {
    return new DeadLetterPublishingRecoverer(kafkaTemplate,
      ((r, e) -> {
        if (e.getCause() instanceof RecoverableDataAccessException) {
          return new TopicPartition(retryTopic, r.partition());
        } else {
          return new TopicPartition(deadLetterTopic, r.partition());
        }
      }));
  }

  ConsumerRecordRecoverer consumerRecordRecoverer = ((consumerRecord, e) -> {
    log.error("Exception in consumerRecordRecoverer: {}", e.getMessage());
    var records = (ConsumerRecord<String, String>) consumerRecord;
    if (e.getCause() instanceof RecoverableDataAccessException) {
      log.info("Inside Recovery");
      failureService.saveFailedRecord(records, e, RETRY);
    } else {
      log.info("Inside Non-Recovery");
      failureService.saveFailedRecord(records, e, DEAD);
    }
  });

  public DefaultErrorHandler errorHandler() {
    var exceptionToIgnoreList = List.of(BadRequestException.class);
    FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

    ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
    expBackOff.setInitialInterval(1_000L);
    expBackOff.setMultiplier(2d);
    expBackOff.setMaxInterval(2_000L);

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
      //fixedBackOff
      //publishingRecoverer(),
      consumerRecordRecoverer,
      expBackOff
    );

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
