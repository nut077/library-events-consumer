package com.github.nut077.libraryeventsconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<String, String> {

  @Override
  @KafkaListener(topics = {"${spring.kafka.topic}"})
  public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
    log.info("ConsumerRecord: {} ", consumerRecord);
    acknowledgment.acknowledge();
  }
}
