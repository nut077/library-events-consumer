package com.github.nut077.libraryeventsconsumer.consumer;

import com.github.nut077.libraryeventsconsumer.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumer {

  private final LibraryEventsService libraryEventsService;

  @KafkaListener(topics = {"${spring.kafka.topic}"})
  public void onMessage(ConsumerRecord<String, String> consumerRecord) {
    log.info("ConsumerRecord: {} ", consumerRecord);
    libraryEventsService.processLibraryEvent(consumerRecord);
  }
}
