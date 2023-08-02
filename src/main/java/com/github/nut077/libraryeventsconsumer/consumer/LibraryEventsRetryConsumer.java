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
public class LibraryEventsRetryConsumer {

  private final LibraryEventsService libraryEventsService;

  @KafkaListener(topics = {"${topics.retry}"},
    autoStartup = "${retryListener.startup:false}",
    groupId = "retry-listener-group")
  public void onMessage(ConsumerRecord<String, String> consumerRecord) {
    log.info("ConsumerRecord in Retry Consumer: {}", consumerRecord);
    consumerRecord.headers().forEach(header -> log.info("Key: {}, value: {}", header.key(), new String(header.value())));
    libraryEventsService.processLibraryEvent(consumerRecord);
  }
}
