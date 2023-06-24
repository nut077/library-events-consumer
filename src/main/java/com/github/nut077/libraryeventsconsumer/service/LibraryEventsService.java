package com.github.nut077.libraryeventsconsumer.service;

import com.github.nut077.libraryeventsconsumer.entity.LibraryEvent;
import com.github.nut077.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.github.nut077.libraryeventsconsumer.utility.ObjectMapperUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

  private final LibraryEventsRepository libraryEventsRepository;

  public void processLibraryEvent(ConsumerRecord<String, String> consumerRecord) {
    LibraryEvent libraryEvent = ObjectMapperUtil.convertJsonStringToObject(consumerRecord.value(), LibraryEvent.class);
    log.info("libraryEvent: {}", ObjectMapperUtil.convertObjectToJsonString(libraryEvent));

    switch (libraryEvent.getLibraryEventType()) {
      case NEW -> {
        save(libraryEvent);
      }
      case UPDATE -> {

      }
      default -> log.info("Invalid library event type");
    }
  }

  private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    LibraryEvent libraryEventCreated = libraryEventsRepository.save(libraryEvent);
    log.info("Successfully persisted the library event: {}", ObjectMapperUtil.convertObjectToJsonString(libraryEventCreated));
  }
}
