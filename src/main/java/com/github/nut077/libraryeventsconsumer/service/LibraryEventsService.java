package com.github.nut077.libraryeventsconsumer.service;

import com.github.nut077.libraryeventsconsumer.entity.LibraryEvent;
import com.github.nut077.libraryeventsconsumer.exception.BadRequestException;
import com.github.nut077.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.github.nut077.libraryeventsconsumer.utility.ObjectMapperUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

  private final LibraryEventsRepository libraryEventsRepository;

  public void processLibraryEvent(ConsumerRecord<String, String> consumerRecord) {
    LibraryEvent libraryEvent = ObjectMapperUtil.convertJsonStringToObject(consumerRecord.value(), LibraryEvent.class);
    log.info("libraryEvent: {}", ObjectMapperUtil.convertObjectToJsonString(libraryEvent));

    switch (libraryEvent.getLibraryEventType()) {
      case NEW -> save(libraryEvent);
      case UPDATE -> {
        validate(libraryEvent);
        save(libraryEvent);
      }
      default -> log.info("Invalid library event type");
    }
  }

  private void validate(LibraryEvent libraryEvent) {
    if (libraryEvent.getId() == null) {
      throw new BadRequestException("Library event id is missing");
    }
    Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getId());
    if (libraryEventOptional.isEmpty()) {
      throw new BadRequestException("Not a valid library event");
    }
    log.info("Validation is successful");
  }

  private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    LibraryEvent libraryEventCreated = libraryEventsRepository.save(libraryEvent);
    log.info("Successfully persisted the library event: {}", ObjectMapperUtil.convertObjectToJsonString(libraryEventCreated));
  }
}
