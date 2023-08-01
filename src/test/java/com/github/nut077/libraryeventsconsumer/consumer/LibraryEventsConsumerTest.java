package com.github.nut077.libraryeventsconsumer.consumer;

import com.github.nut077.libraryeventsconsumer.entity.Book;
import com.github.nut077.libraryeventsconsumer.entity.LibraryEvent;
import com.github.nut077.libraryeventsconsumer.entity.LibraryEventType;
import com.github.nut077.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.github.nut077.libraryeventsconsumer.service.LibraryEventsService;
import com.github.nut077.libraryeventsconsumer.utility.ObjectMapperUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
  "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventsConsumerTest {

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private KafkaListenerEndpointRegistry endpointRegistry;

  @SpyBean
  private LibraryEventsConsumer libraryEventsConsumer;

  @SpyBean
  private LibraryEventsService libraryEventsService;

  @Autowired
  private LibraryEventsRepository libraryEventsRepository;

  @BeforeEach
  void setUp() {
    for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
    }
  }

  @AfterEach
  void tearDown() {
    libraryEventsRepository.deleteAll();
  }

  @Test
  void public_new_library_event() throws ExecutionException, InterruptedException {
    // given
    String req = "{\"id\":null,\"libraryEventType\": \"NEW\",\"book\":{\"id\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(req).get();

    // when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    // then
    verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    List<LibraryEvent> libraryEventList = libraryEventsRepository.findAll();
    assert libraryEventList.size() == 1;
    libraryEventList.forEach(libraryEvent -> {
      assert libraryEvent.getId() != null;
      assertEquals(456, libraryEvent.getBook().getId());
    });
  }

  @Test
  void public_update_library_event() throws ExecutionException, InterruptedException {
    // given
    String req = "{\"id\":456,\"libraryEventType\": \"UPDATE\",\"book\":{\"id\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    LibraryEvent libraryEvent = ObjectMapperUtil.convertJsonStringToObject(req, LibraryEvent.class);
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEventsRepository.save(libraryEvent);

    Book updatedBook = new Book();
    updatedBook.setId(456L);
    updatedBook.setBookName("Kafka Using Spring Boot 3.x.x");
    updatedBook.setBookAuthor("Freedom");
    libraryEvent.setBook(updatedBook);
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

    String updatedJson = ObjectMapperUtil.convertObjectToJsonString(libraryEvent);
    kafkaTemplate.sendDefault(libraryEvent.getId().toString(), updatedJson).get();

    // when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);

    // then
    verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getId()).get();
    assertEquals("Kafka Using Spring Boot 3.x.x", persistedLibraryEvent.getBook().getBookName());
  }

  @Test
  void public_update_null_library_event() throws ExecutionException, InterruptedException {
    // given
    String req = "{\"id\":null,\"libraryEventType\": \"UPDATE\",\"book\":{\"id\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(req).get();

    // when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // then
    verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
  }
}