package com.github.nut077.libraryeventsconsumer.consumer;

import com.github.nut077.libraryeventsconsumer.entity.Book;
import com.github.nut077.libraryeventsconsumer.entity.LibraryEvent;
import com.github.nut077.libraryeventsconsumer.entity.LibraryEventType;
import com.github.nut077.libraryeventsconsumer.repository.FailureRecordRepository;
import com.github.nut077.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.github.nut077.libraryeventsconsumer.service.LibraryEventsService;
import com.github.nut077.libraryeventsconsumer.utility.ObjectMapperUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
  "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "retryListener.startup=false"})
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

  @Autowired
  private FailureRecordRepository failureRecordRepository;

  private Consumer<String, String> consumer;

  @Value("${topics.retry}")
  private String retryTopic;

  @Value("${topics.dlt}")
  private String deadLetterTopic;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @BeforeEach
  void setUp() {
    /*for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
    }*/
    MessageListenerContainer container = endpointRegistry.getListenerContainers()
      .stream().filter(messageListenerContainer ->
        Objects.equals(messageListenerContainer.getGroupId(), groupId)
      ).toList().get(0);
    ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
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

    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

    ConsumerRecord<String, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
    System.out.println("consumerRecord is : " + consumerRecord.value());
    assertEquals(req, consumerRecord.value());
  }

  @Test
  void public_update_null_library_event_failure_record() throws ExecutionException, InterruptedException {
    // given
    String req = "{\"id\":null,\"libraryEventType\": \"UPDATE\",\"book\":{\"id\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(req).get();

    // when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // then
    verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    var count = failureRecordRepository.count();
    assertEquals(1, count);

    failureRecordRepository.findAll()
      .forEach(failureRecord -> System.out.println("failureRecord: " + failureRecord));

  }

  @Test
  void public_update_id_999_library_event() throws ExecutionException, InterruptedException {
    // given
    String req = "{\"id\":999,\"libraryEventType\": \"UPDATE\",\"book\":{\"id\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(req).get();

    // when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(5, TimeUnit.SECONDS);

    // then
    verify(libraryEventsConsumer, times(3)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsService, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

    ConsumerRecord<String, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
    System.out.println("consumerRecord is : " + consumerRecord.value());
    assertEquals(req, consumerRecord.value());
  }
}