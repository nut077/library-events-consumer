package com.github.nut077.libraryeventsconsumer.scheduler;

import com.github.nut077.libraryeventsconsumer.config.LibraryEventsConsumerConfig;
import com.github.nut077.libraryeventsconsumer.entity.FailureRecord;
import com.github.nut077.libraryeventsconsumer.repository.FailureRecordRepository;
import com.github.nut077.libraryeventsconsumer.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

  private final FailureRecordRepository failureRecordRepository;
  private final LibraryEventsService libraryEventsService;

  @Scheduled(fixedRate = 10_000)
  public void retryFailedRecords() {
    log.info("Retrying Failed Records Started!");
    failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
      .forEach(failureRecord -> {
        log.info("Retrying Failed Record: {}", failureRecord);
        var consumerRecord = buildConsumerRecord(failureRecord);
        try {
          libraryEventsService.processLibraryEvent(consumerRecord);
          failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
        } catch (Exception e) {
          log.error("Exception in retryFailedRecords: {}", e.getMessage(), e);
        }
      });
    log.info("Retrying Failed Records Completed!");
  }

  private ConsumerRecord<String, String> buildConsumerRecord(FailureRecord failureRecord) {
    return new ConsumerRecord<>(
      failureRecord.getTopic(),
      failureRecord.getPartition(),
      failureRecord.getOffsetValue(),
      failureRecord.getKeyValue(),
      failureRecord.getErrorRecord()
    );
  }
}
