package com.github.nut077.libraryeventsconsumer.repository;

import com.github.nut077.libraryeventsconsumer.entity.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailureRecordRepository extends JpaRepository<FailureRecord, Long> {

  List<FailureRecord> findAllByStatus(String status);
}
