package com.github.nut077.libraryeventsconsumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FailureRecord {

  @Id
  @GeneratedValue
  private Long id;

  private String topic;
  private String keyValue;
  private String errorRecord;
  private Integer partition;
  private Long offsetValue;
  private String exception;
  private String status;
}
