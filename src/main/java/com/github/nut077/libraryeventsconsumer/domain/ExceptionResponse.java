package com.github.nut077.libraryeventsconsumer.domain;

import java.time.OffsetDateTime;

public record ExceptionResponse(String code, String message, OffsetDateTime timestamp) {
  public ExceptionResponse(String code, String message) {
    this(code, message, OffsetDateTime.now());
  }
}
