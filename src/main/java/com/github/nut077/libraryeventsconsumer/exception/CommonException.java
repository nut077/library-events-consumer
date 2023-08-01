package com.github.nut077.libraryeventsconsumer.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class CommonException extends RuntimeException {

  protected final HttpStatus status;
  protected final String code;

  CommonException(HttpStatus status, String code, String message) {
    super(message);
    this.status = status;
    this.code = code;
  }

}
