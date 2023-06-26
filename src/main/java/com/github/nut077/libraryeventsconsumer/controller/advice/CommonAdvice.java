package com.github.nut077.libraryeventsconsumer.controller.advice;

import com.github.nut077.libraryeventsconsumer.domain.ExceptionResponse;
import com.github.nut077.libraryeventsconsumer.exception.CommonException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@ControllerAdvice
public class CommonAdvice extends ResponseEntityExceptionHandler {

  private ResponseEntity<Object> handle(Exception ex, HttpStatus status, String code) {
    log.error(ex.getMessage(), ex);
    ExceptionResponse exceptionResponse = new ExceptionResponse(code, ex.getMessage());
    return ResponseEntity.status(status).body(exceptionResponse);
  }

  @Override
  protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
    return handle(ex, HttpStatus.valueOf(statusCode.value()), String.valueOf(statusCode.value()));
  }

  @ExceptionHandler(CommonException.class)
  protected ResponseEntity<Object> handle(CommonException ex) {
    return handle(ex, ex.getStatus(), ex.getCode());
  }

  @ExceptionHandler({IllegalStateException.class, IllegalArgumentException.class})
  protected ResponseEntity<Object> handleIllegal(RuntimeException ex) {
    return handle(ex, HttpStatus.BAD_REQUEST, String.valueOf(HttpStatus.BAD_REQUEST.value()));
  }

  @ExceptionHandler(Exception.class)
  protected ResponseEntity<Object> handle(Exception ex) {
    return handle(ex, HttpStatus.INTERNAL_SERVER_ERROR, String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()));
  }
}
