package com.github.nut077.libraryeventsconsumer.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class ObjectMapperUtil {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @SneakyThrows
  public static String convertObjectToJsonString(Object obj) {
    return objectMapper.writeValueAsString(obj);
  }

  @SneakyThrows
  public static <T> T convertJsonStringToObject(String json, Class<T> tClass) {
    return new ObjectMapper().readValue(json, tClass);
  }
}
