/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.common.exceptions;

/**
 * Helper class for creation of exceptions relating to field data size limits.
 */
public final class FieldSizeLimitExceptionHelper {
  private FieldSizeLimitExceptionHelper() {}

  public static void checkSizeLimit(int size, int maxSize, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createFieldSizeLimitException(size, maxSize, logger);
    }
  }

  public static void checkSizeLimit(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createFieldSizeLimitException(size, maxSize, fieldIndex, logger);
    }
  }

  public static void checkSizeLimit(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createFieldSizeLimitException(size, maxSize, fieldName, logger);
    }
  }

  public static UserException createListChildrenLimitException(String fieldName, long maxSize, long size) {
    return UserException
      .unsupportedError(new FieldSizeLimitException())
      .message("List field '%s' exceeded the maximum number of elements %d, actual number of elements is %d", fieldName, maxSize, size)
      .addContext("limit", maxSize)
      .build();
  }

  public static UserException createFieldSizeLimitException(int size, int maxSize, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError(new FieldSizeLimitException())
      .message("Field exceeds the size limit of %d bytes, actual size is %d bytes.", maxSize, size)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createFieldSizeLimitException(int size, int maxSize) {
    return UserException
      .unsupportedError(new FieldSizeLimitException())
      .message("Field exceeds the size limit of %d bytes , actual size is %d bytes.", maxSize, size)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build();
  }

  public static UserException createFieldSizeLimitException(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError(new FieldSizeLimitException())
      .message("Field with index %d exceeds the size limit of %d bytes, actual size is %d bytes.", fieldIndex, maxSize, size)
      .addContext("fieldIndex", fieldIndex)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createFieldSizeLimitException(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError(new FieldSizeLimitException())
      .message("Field '%s' exceeds the size limit of %d bytes, actual size is %d bytes.", fieldName, maxSize, size)
      .addContext("fieldName", fieldName)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static class FieldSizeLimitException extends Exception {
    FieldSizeLimitException() {
      super();
    }
  }
}
