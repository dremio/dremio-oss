/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

  public static void checkReadSizeLimit(int size, int maxSize, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createReadFieldSizeLimitException(size, maxSize, logger);
    }
  }

  public static void checkReadSizeLimit(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createReadFieldSizeLimitException(size, maxSize, fieldIndex, logger);
    }
  }

  public static void checkReadSizeLimit(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createReadFieldSizeLimitException(size, maxSize, fieldName, logger);
    }
  }

  public static void checkWriteSizeLimit(int size, int maxSize, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createWriteFieldSizeLimitException(size, maxSize, logger);
    }
  }

  public static void checkWriteSizeLimit(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createWriteFieldSizeLimitException(size, maxSize, fieldIndex, logger);
    }
  }

  public static void checkWriteSizeLimit(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    if (size > maxSize) {
      throw createWriteFieldSizeLimitException(size, maxSize, fieldName, logger);
    }
  }

  public static UserException createReadFieldSizeLimitException(int size, int maxSize, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to read a too large value for a field. Size was %d but limit was %d.", size, maxSize)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createReadFieldSizeLimitException(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to read a too large value for field with index %d. Size was %d but limit was %d.", fieldIndex, size, maxSize)
      .addContext("fieldIndex", fieldIndex)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createReadFieldSizeLimitException(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to read a too large value for field with name %s. Size was %d but limit was %d.", fieldName, size, maxSize)
      .addContext("fieldName", fieldName)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createWriteFieldSizeLimitException(int size, int maxSize, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to write a too large value for a field. Size was %d but limit was %d.", size, maxSize)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createWriteFieldSizeLimitException(int size, int maxSize, int fieldIndex, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to write a too large value for field with index %d. Size was %d but limit was %d.", fieldIndex, size, maxSize)
      .addContext("fieldIndex", fieldIndex)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }

  public static UserException createWriteFieldSizeLimitException(int size, int maxSize, String fieldName, org.slf4j.Logger logger) {
    return UserException
      .unsupportedError()
      .message("Attempting to write a too large value for field with name %s. Size was %d but limit was %d.", fieldName, size, maxSize)
      .addContext("fieldName", fieldName)
      .addContext("size", size)
      .addContext("limit", maxSize)
      .build(logger);
  }
}
