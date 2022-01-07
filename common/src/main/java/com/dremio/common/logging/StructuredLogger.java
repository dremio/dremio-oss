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
package com.dremio.common.logging;

import java.util.function.Function;

import com.google.protobuf.Message;

/**
 * Logger to log Structured messages.
 */
public interface StructuredLogger<T> {

  /**
   * Log the give <code>data</code> at INFO Level.
   * @param message - A string message.
   * @param data - the protobuf instance to be logged.
   */
  void info(String message, T data);

  /**
   * Log the give <code>data</code> at DEBUG Level.
   * @param message - A string message.
   * @param data - the protobuf instance to be logged.
   */
  void debug(String message, T data);

  /**
   * Get a structured Logger instance for Protobuf Objects.
   * @param clazz - Instance of the Specific Protobuf Class.
   * @param loggerName - logger name.
   * @param <V> The specific type of protobuf.
   * @return an instance of structred Logger.
   */
  static <V extends Message> StructuredLogger<V> get(Class<V> clazz, String loggerName) {
    return ProtobufStructuredLogger.of(loggerName);
  }

  /**
   * Helper method to convert the message to a different type and log that.
   * @param mapper to convert the source type <code>U</code> to structured type <code>T</code> to be logged.
   * @param <U> The source type data being generated in.
   * @return - a wrapped StructuredLogger for the source struct type (which is not a protobuf message).
   */
  default <U> StructuredLogger<U> compose(Function<? super U, ? extends T> mapper) {
    return new ComposedStructuredLogger<>(this, mapper);
  }

  /**
   * An utility class to compose a chain of structured loggers.
   * @param <T> The type of messages this logger will accept
   * @param <R> The type of messages the parent logger will accept.
   */
  final class ComposedStructuredLogger<T, R> implements StructuredLogger<T> {
    private final StructuredLogger<R> parentLogger;
    private final Function<? super T, ? extends R> mapper;

    public ComposedStructuredLogger(StructuredLogger<R> parentLogger, Function<? super T, ? extends R> mapper) {
      this.parentLogger = parentLogger;
      this.mapper = mapper;
    }

    @Override
    public void info(String message, T data) {
      parentLogger.info(message, mapper.apply(data));
    }

    @Override
    public void debug(String message, T data) {
      parentLogger.debug(message, mapper.apply(data));
    }
  }
}
