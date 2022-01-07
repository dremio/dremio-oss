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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

import net.logstash.logback.argument.StructuredArguments;

/**
 * A StructuredLogger implementation that can log protobuf messages converted using the provided converter.
 * @param <T>
 */
class ProtobufStructuredLogger<T extends Message> implements StructuredLogger<T> {
  private final Logger logger;

  ProtobufStructuredLogger(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void info(String message, T data) {
    logger.info(message, StructuredArguments.f(data));
  }

  @Override
  public void debug(String message, T data) {
    logger.debug(message, StructuredArguments.f(data));
  }

  static <M extends Message> ProtobufStructuredLogger<M> of(String loggerName) {
    return new ProtobufStructuredLogger<>(LoggerFactory.getLogger(loggerName));
  }
}
