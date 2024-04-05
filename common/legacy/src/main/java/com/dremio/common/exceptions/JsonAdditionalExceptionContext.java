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

import com.dremio.common.serde.ProtobufByteStringSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;

public abstract class JsonAdditionalExceptionContext implements AdditionalExceptionContext {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JsonAdditionalExceptionContext.class);
  private static final ObjectMapper contextMapper = new ObjectMapper();

  // all classes that extend AdditionalExceptionContext must register themselves with the
  // contextMapper.
  static {
    contextMapper.enable(JsonGenerator.Feature.IGNORE_UNKNOWN);
  }

  /**
   * Deserialize the rawAdditionalContext from a UserException into a new
   * AdditionalExceptionContext.
   *
   * @param clazz The Class object representing the AdditionalExceptionContext to serialize.
   * @param ex A UserException containing serialized AdditionalExceptionContext data.
   * @return A new AdditionalExceptionContext of the serialized type.
   */
  protected static <T extends AdditionalExceptionContext> T fromUserException(
      Class<T> clazz, UserException ex) {
    if (ex.getRawAdditionalExceptionContext() == null) {
      logger.debug("missing additional context in UserException");
      return null;
    }

    try {
      return ProtobufByteStringSerDe.readValue(
          contextMapper.readerFor(clazz),
          ex.getRawAdditionalExceptionContext(),
          ProtobufByteStringSerDe.Codec.NONE,
          logger);
    } catch (IOException ignored) {
      // If the AdditionalExceptionContext data can't deserialize due to whatever reason, it's fine
      // since we
      // will retry the operation.
      logger.debug("unable to deserialize additional exception context", ignored);
      return null;
    }
  }

  @Override
  public ByteString toByteString() {
    try {
      return ProtobufByteStringSerDe.writeValue(
          contextMapper, this, ProtobufByteStringSerDe.Codec.NONE);
    } catch (JsonProcessingException ignored) {
      logger.debug("unable to serialize additional exception context", ignored);
      return null;
    }
  }
}
