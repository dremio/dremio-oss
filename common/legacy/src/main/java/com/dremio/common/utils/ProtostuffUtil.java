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
package com.dremio.common.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.common.exceptions.UserException;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.protostuff.GraphIOUtil;
import io.protostuff.JsonIOUtils;
import io.protostuff.Message;
import io.protostuff.Schema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Utility methods for protostuff */
public final class ProtostuffUtil {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ProtostuffUtil.class);

  private ProtostuffUtil() {}

  /**
   * Clone a Protostuff object
   *
   * @param t the protobuf message to copy
   * @return a deep copy of {@code t}
   */
  public static <T extends Message<T>> T copy(T t) {
    try {
      Schema<T> schema = t.cachedSchema();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GraphIOUtil.writeDelimitedTo(new DataOutputStream(out), t, schema);
      // TODO: avoid array copy
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      T newMessage = schema.newMessage();
      GraphIOUtil.mergeDelimitedFrom(in, newMessage, schema);
      return newMessage;
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message(
              "Failure decoding object, please ensure that you ran dremio-admin upgrade on Dremio.")
          .build(logger);
    }
  }

  /**
   * @param to immutable
   * @param from immutable
   * @param <T>
   * @return result of merge from into to
   */
  public static <T extends Message<T>> T merge(T to, T from) throws IOException {
    Schema<T> schema = from.cachedSchema();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GraphIOUtil.writeDelimitedTo(new DataOutputStream(out), from, schema);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    T cloneTo = copy(to);
    GraphIOUtil.mergeDelimitedFrom(in, cloneTo, schema);
    return cloneTo;
  }

  /**
   * Convert a JSON stream into a Java object
   *
   * @param data the JSON data
   * @param the object to update
   * @param schema the Protostuff schema for the object
   * @param numeric if true, use field id as key
   */
  public static <T> void fromJSON(byte[] data, T message, Schema<T> schema, boolean numeric)
      throws IOException {
    // Configure a parser to intepret non-numeric numbers like NaN correctly
    // although non-standard JSON.
    try (JsonParser parser =
        JsonIOUtils.newJsonParser(null, data, 0, data.length)
            .enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)) {
      JsonIOUtils.mergeFrom(parser, message, schema, numeric);
    }
  }

  /**
   * Convert a JSON string into a Java object
   *
   * @param data the JSON data
   * @param the object to update
   * @param schema the Protostuff schema for the object
   * @param numeric if true, use field id as key
   */
  public static <T> T fromJSON(String data, Schema<T> schema, boolean numeric) throws IOException {
    T message = schema.newMessage();
    fromJSON(data.getBytes(UTF_8), message, schema, numeric);

    return message;
  }

  /**
   * Convert a message into a JSON stream
   *
   * @param out the output stream
   * @param the message to write
   * @param schema the protostuff schema for the message
   * @param numeric if true, use field id as keys
   */
  public static <T> void toJSON(OutputStream out, T message, Schema<T> schema, boolean numeric)
      throws IOException {
    try (JsonGenerator jsonGenerator =
        JsonIOUtils.DEFAULT_JSON_FACTORY
            .createGenerator(out, JsonEncoding.UTF8)
            .disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS)) {
      JsonIOUtils.writeTo(jsonGenerator, message, schema, numeric);
    }
  }

  /**
   * Convert a message into a JSON string
   *
   * @param the message to write
   * @param schema the protostuff schema for the message
   * @param numeric if true, use field id as keys
   * @return a JSON string
   */
  public static <T> String toJSON(T message, Schema<T> schema, boolean numeric) {
    try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
      toJSON(baos, message, schema, numeric);
      return new String(baos.toByteArray(), UTF_8);
    } catch (IOException e) {
      throw new AssertionError("IOException not expected with ByteArrayOutputStream", e);
    }
  }
}
