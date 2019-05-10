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
package com.dremio.common.utils;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Ascii;
import com.google.protobuf.MessageOrBuilder;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

/**
 * Protobuf utilities methods
 */
public final class ProtobufUtils {

  private ProtobufUtils() {
  }

  /*
   * A "protobuf" to camel case property naming strategy which should handle correctly
   * both lower case separated by hyphen protobuf names, along with non standard camel case
   * field names
   */
  @SuppressWarnings("serial")
  private static final PropertyNamingStrategy PROTOBUF_TO_CAMEL_STRATEGY = new PropertyNamingStrategy.PropertyNamingStrategyBase() {
    @Override
    public String translate(String propertyName) {
      if (propertyName == null || propertyName.isEmpty()) {
        return propertyName;
      }

      // Follow protobuf algorithm described at
      // https://developers.google.com/protocol-buffers/docs/reference/java-generated#fields
      final StringBuilder buffer = new StringBuilder(propertyName.length());
      buffer.append(Ascii.toLowerCase(propertyName.charAt(0)));
      boolean toCapitalize = false;
      for (int i = 1; i < propertyName.length(); i++) {
        char c = propertyName.charAt(i);
        if (c == '_') {
          toCapitalize = true;
          continue;
        }

        if (toCapitalize) {
          buffer.append(Ascii.toUpperCase(c));
          toCapitalize = false;
        } else {
          buffer.append(c);
        }
      }
      return buffer.toString();
    }
  };

  /*
   * Mapper which doesn't close streams
   */
  private static final ObjectMapper MAPPER = newMapper()
      .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

  /**
   * Creates a protobuf-ready object mapper
   *
   * Creates a protobuf-ready object mapper which follows same convention as
   * Protostuff format:
   * <ul><li>enums are represented by their index</li>/</ul>
   *
   * @return
   */
  public static final ObjectMapper newMapper() {
    return new ObjectMapper()
        // Reproduce Protostuff configuration
        .enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)
        .enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)
        .disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS)
        .setPropertyNamingStrategy(PROTOBUF_TO_CAMEL_STRATEGY)
        .registerModule(new ProtobufModule());
  }


  /**
   * Writes Protobuf message as JSON
   *
   * Writes the protobuf message as JSON to the provided stream following protostuff style
   *
   * @param os the output stream
   * @param value the protobuf message
   * @throws IOException if an error during serialization happens
   */
  public static final <M extends MessageOrBuilder> void writeAsJSONTo(OutputStream os, M value) throws IOException {
    MAPPER.writeValue(os, value);
  }

  /**
   * Writes Protobuf message as JSON to a byte array
   *
   * @param value the protobuf message
   * @return a byte array representing the JSON string
   * @throws IOException if an error during serialization happens
   */
  public static final <M extends MessageOrBuilder> byte[] toJSONByteArray(M value) throws IOException {
    return MAPPER.writeValueAsBytes(value);
  }


  /**
   * Writes Protobuf message as JSON to a string
   *
   * @param value the protobuf message
   * @return a byte array representing the JSON string
   * @throws IOException if an error during serialization happens
   */
  public static final <M extends MessageOrBuilder> String toJSONString(M value) throws IOException {
    return MAPPER.writeValueAsString(value);
  }

  /**
   * Reads Protobuf message from a JSON string
   *
   * @param value the json conttent
   * @return the protobuf message
   * @throws IOException if an error during serialization happens
   */
  public static final <M extends MessageOrBuilder> M fromJSONString(Class<M> clazz, String json) throws IOException {
    return MAPPER.readValue(json, clazz);
  }
}
