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
package com.dremio.exec.physical.config;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import io.protostuff.ByteString;

/**
 * A class representing extended properties that can be stored as key-value pairs and serialized to/from JSON format.
 */
public class ExtendedProperties {

  /**
   * An enumeration of property keys used to identify different types of extended properties.
   */
  public enum PropertyKey {
    COPY_INTO_QUERY_PROPERTIES,
    QUERY_CONTEXT,
    COPY_ERROR_PROPERTIES
  }

  private Map<String, String> extendedProperties = new HashMap<>();

  /**
   * Constructs an empty instance of ExtendedProperties.
   */
  public ExtendedProperties() {
  }

  private ExtendedProperties(Map<String, String> extendedProperties) {
    this.extendedProperties = extendedProperties;
  }

  /**
   * Retrieves an extended property of the specified class based on the given property key.
   *
   * @param key   The property key to retrieve the extended property.
   * @param clazz The class of the extended property.
   * @param <T>   The type of the extended property.
   * @return The deserialized extended property object, or null if the property key is not found or the JSON is empty.
   */
  public<T extends ExtendedProperty> T getProperty(PropertyKey key, Class<T> clazz) {
    String json = extendedProperties.get(key.name());
    if (json == null || json.isEmpty()) {
      return null;
    }
    return ExtendedProperty.Util.deserialize(json, clazz);
  }

  /**
   * Sets an extended property using the given property key and value.
   *
   * @param key   The property key to set the extended property.
   * @param value The value of the extended property.
   * @param <T>   The type of the extended property.
   */
  public <T extends ExtendedProperty> void setProperty(PropertyKey key, T value) {
    extendedProperties.put(key.name(), ExtendedProperty.Util.serialize(value));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("ExtendedProperties{\n");
    extendedProperties.forEach((key, value) -> builder.append(key).append("=").append(value).append("\n"));
    return builder.append("}").toString();
  }

  /**
   * A utility class providing methods to convert ExtendedProperties to/from ByteString format.
   */
  public static final class Util {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Util() {
    }

    /**
     * Serialize the given ExtendedProperties object to a ByteString in JSON format.
     *
     * @param properties The ExtendedProperties to serialize.
     * @return The ByteString representation of the ExtendedProperties in JSON format.
     * @throws IllegalStateException If there is an error during serialization.
     */
    public static ByteString getByteString(ExtendedProperties properties) {
      String json;
      try {
        json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(properties.extendedProperties);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot serialize object %s to json", properties), e);
      }
      return ByteString.copyFromUtf8(json);
    }

    /**
     * Deserialize a ByteString in JSON format to an ExtendedProperties object.
     *
     * @param byteString The ByteString to deserialize.
     * @return The deserialized ExtendedProperties object.
     * @throws IllegalStateException If there is an error during deserialization.
     */
    public static ExtendedProperties getProperties(ByteString byteString)  {
      if (byteString == null || byteString.equals(ByteString.EMPTY)) {
        return new ExtendedProperties();
      }
      TypeFactory typeFactory = MAPPER.getTypeFactory();
      MapType mapType = typeFactory.constructMapType(HashMap.class, String.class, String.class);
      try {
        return new ExtendedProperties(MAPPER.readValue(byteString.toStringUtf8(), mapType));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot deserialize ByteString %s to object", byteString.toStringUtf8()), e);
      }
    }
  }

}
