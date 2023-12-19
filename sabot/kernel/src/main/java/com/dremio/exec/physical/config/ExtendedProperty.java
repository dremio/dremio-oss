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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An interface representing an extended property that can be serialized and deserialized to/from JSON.
 */
public interface ExtendedProperty {

  final class Util {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Util() {}

    /**
     * Serialize the given extended property to JSON format.
     *
     * @param property The extended property to serialize.
     * @param <T>      The type of the extended property.
     * @return The JSON representation of the extended property.
     * @throws IllegalStateException If there is an error during serialization.
     */
    static<T extends ExtendedProperty> String serialize(T property) {
      try {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(property);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot serialize object %s to json", property), e);
      }
    }

    /**
     * Deserialize the given JSON string to an extended property object of the specified class.
     *
     * @param json       The JSON string to deserialize.
     * @param paramClass The class of the extended property.
     * @param <T>        The type of the extended property.
     * @return The deserialized extended property object.
     * @throws IllegalStateException If there is an error during deserialization.
     */
    static<T extends ExtendedProperty> T deserialize(String json, Class<T> paramClass) {
      try {
        return MAPPER.readValue(json, paramClass);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot deserialize json '%s' to object %s", json, paramClass), e);
      }
    }
  }
}
