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
package com.dremio.exec.store.dfs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The ErrorInfo interface represents the contract for classes that hold error information.
 */
public interface ErrorInfo {

  /**
   * The Util class provides utility methods for serializing and deserializing error information to and from JSON.
   * It contains static methods to convert error information objects to their JSON representation and vice versa using Jackson ObjectMapper.
   * The methods handle exceptions related to JSON processing and throw IllegalStateException if serialization or deserialization fails.
   */
  final class Util {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Util() {
    }

    /**
     * Serializes the given ErrorInfo object to its JSON representation.
     *
     * @param info The ErrorInfo object to be serialized.
     * @param <T>  The type of the ErrorInfo object.
     * @return The JSON representation of the ErrorInfo object.
     * @throws IllegalStateException If serialization to JSON fails.
     */
    public static <T extends ErrorInfo> String getJson(T info) {
      try {
        return MAPPER.writeValueAsString(info);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot serialize error info object to json:\n%s", info), e);
      }
    }

    /**
     * Deserializes the given JSON string to an ErrorInfo object of the specified class.
     *
     * @param json  The JSON string to be deserialized.
     * @param clazz The class of the ErrorInfo object to be deserialized.
     * @param <T>   The type of the ErrorInfo object.
     * @return The deserialized ErrorInfo object.
     * @throws IllegalStateException If deserialization from JSON fails.
     */
    public static <T extends ErrorInfo> T getInfo(String json, Class<T> clazz) {
      try {
        return MAPPER.readValue(json, clazz);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(String.format("Cannot deserialize json to error info object %s:\n%s ", clazz, json), e);
      }
    }
  }
}
