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
package com.dremio.datastore;

import java.io.IOException;

import io.protostuff.Message;
import io.protostuff.Schema;

/**
 * converter to bytes used in the key-value store to serialize keys and values
 * @param <T> the type to be converted to byte[]
 */
public abstract class Serializer<T> extends Converter<T, byte[]> {

  public final byte[] serialize(T t) {
    return convert(t);
  }

  public final T deserialize(byte[] bytes) {
    return revert(bytes);
  }

  /**
   * Convert value in bytes to json string.
   * @return json sting
   */
  public abstract String toJson(T v) throws IOException;

  /**
   * Convert json string to value
   * @return json string
   */
  public abstract T fromJson(String v) throws IOException;

  public static <T extends Message<T>> Serializer<T> of(Schema<T> schema) {
    return new ProtostuffSerializer<>(schema);
  }

}
