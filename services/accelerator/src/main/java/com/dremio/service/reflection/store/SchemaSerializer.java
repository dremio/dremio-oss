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
package com.dremio.service.reflection.store;

import java.io.IOException;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;

import io.protostuff.Message;
import io.protostuff.Schema;

/**
 * Generic protostuff schema serializer
 */
public abstract class SchemaSerializer<T extends Message<T>> extends Serializer<T, byte[]> {
  private final Serializer<T, byte[]> serializer;

  public SchemaSerializer(final Schema<T> schema) {
    this.serializer = ProtostuffSerializer.of(schema);
  }

  @Override
  public byte[] convert(final T value) {
    return serializer.convert(value);
  }

  @Override
  public T revert(final byte[] bytes) {
    return serializer.revert(bytes);
  }

  @Override
  public String toJson(final T v) throws IOException {
    return serializer.toJson(v);
  }

  @Override
  public T fromJson(final String v) throws IOException {
    return serializer.fromJson(v);
  }
}
