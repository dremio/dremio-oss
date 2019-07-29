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
package com.dremio.datastore;

import java.io.IOException;

import com.dremio.common.utils.ProtostuffUtil;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

/**
 * a Serializer implementation for protostuff generated classes
 * @param <T> a protostuff generated class
 */
public class ProtostuffSerializer<T extends Message<T>> extends Serializer<T> {

  private Schema<T> schema;

  // TODO: verify if this is necessary
  private final ThreadLocal<LinkedBuffer> tl = new ThreadLocal<LinkedBuffer>() {
    @Override
    protected LinkedBuffer initialValue() {
      return LinkedBuffer.allocate(256);
    }
  };

  public ProtostuffSerializer(Schema<T> schema) {
    this.schema = schema;
  }

  @Override
  public byte[] convert(T t) {
    final LinkedBuffer buffer = tl.get();
    try {
      return ProtostuffIOUtil.toByteArray(t, schema, buffer);
    } finally {
      buffer.clear();
    }
  }

  @Override
  public T revert(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    T t = schema.newMessage();
    ProtostuffIOUtil.mergeFrom(bytes, t, schema);
    return t;
  }

  @Override
  public String toJson(T v) {
    return ProtostuffUtil.toJSON(v, schema, false);
  }

  @Override
  public T fromJson(String v) throws IOException {
    return ProtostuffUtil.fromJSON(v, schema, false);
  }
}
