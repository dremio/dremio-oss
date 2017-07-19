/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.sources;

import com.dremio.service.namespace.source.proto.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.protostuff.ByteString;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;

/**
 * Source model.
 */
public abstract class Source {
  public static <S extends Source> S fromByteString(SourceType type, ByteString config) {
    @SuppressWarnings("unchecked")
    final S source = (S) fromSource(SourceDefinitions.getSourceSchema(type), config);
    return source;
  }

  @JsonIgnore
  public SourceType getSourceType() {
    // Cannot be done at construction time as:
    // - SourceDefinitions stores Source classes
    // - Protostuff classes have a static DEFAULT_INSTANCE field
    // - it's an initialization loop...
    return SourceDefinitions.getType(this.getClass());
  }

  public ByteString toByteString() {
    @SuppressWarnings("unchecked")
    Schema<Source> schema = (Schema<Source>) SourceDefinitions.getSourceSchema(getSourceType());

    LinkedBuffer buffer = LinkedBuffer.allocate(512);
    byte[] bytes = ProtobufIOUtil.toByteArray(this, schema, buffer);

    return ByteString.copyFrom(bytes);
  }

  private static <S extends Source> S fromSource(Schema<S> schema, ByteString config) {
    final S source = schema.newMessage();
    if (config != null) {
      ProtobufIOUtil.mergeFrom(config.toByteArray(), source, schema);
    }
    return source;
  }
}
