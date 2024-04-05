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

import io.protostuff.Message;
import io.protostuff.Schema;
import java.io.IOException;

/**
 * An abstract serializer is capable of ser/de as well as backing up/restoring data to/from json.
 *
 * @param <IN> Input type. User's type.
 * @param <OUT> Format used for storage.
 */
public abstract class Serializer<IN, OUT> extends Converter<IN, OUT> {

  public final OUT serialize(IN t) {
    return convert(t);
  }

  public final IN deserialize(OUT bytes) {
    return revert(bytes);
  }

  /**
   * Convert value in bytes to json string.
   *
   * @return json sting
   */
  public abstract String toJson(IN v) throws IOException;

  /**
   * Convert json string to value
   *
   * @return json string
   */
  public abstract IN fromJson(String v) throws IOException;

  public static <IN extends Message<IN>> Serializer<IN, byte[]> of(Schema<IN> schema) {
    return new ProtostuffSerializer<>(schema);
  }

  /**
   * A serializer that converts an IN type to a MID that is serialized to an OUT.
   *
   * @param <IN> Input type.
   * @param <MID> Actually serializable type.
   * @param <OUT> Serialized data.
   */
  private static final class WrappedSerializer<IN, MID, OUT> extends Serializer<IN, OUT> {

    private final Serializer<MID, OUT> midoutSerializer;
    private final Converter<IN, MID> in2mid;

    @Override
    public OUT convert(IN v) {
      return midoutSerializer.serialize(in2mid.convert(v));
    }

    @Override
    public IN revert(OUT v) {
      return in2mid.revert(midoutSerializer.deserialize(v));
    }

    private WrappedSerializer(Converter<IN, MID> in2mid, Serializer<MID, OUT> serializer) {
      this.midoutSerializer = serializer;
      this.in2mid = in2mid;
    }

    @Override
    public String toJson(IN v) throws IOException {
      return midoutSerializer.toJson(in2mid.convert(v));
    }

    @Override
    public IN fromJson(String v) throws IOException {
      return in2mid.revert(midoutSerializer.fromJson(v));
    }
  }

  public static <IN, MID, OUT> Serializer<IN, OUT> wrap(
      Converter<IN, MID> in2mid, Serializer<MID, OUT> mid2out) {
    return new WrappedSerializer<>(in2mid, mid2out);
  }
}
