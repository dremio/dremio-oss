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
package com.dremio.exec.planner.serialization.kryo.serializers;

import java.lang.reflect.Field;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;

import com.dremio.common.SuppressForbidden;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

/**
 * Serializer to serialize and deserialize AggregateCall objects.
 */
public class AggregateCallSerializer<T extends AggregateCall> extends Serializer<T> {
  private final FieldSerializer<T> delegate;

  protected AggregateCallSerializer(final Kryo kryo, final Class type) {
    this.delegate = new FieldSerializer<>(kryo, type);
  }

  @Override
  public void write(final Kryo kryo, final Output output, final T object) {
    delegate.write(kryo, output, object);
  }

  @SuppressForbidden
  @Override
  public T read(final Kryo kryo, final Input input, final Class<T> type) {
    final T aggCall = kryo.newInstance(type);
    final FieldSerializer.CachedField[] fields = delegate.getFields();
    for (int i = 0, n = fields.length; i < n; i++) {
      int position = input.position();
      fields[i].read(input, aggCall);
      if (fields[i].getField().getName().equals("collation") && aggCall.getCollation() == null) {
        input.setPosition(position);
        try {
          Field f1 = aggCall.getClass().getDeclaredField("collation");
          f1.setAccessible(true);
          f1.set(aggCall, RelCollations.EMPTY);
        } catch(IllegalAccessException | NoSuchFieldException ignored) {}
      }
    }
    kryo.reference(aggCall);
    T output = aggCall;
    return output;
  }

  public static AggregateCallSerializer of(final Kryo kryo, final Class type) {
    return new AggregateCallSerializer(kryo, type);
  }
}
