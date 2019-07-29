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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A kryo serializer for hadoop's {@link Writable}
 */
public class WritableSerializer<T extends Writable> extends Serializer<T> {

  @Override
  public void write(final Kryo kryo, final Output output, final T object) {
    try {
      object.write(new DataOutputStream(output));
    } catch (final IOException e) {
      throw new RuntimeException("unable to serialize Writable object", e);
    }
  }

  @Override
  public T read(final Kryo kryo, final Input input, final Class<T> type) {
    try {
      final T object = kryo.newInstance(type);
      kryo.reference(object);
      object.readFields(new DataInputStream(input));
      return object;
    } catch (final IOException e) {
      throw new RuntimeException("unable to deserialize Writable object", e);
    }
  }
}
