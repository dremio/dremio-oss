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
package com.dremio.exec.planner.logical.serialization.serializers;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;

public class RelDataTypeSerializer<T extends RelDataType> extends Serializer<T> {
  private final FieldSerializer<T> delegate;
  private final RelDataTypeFactory typeFactory;

  protected RelDataTypeSerializer(final Kryo kryo, final Class type, final RelDataTypeFactory typeFactory) {
    this.typeFactory = Preconditions.checkNotNull(typeFactory, "factory is required");
    this.delegate = new FieldSerializer<>(kryo, type);
  }

  @Override
  public void write(final Kryo kryo, final Output output, final T object) {
    delegate.write(kryo, output, object);
  }

  @Override
  public T read(final Kryo kryo, final Input input, final Class<T> type) {
    // do not use delegate.read because we do not want it to cache the object. Rather, we will cache the normalized type.
    final T dataType = kryo.newInstance(type);
    final FieldSerializer.CachedField[] fields = delegate.getFields();
    for (int i = 0, n = fields.length; i < n; i++) {
      fields[i].read(input, dataType);
    }

    // be gentle to calcite and normalize the returned data type. normalization here means to use same type instances.
    final T result = (T) typeFactory.copyType(dataType);
    kryo.reference(result);
    return result;
  }

  public static RelDataTypeSerializer of(final Kryo kryo, final Class type, final RelDataTypeFactory typeFactory) {
    return new RelDataTypeSerializer(kryo, type, typeFactory);
  }
}
