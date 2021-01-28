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

import java.util.AbstractList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;

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
    if (type.isAssignableFrom(RelRecordType.class)) {
      // Exclude "nullable" field in serializer. Including this field in serializer causes compatibility issue.
      delegate.removeField("nullable");
    }
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

    T result;
    if (dataType instanceof RelRecordType) {
      // We serialized this type disregarding nullable property.
      // Therefore, digest string for this may be inconsistent with nullability which is always false by default,
      // and saving this object to cache will generate incorrect matches.
      // Recreate recordType and match nullability.
      final RelDataType relRecordType = typeFactory.createStructType(dataType.getStructKind(),
          new AbstractList<RelDataType>() {
            @Override
            public RelDataType get(int index) {
              return dataType.getFieldList().get(index).getType();
            }

            @Override
            public int size() {
              return dataType.getFieldCount();
            }
          },
          dataType.getFieldNames());
      final String digest = dataType.getFullTypeString();
      boolean nullable = digest != null ? !digest.endsWith("NOT NULL") : true;
      result = (T) typeFactory.createTypeWithNullability(relRecordType, nullable);
    } else {
      result = (T) typeFactory.copyType(dataType);
    }

    kryo.reference(result);
    return result;
  }

  public static RelDataTypeSerializer of(final Kryo kryo, final Class type, final RelDataTypeFactory typeFactory) {
    return new RelDataTypeSerializer(kryo, type, typeFactory);
  }
}
