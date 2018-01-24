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
package com.dremio.exec.store.pojo;

import java.lang.reflect.Field;
import java.sql.Timestamp;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableTimeStampMilliVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.google.common.base.Charsets;

import io.netty.buffer.ArrowBuf;

public class Writers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Writers.class);

  public static class IntWriter extends AbstractWriter<NullableIntVector> {
    public static final MajorType TYPE = Types.optional(MinorType.INT);

    public IntWriter(Field field) {
      super(field, TYPE);
      if (field.getType() != int.class && field.getType() != Integer.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Integer i = (Integer) field.get(pojo);
      if (i != null) {
        vector.setSafe(outboundIndex, i);
      }
    }
  }

  public static class BitWriter extends AbstractWriter<NullableBitVector>{
    public static final MajorType TYPE = Types.optional(MinorType.BIT);

    public BitWriter(Field field) {
      super(field, TYPE);
      if (field.getType() != boolean.class && field.getType() != Boolean.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Boolean o = (Boolean) field.get(pojo);
      if (o != null) {
        vector.setSafe(outboundIndex, o ? 1 : 0);
      }
    }

  }

  public static class LongWriter extends AbstractWriter<NullableBigIntVector>{
    public static final MajorType TYPE = Types.optional(MinorType.BIGINT);

    public LongWriter(Field field) {
      super(field, TYPE);
      if (field.getType() != long.class && field.getType() != Long.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Long o = (Long) field.get(pojo);
      if (o != null) {
        vector.setSafe(outboundIndex, o);
      }
    }
  }

  public static class DoubleWriter extends AbstractWriter<NullableFloat8Vector>{
    public static final MajorType TYPE = Types.optional(MinorType.FLOAT8);

    public DoubleWriter(Field field) {
      super(field, TYPE);
      if (field.getType() != double.class && field.getType() != Double.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Double o = (Double) field.get(pojo);
      if (o != null) {
        vector.setSafe(outboundIndex, o);
      }
    }

  }

  private abstract static class AbstractStringWriter extends AbstractWriter<NullableVarCharVector>{
    public static final MajorType TYPE = Types.optional(MinorType.VARCHAR);

    private ArrowBuf data;
    private final NullableVarCharHolder h = new NullableVarCharHolder();

    public AbstractStringWriter(Field field, ArrowBuf managedBuf) {
      super(field, TYPE);
      this.data = managedBuf;
      ensureLength(100);
    }

    void ensureLength(int len) {
      data = data.reallocIfNeeded(len);
    }

    @Override
    public void cleanup() {
    }

    public void writeString(String s, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      if (s == null) {
        return;
      } else {
        h.isSet = 1;
        byte[] bytes = s.getBytes(Charsets.UTF_8);
        ensureLength(bytes.length);
        data.clear();
        data.writeBytes(bytes);
        h.buffer = data;
        h.start = 0;
        h.end = bytes.length;
        vector.setSafe(outboundIndex, h);
      }
    }

  }

  public static class EnumWriter extends AbstractStringWriter{
    public EnumWriter(Field field, ArrowBuf managedBuf) {
      super(field, managedBuf);
      if (!field.getType().isEnum()) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Enum<?> e= ((Enum<?>) field.get(pojo));
      if (e == null) {
        return;
      }
      writeString(e.name(), outboundIndex);
    }
  }

  public static class StringWriter extends AbstractStringWriter {
    public StringWriter(Field field, ArrowBuf managedBuf) {
      super(field, managedBuf);
      if (field.getType() != String.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      String s = (String) field.get(pojo);
      writeString(s, outboundIndex);
    }
  }

  public static class TimeStampMilliWriter extends AbstractWriter<NullableTimeStampMilliVector>{
    public static final MajorType TYPE = Types.optional(MinorType.TIMESTAMP);

    public TimeStampMilliWriter(Field field) {
      super(field, TYPE);
      if (field.getType() != Timestamp.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Timestamp o = (Timestamp) field.get(pojo);
      if (o != null) {
        vector.setSafe(outboundIndex, o.getTime());
      }
    }
  }
}
