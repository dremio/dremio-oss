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
package com.dremio.exec.store.parquet2;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

/** hides the specific writer implementation passed to {@link ParquetGroupConverter} */
interface WriterProvider {

  // TODO I should probably just pass a StructOrListWriter
  StructWriter struct(String name);

  MapWriter map(String name);

  ListWriter list(String name);

  IntWriter integer(String name);

  DecimalWriter decimal(String name, int scale, int precision);

  DecimalWriter decimal(String name);

  DateMilliWriter date(String name);

  TimeMilliWriter time(String name);

  BigIntWriter bigInt(String name);

  TimeStampMilliWriter timeStamp(String name);

  VarBinaryWriter varBinary(String name);

  Float4Writer float4(String name);

  Float8Writer float8(String name);

  BitWriter bit(String name);

  VarCharWriter varChar(String name);

  class StructWriterProvider implements WriterProvider {
    private final StructWriter structWriter;

    StructWriterProvider(StructWriter structWriter) {
      this.structWriter = structWriter;
    }

    @Override
    public StructWriter struct(String name) {
      return structWriter.struct(name);
    }

    @Override
    public MapWriter map(String name) {
      return structWriter.map(name, false);
    }

    @Override
    public ListWriter list(String name) {
      return structWriter.list(name);
    }

    @Override
    public IntWriter integer(String name) {
      return structWriter.integer(name);
    }

    @Override
    public DecimalWriter decimal(String name, int scale, int precision) {
      return structWriter.decimal(name, scale, precision);
    }

    @Override
    public DecimalWriter decimal(String name) {
      return structWriter.decimal(name);
    }

    @Override
    public DateMilliWriter date(String name) {
      return structWriter.dateMilli(name);
    }

    @Override
    public TimeMilliWriter time(String name) {
      return structWriter.timeMilli(name);
    }

    @Override
    public BigIntWriter bigInt(String name) {
      return structWriter.bigInt(name);
    }

    @Override
    public TimeStampMilliWriter timeStamp(String name) {
      return structWriter.timeStampMilli(name);
    }

    @Override
    public VarBinaryWriter varBinary(String name) {
      return structWriter.varBinary(name);
    }

    @Override
    public Float4Writer float4(String name) {
      return structWriter.float4(name);
    }

    @Override
    public Float8Writer float8(String name) {
      return structWriter.float8(name);
    }

    @Override
    public BitWriter bit(String name) {
      return structWriter.bit(name);
    }

    @Override
    public VarCharWriter varChar(String name) {
      return structWriter.varChar(name);
    }
  }

  class ListWriterProvider implements WriterProvider {
    private final ListWriter listWriter;

    ListWriterProvider(ListWriter listWriter) {
      this.listWriter = listWriter;
    }

    @Override
    public StructWriter struct(String name) {
      return listWriter.struct();
    }

    @Override
    public MapWriter map(String name) {
      return listWriter.map();
    }

    @Override
    public ListWriter list(String name) {
      return listWriter.list();
    }

    @Override
    public IntWriter integer(String name) {
      return listWriter.integer();
    }

    @Override
    public DecimalWriter decimal(String name, int scale, int precision) {
      return listWriter.decimal();
    }

    @Override
    public DecimalWriter decimal(String name) {
      return listWriter.decimal();
    }

    @Override
    public DateMilliWriter date(String name) {
      return listWriter.dateMilli();
    }

    @Override
    public TimeMilliWriter time(String name) {
      return listWriter.timeMilli();
    }

    @Override
    public BigIntWriter bigInt(String name) {
      return listWriter.bigInt();
    }

    @Override
    public TimeStampMilliWriter timeStamp(String name) {
      return listWriter.timeStampMilli();
    }

    @Override
    public VarBinaryWriter varBinary(String name) {
      return listWriter.varBinary();
    }

    @Override
    public Float4Writer float4(String name) {
      return listWriter.float4();
    }

    @Override
    public Float8Writer float8(String name) {
      return listWriter.float8();
    }

    @Override
    public BitWriter bit(String name) {
      return listWriter.bit();
    }

    @Override
    public VarCharWriter varChar(String name) {
      return listWriter.varChar();
    }
  }

  class MapWriterProvider implements WriterProvider {
    private final MapWriter mapWriter;

    MapWriterProvider(MapWriter mapWriter) {
      this.mapWriter = mapWriter;
    }

    @Override
    public StructWriter struct(String name) {
      return mapWriter.struct();
    }

    @Override
    public MapWriter map(String name) {
      return mapWriter.map(false);
    }

    @Override
    public ListWriter list(String name) {
      return mapWriter.list();
    }

    @Override
    public IntWriter integer(String name) {
      return mapWriter.integer();
    }

    @Override
    public DecimalWriter decimal(String name, int scale, int precision) {
      return mapWriter.decimal();
    }

    @Override
    public DecimalWriter decimal(String name) {
      return mapWriter.decimal();
    }

    @Override
    public DateMilliWriter date(String name) {
      return mapWriter.dateMilli();
    }

    @Override
    public TimeMilliWriter time(String name) {
      return mapWriter.timeMilli();
    }

    @Override
    public BigIntWriter bigInt(String name) {
      return mapWriter.bigInt();
    }

    @Override
    public TimeStampMilliWriter timeStamp(String name) {
      return mapWriter.timeStampMilli();
    }

    @Override
    public VarBinaryWriter varBinary(String name) {
      return mapWriter.varBinary();
    }

    @Override
    public Float4Writer float4(String name) {
      return mapWriter.float4();
    }

    @Override
    public Float8Writer float8(String name) {
      return mapWriter.float8();
    }

    @Override
    public BitWriter bit(String name) {
      return mapWriter.bit();
    }

    @Override
    public VarCharWriter varChar(String name) {
      return mapWriter.varChar();
    }
  }
}
