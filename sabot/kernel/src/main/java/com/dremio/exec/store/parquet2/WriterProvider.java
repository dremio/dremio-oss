/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * hides the specific writer implementation passed to {@link ParquetGroupConverter}
 */
interface WriterProvider {

  //TODO I should probably just pass a MapOrListWriter
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

  class MapWriterProvider implements WriterProvider {
    private final MapWriter mapWriter;

    MapWriterProvider(MapWriter mapWriter) {
      this.mapWriter = mapWriter;
    }

    @Override
    public MapWriter map(String name) {
      return mapWriter.map(name);
    }

    @Override
    public ListWriter list(String name) {
      return mapWriter.list(name);
    }

    @Override
    public IntWriter integer(String name) {
      return mapWriter.integer(name);
    }

    @Override
    public DecimalWriter decimal(String name, int scale, int precision) {
      return mapWriter.decimal(name, scale, precision);
    }

    @Override
    public DecimalWriter decimal(String name) {
      return mapWriter.decimal(name);
    }

    @Override
    public DateMilliWriter date(String name) {
      return mapWriter.dateMilli(name);
    }

    @Override
    public TimeMilliWriter time(String name) {
      return mapWriter.timeMilli(name);
    }

    @Override
    public BigIntWriter bigInt(String name) {
      return mapWriter.bigInt(name);
    }

    @Override
    public TimeStampMilliWriter timeStamp(String name) {
      return mapWriter.timeStampMilli(name);
    }

    @Override
    public VarBinaryWriter varBinary(String name) {
      return mapWriter.varBinary(name);
    }

    @Override
    public Float4Writer float4(String name) {
      return mapWriter.float4(name);
    }

    @Override
    public Float8Writer float8(String name) {
      return mapWriter.float8(name);
    }

    @Override
    public BitWriter bit(String name) {
      return mapWriter.bit(name);
    }

    @Override
    public VarCharWriter varChar(String name) {
      return mapWriter.varChar(name);
    }
  }

  class ListWriterProvider implements WriterProvider {
    private final ListWriter listWriter;

    ListWriterProvider(ListWriter listWriter) {
      this.listWriter = listWriter;
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
}
