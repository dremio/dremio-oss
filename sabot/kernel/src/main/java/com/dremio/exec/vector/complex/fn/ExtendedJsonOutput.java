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
package com.dremio.exec.vector.complex.fn;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.math.BigDecimal;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;

/**
 * Writes JSON Output that will wrap Binary, Date, Time, Timestamp, Integer, Decimal and Interval
 * types with wrapping maps for better type resolution upon deserialization.
 */
public class ExtendedJsonOutput extends BasicJsonOutput {

  public ExtendedJsonOutput(JsonGenerator gen) {
    super(gen, DateOutputFormat.ISO);
  }

  @Override
  public void writeBigInt(long value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.INTEGER.serialized);
    super.writeBigInt(value);
    gen.writeEndObject();
  }

  @Override
  public void writeBinary(byte[] value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.BINARY.serialized);
    super.writeBinary(value);
    gen.writeEndObject();
  }

  @Override
  public void writeDate(LocalDateTime value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.DATE.serialized);
    super.writeDate(value);
    gen.writeEndObject();
  }

  @Override
  public void writeTime(LocalDateTime value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.TIME.serialized);
    super.writeTime(value);
    gen.writeEndObject();
  }

  @Override
  public void writeTimestamp(LocalDateTime value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.TIMESTAMP.serialized);
    super.writeTimestamp(value);
    gen.writeEndObject();
  }

  @Override
  public void writeInterval(Period value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.INTERVAL.serialized);
    super.writeInterval(value);
    gen.writeEndObject();
  }

  @Override
  public void writeBigIntNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.INTEGER.serialized);
    super.writeBigIntNull();
    gen.writeEndObject();
  }

  @Override
  public void writeBinaryNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.BINARY.serialized);
    super.writeBinaryNull();
    gen.writeEndObject();
  }

  @Override
  public void writeDateNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.DATE.serialized);
    super.writeDateNull();
    gen.writeEndObject();
  }

  @Override
  public void writeTimeNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.TIME.serialized);
    super.writeTimeNull();
    gen.writeEndObject();
  }

  @Override
  public void writeTimestampNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.TIMESTAMP.serialized);
    super.writeTimestampNull();
    gen.writeEndObject();
  }

  @Override
  public void writeIntervalNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.INTERVAL.serialized);
    super.writeIntervalNull();
    gen.writeEndObject();
  }

  @Override
  public void writeDecimal(BigDecimal value) throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.DECIMAL.serialized);
    super.writeDecimal(value);
    gen.writeEndObject();
  }

  @Override
  public void writeDecimalNull() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName(ExtendedType.DECIMAL.serialized);
    super.writeDecimalNull();
    gen.writeEndObject();
  }

  @Override
  public void writeTinyInt(byte value) throws IOException {
    writeBigInt(value);
  }

  @Override
  public void writeSmallInt(short value) throws IOException {
    writeBigInt(value);
  }

  @Override
  public void writeInt(int value) throws IOException {
    writeBigInt(value);
  }

  @Override
  public void writeTinyIntNull() throws IOException {
    writeBigIntNull();
  }

  @Override
  public void writeSmallIntNull() throws IOException {
    writeBigIntNull();
  }

  @Override
  public void writeIntNull() throws IOException {
    writeBigIntNull();
  }
}
