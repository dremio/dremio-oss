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
package com.dremio.plugins.elastic.execution;

import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMilliWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.dremio.plugins.elastic.DateFormats.FormatterAndType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Utility classes used to write Elastic data.
 */
class WriteHolders {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriteHolders.class);


  interface WriteHolder {
    void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException;
    void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException;
  }

  static class InvalidWriteHolder implements WriteHolder {

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException {
      throw new IllegalStateException();
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException {
      throw new IllegalStateException();
    }

  }

  static class BitWriteHolder implements WriteHolder {
    private final String name;

    public BitWriteHolder(String name) {
      super();
      this.name = name;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.bit(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.bit(), token, parser);
    }

    private void write(BitWriter writer, JsonToken token, JsonParser parser) throws IOException {
      // TODO JASON - finish other cases
      if (token == JsonToken.VALUE_NUMBER_INT) {
        writer.writeBit(parser.getIntValue() == 1 ? 1 : 0);
      } else if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE){
        writer.writeBit(parser.getBooleanValue() ? 1 : 0);
      } else if (token == JsonToken.VALUE_STRING) {
        writer.writeBit(ElasticsearchJsonReader.parseElasticBoolean(parser.getValueAsString()) ? 1 : 0);
      } else {
        throw UserException.dataReadError()
            .message("While reading from elasticsearch, unexpected data type in a boolean column: " + token).build(logger);
      }

    }

  }

  public static class IntWriteHolder implements WriteHolder {
    private final String name;

    public IntWriteHolder(String name) {
      super();
      this.name = name;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.integer(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.integer(), token, parser);
    }

    public void write(IntWriter writer, JsonToken token, JsonParser parser) throws IOException {
      if (token == JsonToken.VALUE_NUMBER_INT) {
        writer.writeInt(parser.getIntValue());
      } else {
        try {
          writer.writeInt(Integer.parseInt(parser.getValueAsString()));
        } catch (Exception ex1){
          try {
            // it is possible that we need to coerce a float to an integer.
            writer.writeInt((int) Double.parseDouble(parser.getValueAsString()));
          } catch (Exception ex2){
            throw ex1;
          }
        }
      }
    }
  }

  static class BigIntWriteHolder implements WriteHolder {
    private final String name;

    public BigIntWriteHolder(String name) {
      super();
      this.name = name;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.bigInt(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.bigInt(), token, parser);
    }

    private void write(BigIntWriter writer, JsonToken token, JsonParser parser) throws IOException {
      if (token == JsonToken.VALUE_NUMBER_INT) {
        writer.writeBigInt(parser.getLongValue());
      } else {
        try {
          writer.writeBigInt(Long.parseLong(parser.getValueAsString()));
        } catch (Exception ex1){
          try {
            // it is possible that we need to coerce a float to an integer.
            writer.writeBigInt((long) Double.parseDouble(parser.getValueAsString()));
          } catch (Exception ex2){
            throw ex1;
          }
        }
      }

    }
  }

  static class FloatWriteHolder implements WriteHolder {
    private final String name;

    public FloatWriteHolder(String name) {
      super();
      this.name = name;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.float4(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.float4(), token, parser);
    }

    public void write(Float4Writer writer, JsonToken token, JsonParser parser) throws IOException {
      if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_NUMBER_INT) {
        writer.writeFloat4(parser.getFloatValue());
      } else {
        writer.writeFloat4(Float.parseFloat(parser.getValueAsString()));
      }
    }
  }

  static class DoubleWriteHolder implements WriteHolder {
    private final String name;

    public DoubleWriteHolder(String name) {
      super();
      this.name = name;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.float8(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.float8(), token, parser);
    }

    public void write(Float8Writer writer, JsonToken token, JsonParser parser) throws IOException {
      if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_NUMBER_INT) {
        writer.writeFloat8(parser.getDoubleValue());
      } else {
        writer.writeFloat8(Float.parseFloat(parser.getValueAsString()));
      }
    }
  }

  static class DateWriteHolder implements WriteHolder {
    private final FormatterAndType[] formatters;
    private final SchemaPath path;
    private final String name;

    public DateWriteHolder(String name, SchemaPath path, FormatterAndType[] formatters) {
      this.name = name;
      this.formatters = formatters;
      this.path = path;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.dateMilli(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.dateMilli(), token, parser);
    }

    public void write(DateMilliWriter writer, JsonToken token, JsonParser parser) throws IOException {
      writer.writeDateMilli(getMillis(path, parser.getText(), formatters));
    }
  }

  static class TimeWriteHolder implements WriteHolder {
    private final FormatterAndType[] formatters;
    private final SchemaPath path;
    private final String name;

    public TimeWriteHolder(String name, SchemaPath path, FormatterAndType[] formatters) {
      this.name = name;
      this.formatters = formatters;
      this.path = path;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.timeMilli(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.timeMilli(), token, parser);
    }

    public void write(TimeMilliWriter writer, JsonToken token, JsonParser parser) throws IOException {
      writer.writeTimeMilli((int) getMillis(path, parser.getText(), formatters));
    }
  }

  static class VarCharWriteHolder implements WriteHolder {
    private final String name;
    private final WorkingBuffer buffer;

    public VarCharWriteHolder(String name, WorkingBuffer buffer) {
      super();
      this.name = name;
      this.buffer = buffer;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.varChar(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.varChar(), token, parser);
    }

    public void write(VarCharWriter writer, JsonToken token, JsonParser parser) throws IOException {
      writer.writeVarChar(0, buffer.prepareVarCharHolder(parser.getText()), buffer.getBuf());
    }
  }

  static class VarBinaryWriteHolder implements WriteHolder {
    private final String name;
    private final WorkingBuffer buffer;

    public VarBinaryWriteHolder(String name, WorkingBuffer buffer) {
      super();
      this.name = name;
      this.buffer = buffer;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.varBinary(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.varBinary(), token, parser);
    }


    public void write(VarBinaryWriter writer, JsonToken token, JsonParser parser) throws IOException {
      byte[] bytes = DatatypeConverter.parseBase64Binary(parser.getText());
      writer.writeVarBinary(0, buffer.prepareBinary(bytes), buffer.getBuf());
    }
  }

  static class TimestampWriteHolder implements WriteHolder {
    private final FormatterAndType[] formatters;
    private final SchemaPath path;
    private final String name;

    public TimestampWriteHolder(String name, SchemaPath path, FormatterAndType[] formatters) {
      this.name = name;
      this.formatters = formatters;
      this.path = path;
    }

    @Override
    public void writeMap(MapWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.timeStampMilli(name), token, parser);
    }

    @Override
    public void writeList(ListWriter writer, JsonToken token, JsonParser parser) throws IOException{
      write(writer.timeStampMilli(), token, parser);
    }

    public void write(TimeStampMilliWriter writer, JsonToken token, JsonParser parser) throws IOException {
      writer.writeTimeStampMilli(getMillis(path, parser.getText(), formatters));
    }
  }

  private static long getMillis(SchemaPath path, String value, FormatterAndType[] formatters){
    for (FormatterAndType format : formatters) {
      try {
        return format.parseToLong(value);
      } catch (IllegalArgumentException e) {
        logger.debug("Failed to parse date time value {} with format {}", value, format, e);
      }
    }
    throw UserException.dataReadError()
      .message("Failed to parse date time value %s in field %s.", value, path.getAsUnescapedPath())
      .build(logger);
  }


}
