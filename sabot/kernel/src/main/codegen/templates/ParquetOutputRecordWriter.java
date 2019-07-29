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

import org.apache.parquet.io.api.Binary;

import java.lang.Override;
import java.lang.RuntimeException;
import java.util.Arrays;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/ParquetOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import com.google.common.collect.Lists;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;
import org.apache.arrow.vector.holders.*;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.parquet.ParquetTypeHelper;
import com.dremio.exec.vector.*;
import org.apache.arrow.vector.DecimalHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.io.api.Binary;
import io.netty.buffer.ArrowBuf;
import com.dremio.exec.record.BatchSchema;


import com.dremio.common.types.TypeProtos;

import org.joda.time.DateTimeConstants;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of RecordWriter interface which exposes interface:
 *    {@link #writeHeader(List)}
 *    {@link #addField(int,String)}
 * to output the data in string format instead of implementing addField for each type holder.
 *
 * This is useful for text format writers such as CSV, TSV etc.
 *
 * NB: Source code generated using FreeMarker template ${.template_name}
 */
public abstract class ParquetOutputRecordWriter extends AbstractRowBasedRecordWriter {

  private RecordConsumer consumer;
  private MessageType schema;

  public void setUp(MessageType schema, RecordConsumer consumer) {
    this.schema = schema;
    this.consumer = consumer;
  }

  public abstract class ParquetFieldConverter extends FieldConverter {
    public ParquetFieldConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    public abstract void writeValue() throws IOException;
  }

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
  @Override
  public FieldConverter getNewNullable${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Nullable${minor.class}ParquetConverter(fieldId, fieldName, reader);
  }

  public class Nullable${minor.class}ParquetConverter extends ParquetFieldConverter {
    private Nullable${minor.class}Holder holder = new Nullable${minor.class}Holder();
    <#if minor.class?contains("Interval")>
      private final byte[] output = new byte[12];
    </#if>
    <#if minor.class == "Decimal">
    private final byte[] bytes = new byte[16];
    </#if>

    public Nullable${minor.class}ParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeValue() throws IOException {
      <#if  minor.class == "TinyInt" ||
              minor.class == "UInt1" ||
              minor.class == "UInt2" ||
              minor.class == "SmallInt" ||
              minor.class == "Int" ||
              minor.class == "TimeMilli" ||
              minor.class == "Decimal9" ||
              minor.class == "UInt4">
      reader.read(holder);
      consumer.addInteger(holder.value);
      <#elseif
      minor.class == "Float4">
      reader.read(holder);
      consumer.addFloat(holder.value);
      <#elseif
      minor.class == "BigInt" ||
              minor.class == "Decimal18" ||
              minor.class == "TimeStampMilli" ||
              minor.class == "UInt8">
      reader.read(holder);
      consumer.addLong(holder.value);
      <#elseif minor.class == "DateMilli">
      reader.read(holder);
      consumer.addInteger((int) (holder.value / DateTimeConstants.MILLIS_PER_DAY));
      <#elseif
      minor.class == "Float8">
      reader.read(holder);
      consumer.addDouble(holder.value);
      <#elseif
      minor.class == "Bit">
      reader.read(holder);
      consumer.addBoolean(holder.value == 1);
      <#elseif
      minor.class == "Decimal28Sparse" ||
              minor.class == "Decimal38Sparse">
      reader.read(holder);
      byte[] bytes = DecimalUtility.getBigDecimalFromSparse(
              holder.buffer, holder.start, ${minor.class}Holder.nDecimalDigits, holder.scale).unscaledValue().toByteArray();
      byte[] output = new byte[ParquetTypeHelper.getLengthForMinorType(MinorType.${minor.class?upper_case})];
      if (holder.getSign(holder.start, holder.buffer)) {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0xFF);
      } else {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0x0);
      }
      System.arraycopy(bytes, 0, output, output.length - bytes.length, bytes.length);
      consumer.addBinary(Binary.fromByteArray(output));
      <#elseif minor.class?contains("Interval")>
      reader.read(holder);
      <#if minor.class == "IntervalDay">
              Arrays.fill(output, 0, 4, (byte) 0);
      IntervalUtility.intToLEByteArray(holder.days, output, 4);
      IntervalUtility.intToLEByteArray(holder.milliseconds, output, 8);
      <#elseif minor.class == "IntervalYear">
              IntervalUtility.intToLEByteArray(holder.value, output, 0);
      Arrays.fill(output, 4, 8, (byte) 0);
      Arrays.fill(output, 8, 12, (byte) 0);
      <#elseif minor.class == "Interval">
              IntervalUtility.intToLEByteArray(holder.months, output, 0);
      IntervalUtility.intToLEByteArray(holder.days, output, 4);
      IntervalUtility.intToLEByteArray(holder.milliseconds, output, 8);
      </#if>
      consumer.addBinary(Binary.fromByteArray(output));

      <#elseif
      minor.class == "TimeTZ" ||
              minor.class == "Decimal28Dense" ||
              minor.class == "Decimal38Dense">
      <#elseif minor.class == "VarChar" || minor.class == "Var16Char" || minor.class == "VarBinary">
              reader.read(holder);
      ArrowBuf buf = holder.buffer;
      consumer.addBinary(Binary.fromByteBuffer(holder.buffer.nioBuffer(holder.start, holder.end - holder.start)));
      <#elseif minor.class == "Decimal">
      /* Decimals are now Little Endian. So after reading the vector contents into holder,
       * we need to swap the bytes to get BE byte order. Copy the bytes from holder's
       * buffer and swap them before writing the binary.
       */
      reader.read(holder);
      holder.buffer.getBytes(holder.start, bytes, 0, 16);
      DecimalHelper.swapBytes(bytes);
      consumer.addBinary(Binary.fromByteArray(bytes));
      </#if>
    }

    @Override
    public void writeField() throws IOException {
      if (!reader.isSet()) {
        return;
      }
      consumer.startField(fieldName, fieldId);
      writeValue();
      consumer.endField(fieldName, fieldId);
    }
  }
  </#if>
  </#list>
</#list>
  private static class IntervalUtility {
    private static void intToLEByteArray(final int value, final byte[] output, final int outputIndex) {
      int shiftOrder = 0;
      for (int i = outputIndex; i < outputIndex + 4; i++) {
        output[i] = (byte) (value >> shiftOrder);
        shiftOrder += 8;
      }
    }
  }
}
