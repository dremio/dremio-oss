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

import com.dremio.exec.store.AbstractRecordWriter;

import java.lang.Override;
import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/StringOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import com.google.common.collect.Lists;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.RecordWriter.OutputEntryListener;
import com.dremio.exec.vector.*;
import com.dremio.sabot.exec.context.OperatorContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.*;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
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
public abstract class StringOutputRecordWriter extends AbstractRowBasedRecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringOutputRecordWriter.class);
  private final int maxCellSize;

  protected StringOutputRecordWriter(OperatorContext context) {
    maxCellSize = Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
  }

  @Override
  public void setup() throws IOException {
    BatchSchema schema = incoming.getSchema();
    List<String> columnNames = Lists.newArrayList();
    for (int i=0; i < schema.getFieldCount(); i++) {
      columnNames.add(schema.getColumn(i).getName());
    }

    setup(columnNames);
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldConverter getNewStructConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException();
  }

  public FieldConverter getNewRepeatedMapConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException();
  }
  public FieldConverter getNewRepeatedListConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException();
  }

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
  @Override
  public FieldConverter getNewNullable${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Nullable${minor.class}StringFieldConverter(fieldId, fieldName, reader);
  }

  public class Nullable${minor.class}StringFieldConverter extends FieldConverter {
    private Nullable${minor.class}Holder holder = new Nullable${minor.class}Holder();

    public Nullable${minor.class}StringFieldConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
    if (!reader.isSet()) {
      addField(fieldId, null);
      return;
    }

    reader.read(holder);
  <#if  minor.class == "TinyInt" ||
        minor.class == "UInt1" ||
        minor.class == "UInt2" ||
        minor.class == "SmallInt" ||
        minor.class == "Int" ||
        minor.class == "UInt4" ||
        minor.class == "Float4" ||
        minor.class == "BigInt" ||
        minor.class == "UInt8" ||
        minor.class == "Float8">
    addField(fieldId, String.valueOf(holder.value));
  <#elseif minor.class == "Bit">
    addField(fieldId, holder.value == 0 ? "false" : "true");
  <#elseif
        minor.class == "DateMilli" ||
        minor.class == "TimeMilli" ||
        minor.class == "TimeTZ" ||
        minor.class == "TimeStampMilli" ||
        minor.class == "IntervalYear" ||
        minor.class == "IntervalDay" ||
        minor.class == "Interval" ||
        minor.class == "Decimal" ||
        minor.class == "Decimal18" ||
        minor.class == "Decimal28Dense" ||
        minor.class == "Decimal38Dense" ||
        minor.class == "Decimal28Sparse" ||
        minor.class == "Decimal38Sparse">

    // TODO: error check
    addField(fieldId, reader.readObject().toString());

  <#elseif minor.class == "VarChar" || minor.class == "Var16Char" || minor.class == "VarBinary">
    FieldSizeLimitExceptionHelper.checkSizeLimit(holder.end - holder.start, maxCellSize, fieldId, logger);
    addField(fieldId, reader.readObject().toString());
  <#else>
    throw new UnsupportedOperationException(String.format("Unsupported field type: %s",
      holder.getClass()));
   </#if>
    }
  }
  </#if>
  </#list>
</#list>

  /**
   * Start new schema.
   * @param columnNames
   * @return Optional metadata of last batch
   * @throws Exception
   */
  public abstract void setup(List<String> columnNames) throws IOException;
  public abstract void addField(int fieldId, String value) throws IOException;
}
