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

import com.dremio.exec.planner.physical.WriterPrel;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/EventBasedRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;

/** Reads records from the RecordValueAccessor and writes into RecordWriter. */
public class EventBasedRecordWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EventBasedRecordWriter.class);

  private VectorAccessible batch;
  private RowBasedRecordWriter recordWriter;
  private List<FieldConverter> fieldConverters;

  public EventBasedRecordWriter(VectorAccessible batch, RowBasedRecordWriter recordWriter)
          throws IOException {
    this.batch = batch;
    this.recordWriter = recordWriter;

    initFieldWriters();
  }

  /**
   * Write given number of records starting from the beginning of the batch.
   * @param numRecordsToWriter
   * @return
   * @throws IOException
   */
  public int write(int offset, int length) throws IOException {
    final int initialOffset = offset;
    final int max = offset + length;
    for (; offset < max; offset++) {
      recordWriter.startRecord();
      // write the current record
      for (FieldConverter converter : fieldConverters) {
        converter.setPosition(offset);
        converter.startField();
        converter.writeField();
        converter.endField();
      }
      recordWriter.endRecord();
    }

    return offset - initialOffset;
  }

  /**
   * Write a single record located at given index.
   * @param index
   * @return
   * @throws IOException
   */
  public int writeOneRecord(int index) throws IOException {
    recordWriter.startRecord();
    // write the current record
    for (FieldConverter converter : fieldConverters) {
      converter.setPosition(index);
      converter.startField();
      converter.writeField();
      converter.endField();
    }
    recordWriter.endRecord();

    return 1;
  }

  private void initFieldWriters() throws IOException {
    fieldConverters = Lists.newArrayList();
    try {
      int fieldId = 0;
      for (VectorWrapper w : batch) {
        if (w.getField().getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
          continue;
        }
        FieldReader reader = w.getValueVector().getReader();
        FieldConverter converter = getConverter(recordWriter, fieldId, w.getField().getName(),
            w.getValueVector().getMinorType(), reader);
        if (converter != null) {
          fieldId++;
          fieldConverters.add(converter);
        }
      }
    } catch(Exception e) {
      logger.error("Failed to create FieldWriter.", e);
      throw new IOException("Failed to initialize FieldWriters.", e);
    }
  }

  public static abstract class FieldConverter {
    protected int fieldId;
    protected String fieldName;
    protected FieldReader reader;

    public FieldConverter(int fieldId, String fieldName, FieldReader reader) {
      this.fieldId = fieldId;
      this.fieldName = fieldName;
      this.reader = reader;
    }

    public FieldConverter(FieldConverter fieldConverter) {
      this.fieldId = fieldConverter.fieldId;
      this.fieldName = fieldConverter.fieldName;
      this.reader = fieldConverter.reader;
    }

    public void setPosition(int index) {
      reader.setPosition(index);
    }

    public void startField() throws IOException {
      // no op
    }

    public void endField() throws IOException {
      // no op
    }

    public abstract void writeField() throws IOException;
  }

  public static FieldConverter getConverter(final RowBasedRecordWriter recordWriter, final int fieldId,
      final String fieldName, final MinorType type, final  FieldReader reader) {
    if (reader.getField().getFieldType().getType().getTypeID() == ArrowTypeID.Union) {
      return recordWriter.getNewUnionConverter(fieldId, fieldName, reader);
    }
    switch (type) {
      case NULL:
        return recordWriter.getNewNullConverter(fieldId, fieldName, reader);
      case UNION:
        return recordWriter.getNewUnionConverter(fieldId, fieldName, reader);
      case STRUCT:
        return recordWriter.getNewMapConverter(fieldId, fieldName, reader);
      case LIST:
        return recordWriter.getNewListConverter(fieldId, fieldName, reader);
        <#list vv.types as type>
        <#list type.minor as minor>
        <#assign typeMapping = TypeMappings[minor.class]!{}>
        <#assign supported = typeMapping.supported!true>
        <#if supported>
      case ${minor.class?upper_case}:
        return recordWriter.getNewNullable${minor.class}Converter(fieldId, fieldName, reader);
        </#if>
      </#list>
      </#list>
      default:
        throw new UnsupportedOperationException("unknown type: " + type);
    }
  }
}
