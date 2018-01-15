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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="com/dremio/dac/explore/model/DACJobResultsSerializer.java" />

package com.dremio.dac.explore.model;

import java.io.IOException;

import com.dremio.exec.store.AbstractRowBasedRecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.arrow.vector.complex.reader.FieldReader;

import com.fasterxml.jackson.core.JsonGenerator;

/**
 * {@link RowBasedRecordWriter} implementation which writes the record batches as JSON for DAC UI consumption.
 * It can be configured to truncate each cell value to a configurable maximum length. When a cell value is truncated,
 * a URL is added to the cell object to fetch the untruncated cell value.
 *
 * Example cell object:
 * {
 *   "t" : "TEXT" // type : DataType, only present if the column type is UNION
 *   "v" : "value" // properly formatted (possibly truncated) cell value as JSON scalar (primitive types),
 *                 // array (list type) or object (complex types).
 *   "u" : "/job/{jobId}/r/{rowNumber}/c/{columnName}" // URL to fetch the complete cell value
 *                                                     // (non-null only when the cell value is truncated)
 * }
 *
 * For each record the JSON output looks like:
 * {
 *   "row" : [
 *       <cell object for column 1>,
 *       <cell object for column 2>,
 *       .....
 *       <cell object for column n>,
 *   ]
 * }
 *
 * NB: Source code generated using FreeMarker template ${.template_name}
 */
public class DACJobResultsSerializer extends AbstractRowBasedRecordWriter {
  private final JsonGenerator jsonGenerator;
  private final SerializationContext currentRowContext;
  private final int recordSizeLimit;
  private DataJsonOutput gen;

  public DACJobResultsSerializer(final JsonGenerator jsonGenerator, final SerializationContext currentRowContext,
      final int recordSizeLimit) {
    this.jsonGenerator = jsonGenerator;
    this.currentRowContext = currentRowContext;
    this.recordSizeLimit = recordSizeLimit;
  }

  public interface SerializationContext {
    String getCellFetchURL(String columnName);
  }

  @Override
  public void setup() throws IOException {
    gen = new DataJsonOutput(jsonGenerator);
  }

  @Override
  public void startRecord() throws IOException {
    gen.writeStartObject();
    gen.writeFieldName("row");
    gen.writeStartArray();
  }

  @Override
  public void endRecord() throws IOException {
    gen.writeEndArray();
    gen.writeEndObject();
  }

  public void startPartition(WritePartition partition){
    if(!partition.isSinglePartition()){
      throw new IllegalStateException();
    }
  }
  
  @Override
  public void abort() throws IOException {
    // no-op: implement in future??
  }

  @Override
  public void close() throws Exception {
    // no-op; no need to close the JsonGenerator as it is the responsibility of the caller
  }

  @Override
  public FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullJsonConverter(fieldId, fieldName, reader);
  }

  public class NullJsonConverter extends FieldConverter {
    private JsonOutputContext context;

    public NullJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      context = new JsonOutputContext(recordSizeLimit);
    }

    @Override
    public void startField() throws IOException {
      context.reset(recordSizeLimit);
      gen.writeStartObject();
      gen.writeFieldName("v");
    }

    @Override
    public void writeField() throws IOException {
      /* NO-OP */
    }

    @Override
    public void endField() throws IOException {
      gen.writeEndObject();
    }
  }

  <#list vv.types as dremioType>
  <#assign finalType = "Nullable" + dremioType >
  <#if dremioType == "Union" || dremioType == "Map" || dremioType == "List" >
  <#assign finalType = dremioType>
  </#if>
  @Override
  public FieldConverter getNew${finalType}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ${finalType}JsonConverter(fieldId, fieldName, reader);
  }

  public class ${finalType}JsonConverter extends FieldConverter {
    private JsonOutputContext context;

    public ${finalType}JsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      context = new JsonOutputContext(recordSizeLimit);
    }

    @Override
    public void startField() throws IOException {
      context.reset(recordSizeLimit);
      gen.writeStartObject();
      <#if dremioType == "Union">
      // For only union type column, type is written for every cell value
      gen.writeFieldName("t");
      gen.writeVarChar(String.valueOf(com.dremio.dac.explore.DataTypeUtil.getDataType(reader.getMinorType())));
      </#if>
      gen.writeFieldName("v");
    }

    @Override
    public void writeField() throws IOException {
      <#assign typeName = dremioType >

      <#switch dremioType>
      <#case "Float4">
        <#assign typeName = "Float">
        <#break>
      <#case "Float8">
        <#assign typeName = "Double">
        <#break>
      </#switch>

      gen.write${typeName}(reader, context);
    }

    @Override
    public void endField() throws IOException {
      // write the URL to fetch the full cell value if the value is truncated.
      if (context.isTruncated()) {
        gen.writeFieldName("u");
        gen.writeVarChar(currentRowContext.getCellFetchURL(fieldName));
      }
      gen.writeEndObject();
    }
  }
  </#list>
}
