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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="com/dremio/dac/explore/model/APIJobResultsSerializer.java" />

  package com.dremio.dac.explore.model;

  import java.io.IOException;

  import com.dremio.exec.store.AbstractRowBasedRecordWriter;
  import com.dremio.exec.store.WritePartition;
  import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
  import org.apache.arrow.vector.complex.reader.FieldReader;

  import com.fasterxml.jackson.core.JsonGenerator;

/**
 * {@link RowBasedRecordWriter} implementation which writes the record batches as JSON for Rest API consumption.
 *
 * NB: Source code generated using FreeMarker template ${.template_name}
 */
public class APIJobResultsSerializer extends AbstractRowBasedRecordWriter {
  private final DataJsonOutput gen;

  public APIJobResultsSerializer(final JsonGenerator jsonGenerator, final boolean convertNumbersToStrings) {
    this.gen = new DataJsonOutput(jsonGenerator, convertNumbersToStrings);
  }


  @Override
  public void startRecord() throws IOException {
    gen.writeStartObject();
  }

  @Override
  public void endRecord() throws IOException {
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
    private String fieldName;

    public NullJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      this.fieldName = fieldName;
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
      /* NO-OP */
    }

    @Override
    public void endField() throws IOException {
    }
  }

  public class OutputContext extends JsonOutputContext {
    public OutputContext() {
      super(0);
    }

    @Override
    public boolean okToWrite() {
      return true;
    }

    @Override
    public int getRemaining() {
      return Integer.MAX_VALUE;
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
    private OutputContext context;
    private String fieldName;

    public ${finalType}JsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
      this.fieldName = fieldName;
      context = new OutputContext();
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
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
    }
  }
  </#list>
}
