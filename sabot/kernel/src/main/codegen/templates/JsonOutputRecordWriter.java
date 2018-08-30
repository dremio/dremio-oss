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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/JSONOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import com.dremio.exec.vector.complex.fn.JsonOutput;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.List;

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
public abstract class JSONOutputRecordWriter extends AbstractRowBasedRecordWriter {

  protected JsonOutput gen;

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>

  
  @Override
  public FieldConverter getNewNullable${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new Nullable${minor.class}JsonConverter(fieldId, fieldName, reader);
  }

  public class Nullable${minor.class}JsonConverter extends FieldConverter {

    public Nullable${minor.class}JsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
  <#assign typeName = minor.class >
  
  <#switch minor.class>
  <#case "UInt1">
  <#case "UInt2">
  <#case "UInt4">
  <#case "UInt8">
  <#case "TimeStampSec">
  <#case "TimeStampMicro">
  <#case "TimeStampNano">
  <#case "DateDay">
  <#case "TimeSec">
  <#case "TimeMicro">
  <#case "TimeNano">
  <#case "FixedSizeBinary">
    <#assign typeName = "unsupported">
    <#break>

  <#case "DateMilli">
    <#assign typeName = "Date">
    <#break>
  <#case "TimeMilli">
    <#assign typeName = "Time">
    <#break>

  <#case "Decimal9">
  <#case "Decimal18">
  <#case "Decimal28Sparse">
  <#case "Decimal28Dense">
  <#case "Decimal38Dense">
  <#case "Decimal38Sparse">
    <#assign typeName = "Decimal">
    <#break>
  <#case "Float4">
    <#assign typeName = "Float">
    <#break>
  <#case "Float8">
    <#assign typeName = "Double">
    <#break>
    
  <#case "IntervalDay">
  <#case "IntervalYear">
    <#assign typeName = "Interval">
    <#break>
    
  <#case "Bit">
    <#assign typeName = "Boolean">
    <#break>  

  <#case "TimeStampMilli">
    <#assign typeName = "Timestamp">
    <#break>  
    
  <#case "VarBinary">
    <#assign typeName = "Binary">
    <#break>  
    
  </#switch>
  
  <#if typeName == "unsupported">
    throw new UnsupportedOperationException("Unable to currently write ${minor.class} type to JSON.");
  <#else>
    gen.write${typeName}(reader);
  </#if>

    }
  }
  </#if>
  </#list>
</#list>

}
