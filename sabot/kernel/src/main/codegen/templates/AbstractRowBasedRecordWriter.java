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

import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/AbstractRowBasedRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import org.apache.arrow.vector.holders.*;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.io.IOException;
import java.lang.UnsupportedOperationException;

/**
 * generated from ${.template_name}
 */
public abstract class AbstractRowBasedRecordWriter extends RowBasedRecordWriter {

  @Override
  public void setup() throws IOException {
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing Map'");
  }

  @Override
  public FieldConverter getNewStructConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing Struct'");
  }

  @Override
  public FieldConverter getNewUnionConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing Union type'");
  }

  @Override
  public FieldConverter getNewListConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing RepeatedList");
  }

  @Override
  public FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing Null");
  }

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
  @Override
  public FieldConverter getNewNullable${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    throw new UnsupportedOperationException("Doesn't support writing 'Nullable${minor.class}'");
  }
  </#if>
  </#list>
</#list>
}
