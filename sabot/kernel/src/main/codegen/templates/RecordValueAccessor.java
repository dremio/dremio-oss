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
<@pp.changeOutputFile name="/com/dremio/exec/record/RecordValueAccessor.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.record;

import com.google.common.collect.Lists;

import org.apache.arrow.vector.types.pojo.Field;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.expr.TypeHelper;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.*;

/**
 * generated from ${.template_name} 
 * Wrapper around VectorAccessible to iterate over the records and fetch fields within a record. 
 */
public class RecordValueAccessor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordValueAccessor.class);

  private VectorAccessible batch;
  private int currentIndex;
  private ValueVector[] vectors;

  public RecordValueAccessor(VectorAccessible batch) {
    this.batch = batch;

    resetIterator();
    initVectorWrappers();
  }

  public void resetIterator() {
    currentIndex = -1;
  }

  private void initVectorWrappers() {
    BatchSchema schema = batch.getSchema();
    vectors = new ValueVector[schema.getFieldCount()];
    int fieldId = 0;
    for (Field field : schema) {
      Class<? extends ValueVector> vectorClass = TypeHelper.getValueVectorClass(field);
      vectors[fieldId] = batch.getValueAccessorById(vectorClass, fieldId).getValueVector();
      fieldId++;
    }
  }

  public boolean next() {
    return ++currentIndex < batch.getRecordCount();
  }

  public void getFieldById(int fieldId, ComplexHolder holder) {
    final ValueVector vv = vectors[fieldId];
    if (vv instanceof BaseDataValueVector) {
      holder.isSet =vv.getAccessor().isNull(currentIndex) ? 1 : 0;
    } else {
      holder.isSet = vv.isNull(currentIndex) ? 1 : 0;
    }
    holder.reader = vv.getReader();
    holder.reader.setPosition(currentIndex);
  }

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
    <#list vv.modes as mode>
  public void getFieldById(int fieldId, ${mode.prefix}${minor.class}Holder holder) {
    <#if mode.prefix == "Nullable">
    ((${mode.prefix}${minor.class}Vector) vectors[fieldId]).get(currentIndex, holder);
    <#else>
    ((${mode.prefix}${minor.class}Vector) vectors[fieldId]).getAccessor().get(currentIndex, holder);
    </#if>
  }

    </#list>
  </#if>
  </#list>
</#list>
}
