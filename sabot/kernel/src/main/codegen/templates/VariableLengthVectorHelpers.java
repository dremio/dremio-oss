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

import java.lang.Override;

import org.apache.arrow.memory.OutOfMemoryException;
import com.dremio.exec.vector.BaseValueVector;
import com.dremio.exec.vector.VariableWidthVector;

import io.netty.buffer.ArrowBuf;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#if type.major == "VarLen">
<@pp.changeOutputFile name="/org/apache/arrow/vector/${minor.class}VectorHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * generated from ${.template_name}
 */
public final class ${minor.class}VectorHelper extends BaseValueVectorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private ${minor.class}Vector vector;

  public ${minor.class}VectorHelper(${minor.class}Vector vector) {
    super(vector);
    this.vector = vector;
  }

  @Override
  public SerializedField.Builder getMetadataBuilder() {
    SerializedField.Builder builder = super.getMetadataBuilder();
    return builder.setMajorType(com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.${minor.class?upper_case}))
             .addChild(TypeHelper.getMetadata(vector.offsetVector))
             .setValueCount(vector.getAccessor().getValueCount()) //
             .setBufferLength(vector.getBufferSize()); //
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
//     the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField offsetField = metadata.getChild(0);
    TypeHelper.load(vector.offsetVector, offsetField, buffer);

    final int capacity = buffer.capacity();
    final int offsetsLength = offsetField.getBufferLength();
    vector.data = buffer.slice(offsetsLength, capacity - offsetsLength);
    vector.data.writerIndex(capacity - offsetsLength);
    vector.data.retain();
  }

}
</#if> <#-- type.major -->
</#list>
</#list>
