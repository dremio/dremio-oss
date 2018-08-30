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
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.vector.NullableVectorDefinitionSetter;

import java.lang.Override;
import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}VectorHelper" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#assign typeMapping = TypeMappings[minor.class]!{}>
<#assign supported = typeMapping.supported!true>
<#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
<#if supported>

<@pp.changeOutputFile name="/org/apache/arrow/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

import com.dremio.common.types.TypeProtos;
import com.dremio.exec.proto.UserBitShared.NamePart;

<#include "/@includes/vv_imports.ftl" />

/**
 * ${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${className} extends BaseValueVectorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  private ${minor.class}Vector vector;

  public ${className} (${minor.class}Vector vector) {
    super(vector);
    this.vector = vector;
  }

  /**
   * Since there are no inner vectors in nullable scalars and complex vectors, we can't delegate the
   * metadata building step to respective inner vector helper classes. For example, a NullableIntVector
   * would earlier build the metadata by setting its name, valuecount and buffer length through super class
   * and then calling BitVectorHelper and IntVectorHelper for corresponding inner vectors. The last two
   * steps are no longer possible since the metadata children are "INNER BUFFERS" as opposed to "INNER VECTORS".
   *
   * So we work at the buffer level and directly set the metadata for the corresponding inner buffer.
   * This also means that there is no designated name part of metadata children. Earlier we could do this
   * since each inner vector had its respective name. Now we just set the name part of metadata children depending
   * on the buffer type we are working with as can be seen in below methods.
   *
   * The order in which children are added to the metadata is SIGNIFICANT and IMPORTANT. This is probably
   * (CONFIRM it) the same order as the order of buffers returned by getBuffers(clear). The same order is used
   * in the load() function when the compound buffer is consumed and we slice it for loading into inner buffers.
   *
   * For Nullable fixed width scalars: [ validity buffer, data buffer ]
   * For Nullable var width scalars:   [ validity buffer, offset buffer, data buffer ]
   *
   * IMPORTANT NOTE ON VARIABLE WIDTH VECTORS FOR BACKWARD COMPATIBILITY
   *
   * As mentioned above, there are inner buffers as opposed to inner vectors. For variable width vectors,
   * there is an additional offset buffer. This means that metadata children should be 3 [ validity, offset, data ].
   * However, this is likely to cause compatibility problems since we have always had 2 metadata children for
   * nullable scalars -- validity and value. For variable width vectors, the latter one has a nested metadata child:
   *     child 0  -> validity
   *     child 1  -> value
   *                -> offset (nested metadata child of child 1)
   *
   * When working with the inner buffers of variable width vector, we follow the same structure instead of adding
   * 3 metadata children corresponding to each of the inner buffers.
   */

  <#if type.major == "VarLen">
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
          .addChild(buildValidityMetadata())
          .addChild(buildOffsetAndDataMetadata())
          .setMajorType(com.dremio.common.util.MajorTypeHelper.getMajorTypeForField(vector.getField()));
  }

  /* keep the offset buffer as a nested child to avoid compatibility problems */
  private SerializedField buildOffsetAndDataMetadata() {
    SerializedField offsetField = SerializedField.newBuilder()
          .setNamePart(NamePart.newBuilder().setName("$offsets$").build())
          .setValueCount((vector.valueCount == 0) ? 0 : vector.valueCount + 1)
          .setBufferLength((vector.valueCount == 0) ? 0 : (vector.valueCount + 1) * 4)
          .setMajorType(com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT4))
          .build();

    SerializedField.Builder dataBuilder = SerializedField.newBuilder()
          .setNamePart(NamePart.newBuilder().setName("$values$").build())
          .setValueCount(vector.valueCount)
          .setBufferLength(vector.getBufferSize() - getValidityBufferSizeFromCount(vector.valueCount))
          .addChild(offsetField)
          .setMajorType(com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.${dremioMinorType}));

    return dataBuilder.build();
  }

  <#else>
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
          .addChild(buildValidityMetadata())
          .addChild(buildDataMetadata())
          .setMajorType(com.dremio.common.util.MajorTypeHelper.getMajorTypeForField(vector.getField()));
  }

  private SerializedField buildDataMetadata() {
    SerializedField.Builder dataBuilder = SerializedField.newBuilder()
          .setNamePart(NamePart.newBuilder().setName("$values$").build())
          .setValueCount(vector.valueCount)
          <#if  minor.class == "Bit">
          .setBufferLength(getValidityBufferSizeFromCount(vector.valueCount))
          <#else>
          .setBufferLength(vector.valueCount * ${type.width})
          </#if>
          .setMajorType(com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.${dremioMinorType}));

    return dataBuilder.build();
  }
  </#if>

  private SerializedField buildValidityMetadata() {
    SerializedField.Builder validityBuilder = SerializedField.newBuilder()
          .setNamePart(NamePart.newBuilder().setName("$bits$").build())
          .setValueCount(vector.valueCount)
          .setBufferLength(getValidityBufferSizeFromCount(vector.valueCount))
          .setMajorType(com.dremio.common.types.Types.required(TypeProtos.MinorType.BIT));

    return validityBuilder.build();
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    /* clear the current buffers (if any) */
    vector.clear();

    /* get the metadata children */
    final SerializedField bitsField = metadata.getChild(0);
    final SerializedField valuesField = metadata.getChild(1);
    final int bitsLength = bitsField.getBufferLength();
    final int capacity = buffer.capacity();
    final int valuesLength = capacity - bitsLength;

    /* load inner validity buffer */
    loadValidityBuffer(bitsField, buffer);

    <#if type.major == "VarLen" >
    /* load inner offset and value buffers */
    loadOffsetAndDataBuffer(valuesField, buffer.slice(bitsLength, valuesLength));
    vector.setLastSet(metadata.getValueCount() - 1);
    <#else>
    /* load inner value buffer */
    loadDataBuffer(valuesField, buffer.slice(bitsLength, valuesLength));
    </#if>
    vector.valueCount = metadata.getValueCount();
  }

  private void loadValidityBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
    assert expectedLength == actualLength:
      String.format("Expected to load %d bytes but actually loaded %d bytes in validity buffer",  expectedLength,
      actualLength);

    vector.validityBuffer = buffer.slice(0, actualLength);
    vector.validityBuffer.writerIndex(actualLength);
    vector.validityBuffer.retain(1);
  }

  public void loadData(SerializedField metadata, ArrowBuf buffer) {
    /* clear the current buffers (if any) */
    vector.clear();

    /* get the metadata children */
    final SerializedField bitsField = metadata.getChild(0);
    final SerializedField valuesField = metadata.getChild(1);
    final int valuesLength = buffer.capacity();


    <#if type.major == "VarLen" >
    vector.allocateNew(valuesLength, metadata.getValueCount());
    <#else>
    vector.allocateNew(metadata.getValueCount());
    </#if>

    /* set inner validity buffer */
    setValidityBuffer(bitsField);

    <#if type.major == "VarLen" >
    /* load inner offset and value buffers */
    vector.offsetBuffer.close();
    vector.valueBuffer.close();
    loadOffsetAndDataBuffer(valuesField, buffer.slice(0, valuesLength));
    vector.setLastSet(metadata.getValueCount() - 1);
    <#else>
    /* load inner value buffer */
    vector.valueBuffer.close();
    loadDataBuffer(valuesField, buffer.slice(0, valuesLength));
    </#if>
  }

  private void setValidityBuffer(SerializedField metadata) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
      assert expectedLength == actualLength:
      String.format("Expected to load %d bytes but actually set %d bytes in validity buffer",  expectedLength,
      actualLength);

    int index;
    for (index = 0;index < valueCount;index ++)
      BitVectorHelper.setValidityBitToOne(vector.validityBuffer, index);
    vector.validityBuffer.writerIndex(actualLength);
  }

  <#if type.major == "VarLen">
  public void loadOffsetAndDataBuffer(SerializedField metadata, ArrowBuf buffer) {
    final SerializedField offsetField = metadata.getChild(0);
    final int offsetActualLength = offsetField.getBufferLength();
    final int valueCount = offsetField.getValueCount();
    final int offsetExpectedLength = valueCount * 4;
    assert offsetActualLength == offsetExpectedLength :
      String.format("Expected to load %d bytes but actually loaded %d bytes in offset buffer", offsetExpectedLength,
      offsetActualLength);

    vector.offsetBuffer = buffer.slice(0, offsetActualLength);
    vector.offsetBuffer.retain(1);
    vector.offsetBuffer.writerIndex(offsetActualLength);

    final int capacity = buffer.capacity();
    final int dataLength = capacity - offsetActualLength;

    vector.valueBuffer = buffer.slice(offsetActualLength, dataLength);
    vector.valueBuffer.retain(1);
    vector.valueBuffer.writerIndex(dataLength);
  }
  <#else>
  public void loadDataBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int actualLength = metadata.getBufferLength();
    final int valueCount = metadata.getValueCount();
    <#if  minor.class == "Bit">
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
    <#else>
    final int expectedLength = valueCount * ${type.width};
    </#if>
    assert actualLength == expectedLength :
      String.format("Expected to load %d bytes but actually loaded %d bytes in data buffer", expectedLength,
      actualLength);

    vector.valueBuffer = buffer.slice(0, actualLength);
    vector.valueBuffer.retain(1);
    vector.valueBuffer.writerIndex(actualLength);
  }
  </#if>
}
</#if>
</#list>
</#list>
