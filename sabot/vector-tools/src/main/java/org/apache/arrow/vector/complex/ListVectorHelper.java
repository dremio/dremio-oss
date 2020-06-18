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
package org.apache.arrow.vector.complex;


import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;

public class ListVectorHelper extends BaseRepeatedValueVectorHelper<ListVector> {
  private ListVector listVector;

  public ListVectorHelper(ListVector vector) {
    super(vector);
    this.listVector = vector;
  }

  @Override
  public void load(SerializedField metadata, ArrowBuf buffer) {
    /* release the current buffers (if any) */
    listVector.clear();

    /* load inner offset buffer */
    final SerializedField offsetMetadata = metadata.getChild(0);
    final int offsetLength = offsetMetadata.getBufferLength();
    loadOffsetBuffer(offsetMetadata, buffer);

    /* load inner validity buffer */
    final SerializedField bitMetadata = metadata.getChild(1);
    final int bitLength = bitMetadata.getBufferLength();
    loadValidityBuffer(bitMetadata, buffer.slice(offsetLength, bitLength));

    /* load inner data vector */
    final SerializedField vectorMetadata = metadata.getChild(2);
    if (listVector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      listVector.addOrGetVector(FieldType.nullable(getArrowMinorType(vectorMetadata.getMajorType().getMinorType()).getType()));
    }

    final int vectorLength = vectorMetadata.getBufferLength();
    TypeHelper.load(listVector.vector, vectorMetadata, buffer.slice(offsetLength + bitLength, vectorLength));
    listVector.setLastSet(metadata.getValueCount() - 1);
    listVector.valueCount = metadata.getValueCount();
  }

  @Override
  protected void loadDataAndPossiblyOffsetBuffer(SerializedField field, ArrowBuf buf) {
    throw new UnsupportedOperationException();
  }

  private void loadValidityBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
    assert expectedLength == actualLength:
      String.format("Expected to load %d bytes in validity buffer but actually loaded %d bytes", expectedLength,
        actualLength);

    listVector.validityBuffer = buffer.slice(0, actualLength);
    listVector.validityBuffer.writerIndex(actualLength);
    listVector.validityBuffer.retain(1);
  }

  public void materialize(Field field) {
    if (field.getChildren().size() == 0) {
      return;
    }
    Field innerField = field.getChildren().get(0);
    ValueVector innerVector = listVector.addOrGetVector(innerField.getFieldType()).getVector();
    TypeHelper.getHelper(innerVector).ifPresent(t -> t.materialize(innerField));
  }

  public SerializedField.Builder getMetadataBuilder() {
    return SerializedField.newBuilder()
            .setMajorType(MajorType.newBuilder().setMinorType(MinorType.LIST).setMode(DataMode.OPTIONAL).build())
            .setNamePart(NamePart.newBuilder().setName(listVector.getField().getName()))
            .setValueCount(listVector.getValueCount())
            .setBufferLength(listVector.getBufferSize())
            .addChild(buildOffsetMetadata())
            .addChild(buildValidityMetadata())
            .addChild(TypeHelper.getMetadata(listVector.vector));
  }

  protected SerializedField buildValidityMetadata() {
    SerializedField.Builder validityBuilder = SerializedField.newBuilder()
      .setNamePart(NamePart.newBuilder().setName("$bits$").build())
      .setValueCount(listVector.valueCount)
      .setBufferLength(getValidityBufferSizeFromCount(listVector.valueCount))
      .setMajorType(com.dremio.common.types.Types.required(MinorType.BIT));

    return validityBuilder.build();
  }

}
