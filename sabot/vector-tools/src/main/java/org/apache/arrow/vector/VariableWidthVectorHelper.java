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
package org.apache.arrow.vector;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;

import io.netty.buffer.ArrowBuf;

public class VariableWidthVectorHelper<T extends BaseVariableWidthVector> extends BaseValueVectorHelper<T> {

  public VariableWidthVectorHelper(T vector) {
    super(vector);
  }

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
          .setMajorType(com.dremio.common.types.Types.required(CompleteType.fromField(vector.getField()).toMinorType()));

    return dataBuilder.build();
  }

  @Override
  public void loadDataAndPossiblyOffsetBuffer(SerializedField metadata, ArrowBuf buffer) {
    final SerializedField offsetField = metadata.getChild(0);
    final int offsetActualLength = offsetField.getBufferLength();
    final int valueCount = offsetField.getValueCount();
    final int offsetExpectedLength = valueCount * 4;
    assert offsetActualLength == offsetExpectedLength :
      String.format("Expected to load %d bytes but actually loaded %d bytes in offset buffer", offsetExpectedLength,
      offsetActualLength);

    vector.offsetBuffer = buffer.slice(0, offsetActualLength);
    vector.offsetBuffer.getReferenceManager().retain();
    vector.offsetBuffer.writerIndex(offsetActualLength);

    final long capacity = buffer.capacity();
    final long dataLength = capacity - offsetActualLength;

    vector.valueBuffer = buffer.slice(offsetActualLength, dataLength);
    vector.valueBuffer.getReferenceManager().retain();
    vector.valueBuffer.writerIndex(dataLength);
  }

}
