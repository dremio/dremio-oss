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

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;

public class FixedWidthVectorHelper<T extends BaseFixedWidthVector> extends BaseValueVectorHelper<T> {

  private final int size;
  private final boolean bitVector;
  public FixedWidthVectorHelper(T vector) {
    super(vector);
    this.size = vector.getTypeWidth();
    this.bitVector = vector instanceof BitVector;
  }

  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
          .addChild(buildValidityMetadata())
          .addChild(buildDataMetadata())
          .setMajorType(com.dremio.common.util.MajorTypeHelper.getMajorTypeForField(vector.getField()));
  }

  @Override
  protected void loadDataAndPossiblyOffsetBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int actualLength = metadata.getBufferLength();
    final int valueCount = metadata.getValueCount();
    final int expectedLength;
    if(bitVector) {
      expectedLength = getValidityBufferSizeFromCount(valueCount);
    } else {
      expectedLength = valueCount * size;
    }
    assert actualLength == expectedLength :
      String.format("Expected to load %d bytes but actually loaded %d bytes in data buffer", expectedLength,
      actualLength);

    vector.valueBuffer = buffer.slice(0, actualLength);
    vector.valueBuffer.getReferenceManager().retain();
    vector.valueBuffer.writerIndex(actualLength);
    vector.refreshValueCapacity();
  }

  private SerializedField buildDataMetadata() {
    SerializedField.Builder dataBuilder = SerializedField.newBuilder()
          .setNamePart(NamePart.newBuilder().setName("$values$").build())
          .setValueCount(vector.valueCount)
          .setMajorType(com.dremio.common.types.Types.required(CompleteType.fromField(vector.getField()).toMinorType()));
    if(bitVector) {
      dataBuilder.setBufferLength(getValidityBufferSizeFromCount(vector.valueCount));
    } else {
      dataBuilder.setBufferLength(vector.valueCount * size);
    }

    return dataBuilder.build();
  }

}
