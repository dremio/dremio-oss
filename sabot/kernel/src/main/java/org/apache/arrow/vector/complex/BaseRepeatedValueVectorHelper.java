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
package org.apache.arrow.vector.complex;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVectorHelper;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.SerializedField;

import io.netty.buffer.ArrowBuf;

public class BaseRepeatedValueVectorHelper extends BaseValueVectorHelper {

  private BaseRepeatedValueVector vector;

  public BaseRepeatedValueVectorHelper(BaseRepeatedValueVector vector) {
    super(vector);
    this.vector = vector;
  }

  public SerializedField.Builder getMetadataBuilder() {
    SerializedField offsetField = buildOffsetMetadata();
    return super.getMetadataBuilder()
        .addChild(offsetField)
        .addChild(TypeHelper.getMetadata(vector.vector));
  }

  protected SerializedField buildOffsetMetadata() {
    SerializedField.Builder offsetBuilder = SerializedField.newBuilder()
      .setNamePart(UserBitShared.NamePart.newBuilder().setName("$offsets$").build())
      .setValueCount((vector.valueCount == 0) ? 0 : vector.valueCount + 1)
      .setBufferLength((vector.valueCount == 0) ? 0 : (vector.valueCount + 1) * 4)
      .setMajorType(com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.UINT4));

    return offsetBuilder.build();
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    /* release the current buffers (if any) */
    vector.clear();

    /* get the metadata for all children */
    final SerializedField offsetMetadata = metadata.getChild(0);
    final SerializedField vectorMetadata = metadata.getChild(1);
    final int offsetLength = offsetMetadata.getBufferLength();
    final int vectorLength = vectorMetadata.getBufferLength();

    /* load inner offset buffer */
    loadOffsetBuffer(offsetMetadata, buffer);

    /* load inner data vector */
    if (vector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      vector.addOrGetVector(FieldType.nullable(getArrowMinorType(metadata.getMajorType().getMinorType()).getType()));
    }

    TypeHelper.load(vector.vector, vectorMetadata, buffer.slice(offsetLength, vectorLength));
  }

  protected void loadOffsetBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = valueCount * BaseRepeatedValueVector.OFFSET_WIDTH;
    assert actualLength == expectedLength :
      String.format("Expected to load %d bytes in offset buffer but actually loaded %d bytes", expectedLength,
        actualLength);

    vector.offsetBuffer = buffer.slice(0, actualLength);
    vector.offsetBuffer.writerIndex(actualLength);
    vector.offsetBuffer.retain(1);
  }

  /*
   * RepeatedValueVector (or ListVector) no longer has inner offsetVector.
   * There is just the buffer that stores all offsets. In FixedWidthRepeatedReader
   * we earlier had the liberty to get the offset vector and mutate it directly
   * in a safe manner since the operations were carried out on the inner vector.
   * We no longer have such provision of setting offsets in a safe manner. Hence
   * we need these helper methods that directly work on the offset buffer
   * of the vector, do get/set operations and reallocation if needed.
   * An alternative would be to introduce static methods in ListVector or
   * BaseRepeatedValueVector interface for specifically setting data in
   * inner offset buffer but that approach is going to pollute the public
   * API in OSS.
   */
  public static void setOffsetHelper(final BaseRepeatedValueVector vector,
                                     final int indexToGet, final int indexToSet,
                                     final BufferAllocator vectorAllocator) {
    final int valueToSet = vector.offsetBuffer.getInt(indexToGet * BaseRepeatedValueVector.OFFSET_WIDTH);
    while (indexToSet >= getOffsetBufferValueCapacity(vector)) {
      reallocOffsetBuffer(vector, vectorAllocator);
    }
    vector.offsetBuffer.setInt(indexToSet * BaseRepeatedValueVector.OFFSET_WIDTH, valueToSet);
  }

  private static int getOffsetBufferValueCapacity(final BaseRepeatedValueVector vector) {
    return (int)((vector.offsetBuffer.capacity() * 1.0)/BaseRepeatedValueVector.OFFSET_WIDTH);
  }

  private static void reallocOffsetBuffer(final BaseRepeatedValueVector vector,
                                          final BufferAllocator allocator) {
    int currentBufferCapacity = vector.offsetBuffer.capacity();
    long baseSize = (long)vector.offsetAllocationSizeInBytes;
    if(baseSize < (long)currentBufferCapacity) {
      baseSize = (long)currentBufferCapacity;
    }
    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);
    ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setZero(0, newBuf.capacity());
    newBuf.setBytes(0, vector.offsetBuffer, 0, currentBufferCapacity);
    vector.offsetBuffer.release(1);
    vector.offsetBuffer = newBuf;
    vector.offsetAllocationSizeInBytes = (int)newAllocationSize;
  }
}
