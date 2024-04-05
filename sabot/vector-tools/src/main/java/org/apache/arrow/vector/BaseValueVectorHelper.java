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

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.pojo.Field;

public abstract class BaseValueVectorHelper<T extends FieldVector> implements ValueVectorHelper {

  private enum Mode {
    FIXED,
    VARIABLE,
    OTHER
  }

  private final Mode mode;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final T vector;

  private final BaseFixedWidthVector fixedVector;
  private final BaseVariableWidthVector variableVector;
  private final IntConsumer countSetter;
  private final Consumer<ArrowBuf> validitySetter;

  public BaseValueVectorHelper(T vector) {
    this.vector = vector;
    if (vector instanceof BaseFixedWidthVector) {
      fixedVector = (BaseFixedWidthVector) vector;
      countSetter = i -> fixedVector.valueCount = i;
      validitySetter = i -> fixedVector.validityBuffer = i;
      variableVector = null;
      mode = Mode.FIXED;
    } else if (vector instanceof BaseVariableWidthVector) {
      variableVector = (BaseVariableWidthVector) vector;
      countSetter = i -> variableVector.valueCount = i;
      validitySetter = i -> variableVector.validityBuffer = i;
      fixedVector = null;
      mode = Mode.VARIABLE;
    } else {
      mode = Mode.OTHER;
      fixedVector = null;
      variableVector = null;
      countSetter =
          i -> {
            throw new UnsupportedOperationException();
          };
      validitySetter =
          i -> {
            throw new UnsupportedOperationException();
          };
    }
  }

  /** Returns true if non variable */
  private final boolean checkFixedOrVariable() {
    switch (mode) {
      case FIXED:
      case OTHER:
      default:
        return true;
      case VARIABLE:
        return false;
    }
  }

  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder().build();
  }

  @Override
  public SerializedField.Builder getMetadataBuilder() {
    return SerializedField.newBuilder()
        .setNamePart(NamePart.newBuilder().setName(vector.getField().getName()).build())
        .setValueCount(vector.getValueCount())
        .setBufferLength(vector.getBufferSize());
  }

  /* number of bytes for the validity buffer for the given valueCount */
  public static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  protected SerializedField buildValidityMetadata() {
    SerializedField.Builder validityBuilder =
        SerializedField.newBuilder()
            .setNamePart(NamePart.newBuilder().setName("$bits$").build())
            .setValueCount(vector.getValueCount())
            .setBufferLength(getValidityBufferSizeFromCount(vector.getValueCount()))
            .setMajorType(
                MajorType.newBuilder()
                    .setMode(DataMode.REQUIRED)
                    .setMinorType(TypeProtos.MinorType.BIT));

    return validityBuilder.build();
  }

  @Override
  public void loadFromValidityAndDataBuffers(
      SerializedField metadata, ArrowBuf dataBuffer, ArrowBuf validityBuffer) {
    if (!checkFixedOrVariable()) {
      throw new UnsupportedOperationException(
          "this loader is not supported for variable width vectors");
    }

    /* clear the current buffers (if any) */
    vector.clear();
    /* get the metadata children */
    final SerializedField bitsField = metadata.getChild(0);
    final SerializedField valuesField = metadata.getChild(1);
    /* load inner validity buffer */
    loadValidityBuffer(bitsField, validityBuffer);
    /* load inner value buffer */
    loadDataAndPossiblyOffsetBuffer(valuesField, dataBuffer);
    countSetter.accept(metadata.getValueCount());
  }

  private void loadValidityBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
    assert expectedLength == actualLength
        : String.format(
            "Expected to load %d bytes but actually loaded %d bytes in validity buffer",
            expectedLength, actualLength);
    ArrowBuf validity = buffer.slice(0, actualLength);
    validity.writerIndex(actualLength);
    validity.getReferenceManager().retain(1);
    validitySetter.accept(validity);
  }

  @Override
  public void loadData(SerializedField metadata, ArrowBuf buffer) {
    /* clear the current buffers (if any) */
    vector.clear();

    /* get the metadata children */
    final SerializedField bitsField = metadata.getChild(0);
    final SerializedField valuesField = metadata.getChild(1);
    final long valuesLength = buffer.capacity();

    if (checkFixedOrVariable()) {
      fixedVector.allocateNew(metadata.getValueCount());
    } else {
      variableVector.allocateNew(valuesLength, metadata.getValueCount());
    }

    /* set inner validity buffer */
    setValidityBuffer(bitsField);

    if (checkFixedOrVariable()) {
      /* load inner value buffer */
      fixedVector.valueBuffer.close();
      loadDataAndPossiblyOffsetBuffer(valuesField, buffer.slice(0, valuesLength));
    } else {
      /* load inner offset and value buffers */
      variableVector.offsetBuffer.close();
      variableVector.valueBuffer.close();
      loadDataAndPossiblyOffsetBuffer(valuesField, buffer.slice(0, valuesLength));
      variableVector.setLastSet(metadata.getValueCount() - 1);
    }
  }

  @Override
  public void load(SerializedField metadata, ArrowBuf buffer) {
    /* clear the current buffers (if any) */
    vector.clear();

    /* get the metadata children */
    final SerializedField bitsField = metadata.getChild(0);
    final SerializedField valuesField = metadata.getChild(1);
    final long bitsLength = bitsField.getBufferLength();
    final long capacity = buffer.capacity();
    final long valuesLength = capacity - bitsLength;

    /* load inner validity buffer */
    loadValidityBuffer(bitsField, buffer);

    loadDataAndPossiblyOffsetBuffer(valuesField, buffer.slice(bitsLength, valuesLength));
    if (!checkFixedOrVariable()) {
      variableVector.setLastSet(metadata.getValueCount() - 1);
    }
    countSetter.accept(metadata.getValueCount());
  }

  protected abstract void loadDataAndPossiblyOffsetBuffer(SerializedField field, ArrowBuf buf);

  private void setValidityBuffer(SerializedField metadata) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = getValidityBufferSizeFromCount(valueCount);
    assert expectedLength == actualLength
        : String.format(
            "Expected to load %d bytes but actually set %d bytes in validity buffer",
            expectedLength, actualLength);

    int index;
    for (index = 0; index < valueCount; index++) {
      BitVectorHelper.setBit(validityBuffer(), index);
    }
    validityBuffer().writerIndex(actualLength);
  }

  private ArrowBuf validityBuffer() {
    return vector.getValidityBuffer();
  }

  @Override
  public void materialize(Field field) {
    throw new UnsupportedOperationException();
  }
}
