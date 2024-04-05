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

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.proto.UserBitShared.SerializedField.Builder;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseValueVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVectorHelper;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;

public class StructVectorHelper implements ValueVectorHelper {
  private StructVector structVector;

  public StructVectorHelper(StructVector vector) {
    this.structVector = vector;
  }

  @Override
  public void load(SerializedField metadata, ArrowBuf buf) {
    /* clear the current buffers (if any) */
    structVector.clear();

    final List<SerializedField> childList = metadata.getChildList();
    int bufOffset = 0;
    long readableBytes = buf.readableBytes();

    structVector.valueCount = metadata.getValueCount();

    // the bits vector is the first child (the order in which the children are added in
    // getMetadataBuilder is significant)
    {
      SerializedField child = childList.get(0);
      final Field fieldDef = SerializedFieldHelper.create(child);
      Preconditions.checkState(
          fieldDef.getName().equals("$bits$"), "expected validity vector: %s", fieldDef);
      bufOffset = loadValidityBuffer(child, buf);
    }

    Set<String> children = new HashSet<>();
    for (Field childField : structVector.getField().getChildren()) {
      children.add(childField.getName().toLowerCase());
    }

    final List<SerializedField> fields = childList.subList(1, childList.size());
    for (final SerializedField child : fields) {
      final Field fieldDef = SerializedFieldHelper.create(child);

      FieldVector vector = structVector.getChild(fieldDef.getName());
      if (vector == null) {
        //         if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, structVector.allocator);
        structVector.putChild(fieldDef.getName(), vector);
      }
      children.remove(fieldDef.getName().toLowerCase());
      bufOffset = load(buf, bufOffset, child, vector);
    }

    for (String remaingChild : children) {
      FieldVector childVector = structVector.getChild(remaingChild);
      childVector.allocateNew();
      childVector.setValueCount(metadata.getValueCount());
    }
    Preconditions.checkState(
        bufOffset == readableBytes,
        "buffer offset %s not equal to readable bytes %s",
        bufOffset,
        readableBytes);
  }

  private int loadValidityBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = BaseValueVectorHelper.getValidityBufferSizeFromCount(valueCount);
    assert expectedLength == actualLength
        : String.format(
            "Expected to load %d bytes in validity buffer but actually loaded %d bytes",
            expectedLength, actualLength);

    structVector.validityBuffer = buffer.slice(0, actualLength);
    structVector.validityBuffer.writerIndex(actualLength);
    structVector.validityBuffer.getReferenceManager().retain(1);

    return actualLength;
  }

  private int load(ArrowBuf buf, int bufOffset, final SerializedField child, FieldVector vector) {
    if (child.getValueCount() == 0) {
      vector.clear();
    } else {
      TypeHelper.load(vector, child, buf.slice(bufOffset, child.getBufferLength()));
    }
    bufOffset += child.getBufferLength();
    return bufOffset;
  }

  @Override
  public void materialize(Field field) {
    List<Field> children = field.getChildren();

    for (Field child : children) {
      FieldVector v = TypeHelper.getNewVector(child, structVector.allocator, structVector.callBack);
      TypeHelper.getHelper(v).ifPresent(t -> t.materialize(child));
      structVector.putChild(child.getName(), v);
    }
  }

  @Override
  public SerializedField getMetadata() {
    int bufferSize = structVector.getBufferSize();
    SerializedField.Builder b =
        SerializedField.newBuilder()
            .setNamePart(NamePart.newBuilder().setName(structVector.getField().getName()))
            .setMajorType(Types.optional(MinorType.STRUCT))
            .setBufferLength(bufferSize)
            .setValueCount(structVector.valueCount);

    b.addChild(buildValidityMetadata());
    int expectedBufferSize =
        BaseValueVectorHelper.getValidityBufferSizeFromCount(structVector.valueCount);
    for (ValueVector v : structVector.getChildren()) {
      SerializedField metadata = TypeHelper.getMetadata(v);
      expectedBufferSize += metadata.getBufferLength();
      b.addChild(metadata);
    }
    Preconditions.checkState(
        expectedBufferSize == bufferSize,
        "Invalid buffer count: %s != %s",
        expectedBufferSize,
        bufferSize);
    return b.build();
  }

  private SerializedField buildValidityMetadata() {
    SerializedField.Builder validityBuilder =
        SerializedField.newBuilder()
            .setNamePart(NamePart.newBuilder().setName("$bits$").build())
            .setValueCount(structVector.valueCount)
            .setBufferLength(
                BaseValueVectorHelper.getValidityBufferSizeFromCount(structVector.valueCount))
            .setMajorType(com.dremio.common.types.Types.required(MinorType.BIT));

    return validityBuilder.build();
  }

  @Override
  public Builder getMetadataBuilder() {
    return null;
  }

  @Override
  public void loadFromValidityAndDataBuffers(
      SerializedField metadata, ArrowBuf dataBuffer, ArrowBuf validityBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadData(SerializedField metadata, ArrowBuf buffer) {
    throw new UnsupportedOperationException();
  }
}
