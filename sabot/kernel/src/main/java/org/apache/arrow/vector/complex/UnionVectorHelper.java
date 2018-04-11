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


import com.dremio.common.types.TypeProtos;
import org.apache.arrow.vector.BaseValueVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.ComplexTypeHelper;

import io.netty.buffer.ArrowBuf;

public class UnionVectorHelper {
  private UnionVector unionVector;

  public UnionVectorHelper(UnionVector vector) {
    this.unionVector = vector;
  }

  public void load(UserBitShared.SerializedField metadata, ArrowBuf buffer) {
    /* clear the current buffers (if any) */
    unionVector.clear();

    unionVector.valueCount = metadata.getValueCount();

    int typesLength = metadata.getChild(0).getBufferLength();
    int mapLength = metadata.getChild(1).getBufferLength();
    loadTypeBuffer(metadata.getChild(0), buffer);
    TypeHelper.load(unionVector.internalMap, metadata.getChild(1), buffer.slice(typesLength, mapLength));
  }

  private void loadTypeBuffer(SerializedField metadata, ArrowBuf buffer) {
    final int valueCount = metadata.getValueCount();
    final int actualLength = metadata.getBufferLength();
    final int expectedLength = valueCount * 1;
    assert expectedLength == actualLength:
      String.format("Expected to load %d bytes in type buffer but actually loaded %d bytes", expectedLength,
        actualLength);

    unionVector.typeBuffer = buffer.slice(0, actualLength);
    unionVector.typeBuffer.writerIndex(actualLength);
    unionVector.typeBuffer .retain(1);
  }

  public void materialize(Field field) {
    for (Field child : field.getChildren()) {
      FieldVector v = TypeHelper.getNewVector(child, unionVector.getAllocator());
      ComplexTypeHelper.materialize(v, child);
      unionVector.addVector(v);
    }
  }

  public SerializedField getMetadata() {
    SerializedField.Builder b = SerializedField.newBuilder()
            .setNamePart(NamePart.newBuilder().setName(unionVector.getField().getName()))
            .setMajorType(Types.optional(MinorType.UNION))
            .setBufferLength(unionVector.getBufferSize())
            .setValueCount(unionVector.valueCount);

    b.addChild(buildTypeField());
    b.addChild(TypeHelper.getMetadata(unionVector.internalMap));
    return b.build();
  }

  private SerializedField buildTypeField() {
    SerializedField.Builder typeBuilder = SerializedField.newBuilder()
      .setNamePart(UserBitShared.NamePart.newBuilder().setName("types").build())
      .setValueCount(unionVector.valueCount)
      .setBufferLength(unionVector.valueCount)
      .setMajorType(com.dremio.common.types.Types.required(TypeProtos.MinorType.UINT1));

    return typeBuilder.build();
  }

  public MapVector getInternalMap() {
    return unionVector.internalMap;
  }
}
