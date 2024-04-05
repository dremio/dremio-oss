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
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVectorHelper;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;

public class NonNullableStructVectorHelper implements ValueVectorHelper {
  private NonNullableStructVector structVector;

  public NonNullableStructVectorHelper(NonNullableStructVector vector) {
    if (vector instanceof StructVector) {
      throw new IllegalArgumentException("Invalid vector: " + vector);
    }
    this.structVector = vector;
  }

  @Override
  public void load(SerializedField metadata, ArrowBuf buf) {
    final List<SerializedField> fields = metadata.getChildList();
    structVector.valueCount = metadata.getValueCount();

    int bufOffset = 0;
    for (final SerializedField child : fields) {
      final Field fieldDef = SerializedFieldHelper.create(child);

      FieldVector vector = structVector.getChild(fieldDef.getName());
      if (vector == null) {
        //         if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, structVector.allocator);
        structVector.putChild(fieldDef.getName(), vector);
      }
      if (child.getValueCount() == 0) {
        vector.clear();
      } else {
        TypeHelper.load(vector, child, buf.slice(bufOffset, child.getBufferLength()));
      }
      bufOffset += child.getBufferLength();
    }

    Preconditions.checkState(bufOffset == buf.capacity());
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
    SerializedField.Builder b =
        SerializedField.newBuilder()
            .setNamePart(NamePart.newBuilder().setName(structVector.getField().getName()))
            .setMajorType(Types.optional(MinorType.STRUCT))
            .setBufferLength(structVector.getBufferSize())
            .setValueCount(structVector.valueCount);

    for (ValueVector v : structVector.getChildren()) {
      b.addChild(TypeHelper.getMetadata(v));
    }
    return b.build();
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
