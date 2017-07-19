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
package org.apache.arrow.vector.complex;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.BasicTypeHelper;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.ComplexTypeHelper;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

public class NullableMapVectorHelper {
  private NullableMapVector mapVector;

  public NullableMapVectorHelper(NullableMapVector vector) {
    this.mapVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buf) {
    final List<SerializedField> childList = metadata.getChildList();
    int bufOffset = 0;
    int readableBytes = buf.readableBytes();

    mapVector.valueCount = metadata.getValueCount();

    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    {
      SerializedField child = childList.get(0);
      final Field fieldDef = SerializedFieldHelper.create(child);
      Preconditions.checkState(fieldDef.getName().equals("$bits$"), "expected validity vector: %s", fieldDef);
      bufOffset = load(buf, bufOffset, child, mapVector.bits);
    }

    Set<String> children = new HashSet<>();
    for (Field childField : mapVector.getField().getChildren()) {
      children.add(childField.getName().toLowerCase());
    }

    final List<SerializedField> fields = childList.subList(1, childList.size());
    for (final SerializedField child : fields) {
      final Field fieldDef = SerializedFieldHelper.create(child);


      FieldVector vector = mapVector.getChild(fieldDef.getName());
      if (vector == null) {
//         if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, mapVector.allocator);
        mapVector.putChild(fieldDef.getName(), vector);
      }
      children.remove(fieldDef.getName().toLowerCase());
      bufOffset = load(buf, bufOffset, child, vector);

    }

    for (String remaingChild : children) {
      FieldVector childVector = mapVector.getChild(remaingChild);
      childVector.allocateNew();
      childVector.getMutator().setValueCount(metadata.getValueCount());
    }
    Preconditions.checkState(bufOffset == readableBytes, "buffer offset %s not equal to readable bytes %s", bufOffset, readableBytes);
  }

  private int load(ArrowBuf buf, int bufOffset, final SerializedField child, ValueVector vector) {
    if (child.getValueCount() == 0) {
      vector.clear();
    } else {
      TypeHelper.load(vector, child, buf.slice(bufOffset, child.getBufferLength()));
    }
    bufOffset += child.getBufferLength();
    return bufOffset;
  }

  public void materialize(Field field) {
    List<Field> children = field.getChildren();

    for (Field child : children) {
      FieldVector v = TypeHelper.getNewVector(child, mapVector.allocator, mapVector.callBack);
      ComplexTypeHelper.materialize(v, child);
      mapVector.putChild(child.getName(), v);
    }
  }

  public SerializedField getMetadata() {
    int bufferSize = mapVector.getBufferSize();
    SerializedField.Builder b = SerializedField.newBuilder()
        .setNamePart(NamePart.newBuilder().setName(mapVector.getField().getName()))
        .setMajorType(Types.optional(MinorType.MAP))
        .setBufferLength(bufferSize)
        .setValueCount(mapVector.valueCount);

    b.addChild(TypeHelper.getMetadata(mapVector.bits));
    int expectedBufferSize = mapVector.bits.getBufferSize();
    for(ValueVector v : mapVector.getChildren()) {
      SerializedField metadata = TypeHelper.getMetadata(v);
      expectedBufferSize += metadata.getBufferLength();
      b.addChild(metadata);
    }
    Preconditions.checkState(expectedBufferSize == bufferSize, "Invalid buffer count: %s != %s", expectedBufferSize, bufferSize);
    return b.build();
  }
}
