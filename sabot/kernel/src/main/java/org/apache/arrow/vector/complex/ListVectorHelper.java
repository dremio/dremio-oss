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


import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import org.apache.arrow.vector.LastSetter;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.ComplexTypeHelper;

import io.netty.buffer.ArrowBuf;

public class ListVectorHelper extends BaseRepeatedValueVectorHelper {
  private ListVector listVector;

  public ListVectorHelper(ListVector vector) {
    super(vector);
    this.listVector = vector;
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    final SerializedField offsetMetadata = metadata.getChild(0);
    TypeHelper.load(listVector.offsets, offsetMetadata, buffer);

    final int offsetLength = offsetMetadata.getBufferLength();
    final SerializedField bitMetadata = metadata.getChild(1);
    final int bitLength = bitMetadata.getBufferLength();
    TypeHelper.load(listVector.bits, bitMetadata, buffer.slice(offsetLength, bitLength));

    final SerializedField vectorMetadata = metadata.getChild(2);
    if (listVector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      listVector.addOrGetVector(FieldType.nullable(getArrowMinorType(vectorMetadata.getMajorType().getMinorType()).getType()));
    }

    final int vectorLength = vectorMetadata.getBufferLength();
    TypeHelper.load(listVector.vector, vectorMetadata, buffer.slice(offsetLength + bitLength, vectorLength));
    LastSetter.set(listVector, metadata.getValueCount());
  }

  public void materialize(Field field) {
    if (field.getChildren().size() == 0) {
      return;
    }
    Field innerField = field.getChildren().get(0);
    ValueVector innerVector = listVector.addOrGetVector(innerField.getFieldType()).getVector();
    ComplexTypeHelper.materialize(innerVector, innerField);
  }

  public SerializedField.Builder getMetadataBuilder() {
    return SerializedField.newBuilder()
            .setMajorType(MajorType.newBuilder().setMinorType(MinorType.LIST).setMode(DataMode.OPTIONAL).build())
            .setNamePart(NamePart.newBuilder().setName(listVector.getField().getName()))
            .setValueCount(listVector.getAccessor().getValueCount())
            .setBufferLength(listVector.getBufferSize())
            .addChild(TypeHelper.getMetadata(listVector.offsets))
            .addChild(TypeHelper.getMetadata(listVector.bits))
            .addChild(TypeHelper.getMetadata(listVector.vector));
  }
}
