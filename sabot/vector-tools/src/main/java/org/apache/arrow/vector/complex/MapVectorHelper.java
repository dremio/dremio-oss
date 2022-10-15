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

import com.dremio.common.types.TypeProtos;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;

public class MapVectorHelper extends ListVectorHelper {

  private final MapVector mapVector;

  public MapVectorHelper(MapVector vector) {
    super(vector);
    this.mapVector = vector;
  }

  @Override
  public UserBitShared.SerializedField.Builder getMetadataBuilder() {
    return UserBitShared.SerializedField.newBuilder()
      .setMajorType(TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.MAP).setMode(TypeProtos.DataMode.OPTIONAL).build())
      .setNamePart(UserBitShared.NamePart.newBuilder().setName(mapVector.getField().getName()))
      .setValueCount(mapVector.getValueCount())
      .setBufferLength(mapVector.getBufferSize())
      .addChild(buildOffsetMetadata())
      .addChild(buildValidityMetadata())
      .addChild(TypeHelper.getMetadata(mapVector.vector));
  }
}
