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
package org.apache.arrow.vector;


import org.apache.arrow.vector.types.SerializedFieldHelper;

import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.SerializedField;

public class BaseValueVectorHelper {
  private BaseValueVector vector;

  public BaseValueVectorHelper(BaseValueVector vector) {
    this.vector = vector;
  }

  public SerializedField getMetadata() {
    return getMetadataBuilder().build();
  }

  public SerializedField.Builder getMetadataBuilder() {
//    return SerializedFieldHelper.getAsBuilder(vector.getField())
    return SerializedField.newBuilder().setNamePart(NamePart.newBuilder().setName(vector.name).build())
        .setValueCount(vector.getAccessor().getValueCount())
        .setBufferLength(vector.getBufferSize());
  }
}
