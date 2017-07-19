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

import org.apache.arrow.vector.BaseValueVectorHelper;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;

import io.netty.buffer.ArrowBuf;

public class BaseRepeatedValueVectorHelper extends BaseValueVectorHelper {

  private BaseRepeatedValueVector vector;

  public BaseRepeatedValueVectorHelper(BaseRepeatedValueVector vector) {
    super(vector);
    this.vector = vector;
  }

  public UserBitShared.SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
        .addChild(TypeHelper.getMetadata(vector.offsets))
        .addChild(TypeHelper.getMetadata(vector.vector));
  }

  public void load(UserBitShared.SerializedField metadata, ArrowBuf buffer) {
    final UserBitShared.SerializedField offsetMetadata = metadata.getChild(0);
    TypeHelper.load(vector.offsets, offsetMetadata, buffer);

    final UserBitShared.SerializedField vectorMetadata = metadata.getChild(1);
    if (vector.getDataVector() == BaseRepeatedValueVector.DEFAULT_DATA_VECTOR) {
      vector.addOrGetVector(FieldType.nullable(getArrowMinorType(metadata.getMajorType().getMinorType()).getType()));
    }

    final int offsetLength = offsetMetadata.getBufferLength();
    final int vectorLength = vectorMetadata.getBufferLength();
    TypeHelper.load(vector.vector, vectorMetadata, buffer.slice(offsetLength, vectorLength));
  }
}
