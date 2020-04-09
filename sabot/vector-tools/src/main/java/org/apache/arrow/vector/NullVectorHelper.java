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

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.proto.UserBitShared.SerializedField.Builder;

import io.netty.buffer.ArrowBuf;

public class NullVectorHelper implements ValueVectorHelper {

  private NullVector vector;

  public NullVectorHelper(NullVector vector) {
    this.vector = vector;
  }

  public SerializedField getMetadata() {
    return SerializedField.newBuilder()
        .setMajorType(com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.NULL))
        .setBufferLength(0)
        .setValueCount(0)
        .build();
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
  }

  @Override
  public void loadFromValidityAndDataBuffers(SerializedField metadata, ArrowBuf dataBuffer, ArrowBuf validityBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadData(SerializedField metadata, ArrowBuf buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Builder getMetadataBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void materialize(Field field) {
    throw new UnsupportedOperationException();
  }
}
