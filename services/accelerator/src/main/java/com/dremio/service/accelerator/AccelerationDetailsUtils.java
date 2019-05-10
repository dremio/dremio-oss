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
package com.dremio.service.accelerator;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.service.accelerator.proto.AccelerationDetails;

import io.protostuff.ByteString;

/**
 * Helper methods for serializing/deserializing {@link com.dremio.service.accelerator.proto.AccelerationDetails}
 */
public class AccelerationDetailsUtils {
  private static final ProtostuffSerializer<AccelerationDetails> SERIALIZER = new ProtostuffSerializer<>(AccelerationDetails.getSchema());

  public static byte[] serialize(AccelerationDetails details) {
    if (details == null) {
      return null;
    }
    return SERIALIZER.convert(details);
  }

  public static AccelerationDetails deserialize(ByteString bytes) {
    if (bytes == null) {
      return null;
    }
    return SERIALIZER.revert(bytes.toByteArray());
  }

  public static AccelerationDetails deserialize(com.google.protobuf.ByteString bytes) {
    if (bytes == null) {
      return null;
    }
    return SERIALIZER.revert(bytes.toByteArray());
  }
}
