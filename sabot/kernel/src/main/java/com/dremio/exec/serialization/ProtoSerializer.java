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
package com.dremio.exec.serialization;

import com.dremio.common.utils.ProtobufUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;

/** Utilities to serialize/deserialize JSON encoded protobuf */
public final class ProtoSerializer {
  private ProtoSerializer() {}

  /**
   * Returns JacksonSerializer to serialize/deserialize protobuf
   *
   * @param clazz Type of protobuf object
   * @param maxStringLength Max allowed size of the String fields
   * @return JacksonSerializer to be used for serializing/deserializing JSON
   * @param <M> Type of protobuf object
   */
  public static <M extends Message> InstanceSerializer<M> of(Class<M> clazz, int maxStringLength) {
    ObjectMapper mapper = ProtobufUtils.newMapper();
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(maxStringLength).build());
    mapper = mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    return new JacksonSerializer<>(mapper, clazz);
  }
}
