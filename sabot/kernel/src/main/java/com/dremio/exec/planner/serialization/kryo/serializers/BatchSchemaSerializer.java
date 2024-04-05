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
package com.dremio.exec.planner.serialization.kryo.serializers;

import com.dremio.exec.record.BatchSchema;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/** Serializer to serialize and deserialize BatchSchema objects. */
public class BatchSchemaSerializer extends Serializer<BatchSchema> {

  @Override
  public void write(Kryo kryo, Output output, BatchSchema schema) {
    kryo.writeObject(output, schema.serialize());
  }

  @Override
  public BatchSchema read(Kryo kryo, Input input, Class<BatchSchema> type) {
    final byte[] bytes = kryo.readObject(input, byte[].class);
    return BatchSchema.deserialize(bytes);
  }
}
