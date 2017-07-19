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
package com.dremio.service.namespace;

import java.io.IOException;

import com.dremio.datastore.Serializer;
import com.dremio.datastore.StringSerializer;
import com.dremio.service.namespace.proto.EntityId;

/**
 * A serializer for  {@link EntityId}
 */
public class EntityIdSerializer extends Serializer<EntityId> {
  @Override
  public String toJson(EntityId v) throws IOException {
    return StringSerializer.INSTANCE.toJson(v.getId());
  }

  @Override
  public EntityId fromJson(String v) throws IOException {
    return new EntityId(StringSerializer.INSTANCE.fromJson(v));
  }

  @Override
  public byte[] convert(EntityId v) {
    return StringSerializer.INSTANCE.convert(v.getId());
  }

  @Override
  public EntityId revert(byte[] v) {
    return new EntityId(StringSerializer.INSTANCE.revert(v));
  }

}
