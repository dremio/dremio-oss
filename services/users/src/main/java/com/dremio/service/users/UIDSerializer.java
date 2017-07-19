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
package com.dremio.service.users;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import com.dremio.datastore.Serializer;
import com.dremio.service.users.proto.UID;

/**
 * A Serializer implementation for {@link UID}
 */
public final class UIDSerializer extends Serializer<UID> {

  public static final UIDSerializer INSTANCE = new UIDSerializer();

  public UIDSerializer() {}

  @Override
  public String toJson(UID v) {
    return v.getId();
  }

  @Override
  public UID fromJson(String v) throws IOException {
    return new UID(v);
  }

  @Override
  public byte[] convert(UID v) {
    return v.getId().getBytes(UTF_8);
  }

  @Override
  public UID revert(byte[] bytes) {
    return new UID(new String(bytes, UTF_8));
  }
}
