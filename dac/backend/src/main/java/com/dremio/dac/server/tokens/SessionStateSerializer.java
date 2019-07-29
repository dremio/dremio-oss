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
package com.dremio.dac.server.tokens;

import java.io.IOException;

import com.dremio.dac.proto.model.tokens.SessionState;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;

/**
 * Serializer for session state associated with a token.
 */
public final class SessionStateSerializer extends Serializer<SessionState> {

  private final Serializer<SessionState> serializer =
    ProtostuffSerializer.of(SessionState.getSchema());

  @Override
  public byte[] convert(final SessionState v) {
    return serializer.convert(v);
  }

  @Override
  public SessionState revert(final byte[] v) {
    return serializer.revert(v);
  }

  @Override
  public String toJson(final SessionState v) throws IOException {
    return serializer.toJson(v);
  }

  @Override
  public SessionState fromJson(final String v) throws IOException {
    return serializer.fromJson(v);
  }
}
