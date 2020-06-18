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

import com.dremio.dac.proto.model.tokens.SessionState;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Token store creator.
 */
public final class TokenStoreCreator implements LegacyKVStoreCreationFunction<String, SessionState> {

  @Override
  public LegacyKVStore<String, SessionState> build(final LegacyStoreBuildingFactory factory) {
    return factory.<String, SessionState>newStore()
      .name(TokenUtils.TOKENS_TABLE_NAME)
      .keyFormat(Format.ofString())
      .valueFormat(Format.ofProtostuff(SessionState.class))
      .build();
  }
}
