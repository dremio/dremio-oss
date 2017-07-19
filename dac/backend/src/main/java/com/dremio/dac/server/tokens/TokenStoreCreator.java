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
package com.dremio.dac.server.tokens;

import com.dremio.dac.proto.model.tokens.SessionState;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;

/**
 * Token store creator.
 */
public final class TokenStoreCreator implements StoreCreationFunction<KVStore<String, SessionState>> {

  @Override
  public KVStore<String, SessionState> build(final StoreBuildingFactory factory) {
    return factory.<String, SessionState>newStore()
      .name(TokenUtils.TOKENS_TABLE_NAME)
      .keySerializer(StringSerializer.INSTANCE.getClass())
      .valueSerializer(SessionStateSerializer.class)
      .build();
  }
}
