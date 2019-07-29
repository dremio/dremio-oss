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
package com.dremio.datastore;

import java.util.UUID;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Test local mapdb kvstore.
 */
public class TestLocalKVStore extends AbstractTestKVStore {
  @ClassRule
  public static final TemporaryFolder tmpFolder = new TemporaryFolder();

  private CoreStoreProviderImpl coreStoreProvider;

  @Override
  public void initProvider() throws Exception {
    coreStoreProvider = new CoreStoreProviderImpl(tmpFolder.getRoot().toString(), true, false);
    coreStoreProvider.start();
  }

  @Override
  public void closeProvider() throws Exception {
    if (coreStoreProvider != null) {
      coreStoreProvider.close();
    }
    coreStoreProvider = null;
  }

  @Override
  Backend createBackEndForKVStore() {

    final String name = UUID.randomUUID().toString();
    final StringSerializer serde = StringSerializer.INSTANCE;
    final KVStore<byte[], byte[]> map = coreStoreProvider.getDB(name);
    return new Backend() {
      @Override
      public String get(String key) {
        final byte [] value = map.get(serde.convert(key));
        if (value != null) {
          return serde.revert(value);
        }
        return null;
      }

      @Override
      public void put(String key, String value) {
        map.put(serde.convert(key), serde.convert(value));
      }

      @Override
      public String getName() {
        return name;
      }
    };
  }

  @Override
  KVStore<String, String> createKVStore(Backend backend) {
    return new LocalKVStore<>(coreStoreProvider.<String, String>newStore()
      .name(backend.getName())
      .keySerializer(StringSerializer.class)
      .valueSerializer(StringSerializer.class)
      .build());
  }

}
