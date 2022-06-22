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
package com.dremio.datastore.adapter.stores;

import java.util.Arrays;
import java.util.List;

import com.dremio.datastore.adapter.TestLegacyStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Used to test that bytes[] as values with Strings as keys.
 */
public class LegacyRawByteStore implements TestLegacyStoreCreationFunction<String, byte[]> {
  @Override
  public LegacyKVStore<String, byte[]> build(LegacyStoreBuildingFactory factory) {
    return factory.<String, byte[]>newStore()
      .name("legacy-raw-byte-store")
      .keyFormat(getKeyFormat())
      .valueFormat(Format.ofBytes())
      .build();
  }

  @Override
  public Format<String> getKeyFormat() {
    return Format.ofString();
  }

  @Override
  public List<Class<?>> getKeyClasses() {
    return Arrays.asList(String.class);
  }
}
