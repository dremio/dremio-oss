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
 * StoreCreationFunction implementation to provide configurations for this test store.
 */
public class LegacyStringStore implements TestLegacyStoreCreationFunction<String, String> {
  @Override
  public LegacyKVStore<String, String> build(LegacyStoreBuildingFactory factory) {
    return factory.<String, String>newStore()
      .name("legacy-string-store")
      .keyFormat(getKeyFormat())
      .valueFormat(Format.ofString())
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
