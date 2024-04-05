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

import static com.dremio.datastore.api.KVStoreProvider.getConstructor;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.dremio.datastore.api.KVStoreProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestKVStoreProviderConstructor {
  @ParameterizedTest
  @ValueSource(
      classes = {
        LocalKVStoreProvider.class,
        NoopKVStoreProvider.class,
        RemoteKVStoreProvider.class
      })
  void testHasTheExpectedConstructor(Class<? extends KVStoreProvider> cls) {
    assertThatNoException().isThrownBy(() -> getConstructor(cls));
  }
}
