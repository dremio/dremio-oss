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
import com.dremio.datastore.generator.ByteContainerStoreGenerator.ByteContainer;

/**
 * Used to test that bytes and wrapped values are stored/retrieved properly.
 * <p>
 * We use a byte container instead of raw byte[] so we may implement a custom
 * equals method. Normal array comparison is address-wise, not element-wise.
 */
public class LegacyByteContainerStore implements TestLegacyStoreCreationFunction<String, ByteContainer> {
  @Override
  public LegacyKVStore<String, ByteContainer> build(LegacyStoreBuildingFactory factory) {
    return factory.<String, ByteContainer>newStore()
      .name("legacy-byte-container-store")
      .keyFormat(getKeyFormat())
      .valueFormat(Format.wrapped(ByteContainer.class, ByteContainer::getBytes, ByteContainer::new, Format.ofBytes()))
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
