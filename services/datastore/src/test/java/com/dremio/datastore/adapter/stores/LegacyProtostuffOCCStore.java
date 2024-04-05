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

import com.dremio.datastore.adapter.TestLegacyStoreCreationFunction;
import com.dremio.datastore.adapter.extractors.ProtostuffDummyObjVersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.proto.DummyId;
import com.dremio.datastore.proto.DummyObj;
import java.util.Arrays;
import java.util.List;

/** Tests using Protostuff keys and classes with ProtostuffDummyObjVersionExtractor. */
public class LegacyProtostuffOCCStore
    implements TestLegacyStoreCreationFunction<DummyId, DummyObj> {
  @Override
  public LegacyKVStore<DummyId, DummyObj> build(LegacyStoreBuildingFactory factory) {
    return factory
        .<DummyId, DummyObj>newStore()
        .name("legacy-protostuff-occ-store")
        .keyFormat(getKeyFormat())
        .valueFormat(Format.ofProtostuff(DummyObj.class))
        .versionExtractor(ProtostuffDummyObjVersionExtractor.class)
        .build();
  }

  @Override
  public Format<DummyId> getKeyFormat() {
    return Format.ofProtostuff(DummyId.class);
  }

  @Override
  public List<Class<?>> getKeyClasses() {
    return Arrays.asList(DummyId.class);
  }
}
