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

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.proto.Dummy.DummyId;
import com.dremio.datastore.proto.Dummy.DummyObj;

/**
 * Tests using protobuf keys and values.
 */
public class LegacyProtobufStore implements LegacyStoreCreationFunction<LegacyKVStore<DummyId, DummyObj>> {
  @Override
  public LegacyKVStore<DummyId, DummyObj> build(LegacyStoreBuildingFactory factory) {
    return factory.<DummyId, DummyObj>newStore()
      .name("legacy-protobuf-store")
      .keyFormat(Format.ofProtobuf(DummyId.class))
      .valueFormat(Format.ofProtobuf(DummyObj.class))
      .build();
  }
}
