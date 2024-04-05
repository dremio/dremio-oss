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
package com.dremio.datastore.stores;

import com.dremio.datastore.TestStoreCreationFunction;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.proto.Dummy;
import java.util.Arrays;
import java.util.List;

/** Test protobuf store that creates a DummyId key and DummyObj value KVStore. */
public class ProtobufStore implements TestStoreCreationFunction<Dummy.DummyId, Dummy.DummyObj> {
  @Override
  public KVStore<Dummy.DummyId, Dummy.DummyObj> build(StoreBuildingFactory factory) {
    return factory
        .<Dummy.DummyId, Dummy.DummyObj>newStore()
        .name("protobuf-store")
        .keyFormat(getKeyFormat())
        .valueFormat(Format.ofProtobuf(Dummy.DummyObj.class))
        .build();
  }

  @Override
  public Format<Dummy.DummyId> getKeyFormat() {
    return Format.ofProtobuf(Dummy.DummyId.class);
  }

  @Override
  public List<Class<?>> getKeyClasses() {
    return Arrays.asList(Dummy.DummyId.class);
  }
}
