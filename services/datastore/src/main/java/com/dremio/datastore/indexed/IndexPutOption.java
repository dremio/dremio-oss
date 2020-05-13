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

package com.dremio.datastore.indexed;

import java.util.List;

import org.immutables.value.Value;

import com.dremio.datastore.RemoteDataStoreProtobuf;
import com.dremio.datastore.RemoteDataStoreProtobuf.PutOptionInfo;
import com.dremio.datastore.RemoteDataStoreProtobuf.PutOptionType;
import com.dremio.datastore.api.KVStore;

/**
 * A shim to transport explicit IndexField values originating from
 * {@link RemoteDataStoreProtobuf} "put" requests into
 * {@link KVStore#put(Object, Object, KVStore.PutOption...)} implementations.
 */
@Value.Immutable
public interface IndexPutOption extends KVStore.PutOption {
  // TODO (DX-22212): The KVStore API should not depend on classes from the remote datastore
  // RPC definition.
  List<RemoteDataStoreProtobuf.PutRequestIndexField> getIndexedFields();

  @Override
  default PutOptionInfo getPutOptionInfo() {
    return PutOptionInfo.newBuilder()
      .setType(PutOptionType.REMOTE_INDEX)
      .addAllIndexFields(getIndexedFields())
      .build();
  }
}
