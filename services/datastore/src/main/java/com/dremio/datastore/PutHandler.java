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

import java.util.ConcurrentModificationException;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Takes in rpc PutRequests and translates and applies them to the appropriate core store.
 */
public class PutHandler {
  private final CoreStoreProviderRpcService coreStoreProvider;

  public PutHandler(CoreStoreProviderRpcService coreStoreProvider) {
    this.coreStoreProvider = coreStoreProvider;
  }

  public RemoteDataStoreProtobuf.PutResponse apply(RemoteDataStoreProtobuf.PutRequest request) {
    final RemoteDataStoreProtobuf.PutOptionInfo putOptionInfo = request.getOptionInfo();
    final CoreKVStore store = coreStoreProvider.getStore(request.getStoreId());
    final RemoteDataStoreProtobuf.PutResponse.Builder builder = RemoteDataStoreProtobuf.PutResponse.newBuilder();
    final KVStoreTuple<?> key = store.newKey().setSerializedBytes(request.getKey().toByteArray());
    final KVStoreTuple<?> value = store.newValue().setSerializedBytes(request.getValue().toByteArray());
    final Document<?, ?> result;

    try {
      if (putOptionInfo.hasType()) {
        final RemoteDataStoreProtobuf.PutOptionType optionType = putOptionInfo.getType();
        final KVStore.PutOption putOption;
        switch (optionType) {
          case CREATE:
            putOption = KVStore.PutOption.CREATE;
            break;
          case VERSION:
            Preconditions.checkArgument(putOptionInfo.hasParameter());
            putOption = new ImmutableVersionOption.Builder().setTag(putOptionInfo.getParameter()).build();
            break;
          default:
            throw new DatastoreException("Invalid put option specified " + optionType.toString());
        }

        result = store.put(key, value, putOption);
      } else {
        result = store.put(key, value);
      }
      if(!Strings.isNullOrEmpty(result.getTag())) {
        builder.setTag(result.getTag());
      }
    } catch (DatastoreException | ConcurrentModificationException e) {
      builder
        .setConcurrentModificationError(e instanceof ConcurrentModificationException)
        .setErrorMessage(e.getMessage());
    }

    return builder.build();
  }
}
