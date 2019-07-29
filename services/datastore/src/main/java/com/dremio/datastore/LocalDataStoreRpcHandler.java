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
import java.util.List;
import java.util.Map;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.RemoteDataStoreProtobuf.ContainsRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.ContainsResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.DeleteRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.DeleteResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.FindRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.FindResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetCountsRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetCountsResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetStoreRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.GetStoreResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.PutRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.PutResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.SearchRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.SearchResponse;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Handle requests from remote kvstores.
 */
public class LocalDataStoreRpcHandler extends DefaultDataStoreRpcHandler {

  private final CoreStoreProviderRpcService coreStoreProvider;

  public LocalDataStoreRpcHandler(String hostName, CoreStoreProviderRpcService coreStoreProvider) {
    super(hostName);
    this.coreStoreProvider = coreStoreProvider;
  }

  @Override
  public GetResponse get(GetRequest request) {
    final CoreKVStore<Object, Object> store = coreStoreProvider.getStore(request.getStoreId());
    if (request.getKeysCount() == 1) {
      final KVStoreTuple<?> value = store.get(store.newKey().setSerializedBytes(request.getKeys(0).toByteArray()));
      if (!value.isNull()) {
        return GetResponse.newBuilder().addValues(ByteString.copyFrom(value.getSerializedBytes())).build();
      } else {
        return GetResponse.newBuilder().addValues(ByteString.EMPTY).build();
      }
    } else {
      final List<KVStoreTuple<Object>> keys = Lists.transform(request.getKeysList(), new Function<ByteString, KVStoreTuple<Object>>() {
        @Override
        public KVStoreTuple<Object> apply(ByteString input) {
          return store.newKey().setSerializedBytes(input.toByteArray());
        }
      });

      final GetResponse.Builder builder = GetResponse.newBuilder();
      for (KVStoreTuple<Object> value : store.get(keys)) {
        if (value == null || value.isNull()) {
          builder.addValues(ByteString.EMPTY);
        } else {
          builder.addValues(ByteString.copyFrom(value.getSerializedBytes()));
        }
      }
      return builder.build();
    }
  }

  @Override
  public FindResponse find(FindRequest request) {
    final CoreKVStore<Object, Object> store = coreStoreProvider.getStore(request.getStoreId());
    final Iterable<Map.Entry<KVStoreTuple<Object>, KVStoreTuple<Object>>> iterable;

    if (request.hasEnd() || request.hasStart()) {
      FindByRange<KVStoreTuple<Object>> findByRange = new FindByRange<KVStoreTuple<Object>>()
        .setStart(store.newKey().setSerializedBytes(request.getStart().toByteArray()), request.getIncludeStart())
        .setEnd(store.newKey().setSerializedBytes(request.getEnd().toByteArray()), request.getIncludeEnd());
      iterable = store.find(findByRange);
    } else { // find all
      iterable = store.find();
    }
    final FindResponse.Builder builder = FindResponse.newBuilder();

    for (Map.Entry<KVStoreTuple<Object>, KVStoreTuple<Object>> entry: iterable) {
      builder.addKeys(ByteString.copyFrom(entry.getKey().getSerializedBytes()));
      builder.addValues(ByteString.copyFrom(entry.getValue().getSerializedBytes()));
    }
    return builder.build();
  }

  @Override
  public ContainsResponse contains(ContainsRequest request) {
    final CoreKVStore<Object, Object> store = coreStoreProvider.getStore(request.getStoreId());
    final boolean contains = store.contains(store.newKey().setSerializedBytes(request.getKey().toByteArray()));
    return ContainsResponse.newBuilder().setContains(contains).build();
  }

  @Override
  public SearchResponse search(SearchRequest request) {
    final CoreIndexedStore<Object, Object> store = (CoreIndexedStore<Object, Object>)coreStoreProvider.getStore(request.getStoreId());
    final FindByCondition findByCondition = new FindByCondition();
    findByCondition.setLimit(request.getLimit());
    findByCondition.setOffset(request.getOffset());
    findByCondition.setPageSize(request.getPageSize());
    findByCondition.addSortings(request.getSortList());
    findByCondition.setCondition(request.getQuery());

    final SearchResponse.Builder builder = SearchResponse.newBuilder();
    for (Map.Entry<KVStoreTuple<Object>, KVStoreTuple<Object>> entry: store.find(findByCondition)) {
      builder.addKey(ByteString.copyFrom(entry.getKey().getSerializedBytes()));
      builder.addValue(ByteString.copyFrom(entry.getValue().getSerializedBytes()));
    }
    return builder.build();
  }

  @Override
  public GetCountsResponse getCounts(GetCountsRequest request) {
    final CoreIndexedStore<Object, Object> store = (CoreIndexedStore<Object, Object>)coreStoreProvider.getStore(request.getStoreId());
    final SearchQuery[] queries = new SearchQuery[request.getQueriesCount()];
    for (int i = 0; i < request.getQueriesCount(); ++i ) {
      queries[i] = request.getQueries(i);
    }
    return GetCountsResponse.newBuilder().addAllCounts(store.getCounts(queries)).build();
  }

  @Override
  public PutResponse put(PutRequest request) {
    final CoreKVStore<Object, Object> store = coreStoreProvider.getStore(request.getStoreId());
    final KVStoreTuple<Object> value = store.newValue().setSerializedBytes(request.getValue().toByteArray());
    try {
      store.put(store.newKey().setSerializedBytes(request.getKey().toByteArray()), value);
    } catch (ConcurrentModificationException cme) {
      return PutResponse.newBuilder().setConcurrentModificationError(cme.getMessage()).build();
    }
    return value.getTag() == null? PutResponse.getDefaultInstance() : PutResponse.newBuilder().setVersion(value.getTag()).build();
  }

  @Override
  public DeleteResponse delete(DeleteRequest request) {
    final CoreKVStore<Object, Object> store = coreStoreProvider.getStore(request.getStoreId());
    if (request.hasPreviousVersion()) {
      try {
        store.delete(store.newKey().setSerializedBytes(request.getKey().toByteArray()), request.getPreviousVersion());
      } catch (ConcurrentModificationException cme) {
        return DeleteResponse.newBuilder().setConcurrentModificationError(cme.getMessage()).build();
      }
    } else {
      store.delete(store.newKey().setSerializedBytes(request.getKey().toByteArray()));
    }
    return DeleteResponse.getDefaultInstance();
  }

  @Override
  public GetStoreResponse getStore(GetStoreRequest request) {
    StoreBuilderConfig config = new StoreBuilderConfig();
    config.setKeySerializerClassName(request.getKeySerializerClass());
    config.setValueSerializerClassName(request.getValueSerializerClass());
    config.setName(request.getName());
    // Protobuf doesn't store null strings
    if (request.hasVersionExtractorClass()) {
      config.setVersionExtractorClassName(request.getVersionExtractorClass());
    }
    if (request.hasDocumentConverterClass()) {
      config.setDocumentConverterClassName(request.getDocumentConverterClass());
    }
    String storeId = coreStoreProvider.getOrCreateStore(config);
    return GetStoreResponse.newBuilder().setStoreId(storeId).build();
  }
}
