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

import com.dremio.datastore.RemoteDataStoreProtobuf.ContainsRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.ContainsResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.DeleteRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.DeleteResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.DocumentResponse;
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
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.ImmutableMaxResultsOption;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

/** Handle requests from remote kvstores. */
public class LocalDataStoreRpcHandler extends DefaultDataStoreRpcHandler {

  private final CoreStoreProviderRpcService coreStoreProvider;
  private final PutHandler putHandler;

  public LocalDataStoreRpcHandler(String hostName, CoreStoreProviderRpcService coreStoreProvider) {
    super(hostName);
    this.coreStoreProvider = coreStoreProvider;
    this.putHandler = new PutHandler(coreStoreProvider);
  }

  @Override
  public GetResponse get(GetRequest request) {
    final CoreKVStore store = coreStoreProvider.getStore(request.getStoreId());
    final GetResponse.Builder builder = GetResponse.newBuilder();
    if (request.getKeysCount() == 1) {
      final Document<KVStoreTuple<?>, KVStoreTuple<?>> result =
          store.get(store.newKey().setSerializedBytes(request.getKeys(0).toByteArray()));
      return builder.addDocuments(getDocumentResponse(result)).build();
    } else {
      final List<KVStoreTuple<?>> keys =
          Lists.transform(
              request.getKeysList(),
              new Function<ByteString, KVStoreTuple<?>>() {
                @Override
                public KVStoreTuple<?> apply(ByteString input) {
                  return store.newKey().setSerializedBytes(input.toByteArray());
                }
              });
      final Iterable<Document<KVStoreTuple<?>, KVStoreTuple<?>>> results = store.get(keys);
      for (Document<KVStoreTuple<?>, KVStoreTuple<?>> result : results) {
        builder.addDocuments(getDocumentResponse(result));
      }
      return builder.build();
    }
  }

  @Override
  public FindResponse find(FindRequest request) {
    final CoreKVStore store = coreStoreProvider.getStore(request.getStoreId());
    final Iterable<Document<KVStoreTuple<?>, KVStoreTuple<?>>> results;

    List<KVStore.FindOption> optionsList = new ArrayList<>();
    if (request.getMaxResults() < Integer.MAX_VALUE) {
      optionsList.add(
          new ImmutableMaxResultsOption.Builder().setMaxResults(request.getMaxResults()).build());
    }
    KVStore.FindOption[] options = optionsList.toArray(new KVStore.FindOption[0]);

    if (request.hasEnd() || request.hasStart()) {
      final ImmutableFindByRange.Builder rangeBuilder =
          new ImmutableFindByRange.Builder<KVStoreTuple<?>>();
      if (request.hasStart()) {
        rangeBuilder
            .setIsStartInclusive(request.getIncludeStart())
            .setStart(store.newKey().setSerializedBytes(request.getStart().toByteArray()));
      }
      if (request.hasEnd()) {
        rangeBuilder
            .setIsEndInclusive(request.getIncludeEnd())
            .setEnd(store.newKey().setSerializedBytes(request.getEnd().toByteArray()));
      }

      results = store.find(rangeBuilder.build(), options);
    } else { // find all
      results = store.find(options);
    }
    final FindResponse.Builder findResponseBuilder = FindResponse.newBuilder();

    for (Document<KVStoreTuple<?>, KVStoreTuple<?>> result : results) {
      findResponseBuilder.addDocuments(getDocumentResponse(result));
    }
    return findResponseBuilder.build();
  }

  @Override
  public ContainsResponse contains(ContainsRequest request) {
    final CoreKVStore store = coreStoreProvider.getStore(request.getStoreId());
    final boolean contains =
        store.contains(store.newKey().setSerializedBytes(request.getKey().toByteArray()));
    return ContainsResponse.newBuilder().setContains(contains).build();
  }

  @Override
  public SearchResponse search(SearchRequest request) {
    final CoreIndexedStore store =
        (CoreIndexedStore<Object, Object>) coreStoreProvider.getStore(request.getStoreId());
    final FindByCondition findByCondition =
        new ImmutableFindByCondition.Builder()
            .setLimit(request.getLimit())
            .setOffset(request.getOffset())
            .setPageSize(request.getPageSize())
            .setSort(request.getSortList())
            .setCondition(request.getQuery())
            .build();

    final SearchResponse.Builder searchResponseBuilder = SearchResponse.newBuilder();
    final Iterable<Document<KVStoreTuple<?>, KVStoreTuple<?>>> results =
        store.find(findByCondition);
    for (Document<KVStoreTuple<?>, KVStoreTuple<?>> result : results) {
      searchResponseBuilder.addDocuments(getDocumentResponse(result));
    }
    return searchResponseBuilder.build();
  }

  @Override
  public GetCountsResponse getCounts(GetCountsRequest request) {
    final CoreIndexedStore store =
        (CoreIndexedStore<Object, Object>) coreStoreProvider.getStore(request.getStoreId());
    final SearchQuery[] queries = new SearchQuery[request.getQueriesCount()];
    for (int i = 0; i < request.getQueriesCount(); ++i) {
      queries[i] = request.getQueries(i);
    }
    return GetCountsResponse.newBuilder().addAllCounts(store.getCounts(queries)).build();
  }

  @Override
  public PutResponse put(PutRequest request) {
    return putHandler.apply(request, false);
  }

  @Override
  public DeleteResponse delete(DeleteRequest request) {
    final CoreKVStore store = coreStoreProvider.getStore(request.getStoreId());
    if (request.hasTag()) {
      try {
        store.delete(
            store.newKey().setSerializedBytes(request.getKey().toByteArray()),
            new ImmutableVersionOption.Builder().setTag(request.getTag()).build());
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
    String storeId = coreStoreProvider.getStoreID(request.getName());
    return GetStoreResponse.newBuilder().setStoreId(storeId).build();
  }

  private DocumentResponse getDocumentResponse(Document<KVStoreTuple<?>, KVStoreTuple<?>> result) {
    if (result == null) {
      return DocumentResponse.getDefaultInstance();
    }
    final DocumentResponse.Builder documentResponseBuilder = DocumentResponse.newBuilder();
    documentResponseBuilder.setKey(ByteString.copyFrom(result.getKey().getSerializedBytes()));
    documentResponseBuilder.setValue(ByteString.copyFrom(result.getValue().getSerializedBytes()));
    final String tag = result.getTag();
    if (!Strings.isNullOrEmpty(tag)) {
      documentResponseBuilder.setTag(tag);
    }
    return documentResponseBuilder.build();
  }
}
