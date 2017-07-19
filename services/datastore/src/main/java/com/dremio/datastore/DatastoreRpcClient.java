/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.RemoteDataStoreProtobuf.CheckAndDeleteRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.CheckAndDeleteResponse;
import com.dremio.datastore.RemoteDataStoreProtobuf.CheckAndPutRequest;
import com.dremio.datastore.RemoteDataStoreProtobuf.CheckAndPutResponse;
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
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.simple.ReceivedResponseMessage;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * Raw interfaces over wire, does not use any types.
 * Upper levels should use tuple to avoid paying cost of double serialization..
 */
public class DatastoreRpcClient {

  private final DatastoreRpcService rpcService;

  public DatastoreRpcClient(DatastoreRpcService rpcService) {
    this.rpcService = rpcService;
  }

  public String buildStore(StoreBuilderConfig config) {
    GetStoreRequest.Builder builder = GetStoreRequest.newBuilder()
      .setKeySerializerClass(config.getKeySerializerClassName())
      .setValueSerializerClass(config.getValueSerializerClassName())
      .setName(config.getName());
    if (config.getVersionExtractorClassName() != null && !config.getVersionExtractorClassName().isEmpty()) {
      builder.setVersionExtractorClass(config.getVersionExtractorClassName());
    }
    if (config.getDocumentConverterClassName() != null && !config.getDocumentConverterClassName().isEmpty()) {
      builder.setDocumentConverterClass(config.getDocumentConverterClassName());
    }
    try {
      ReceivedResponseMessage<GetStoreResponse> response = rpcService.getGetStoreEndpoint().send(builder.build());
      return response.getBody().getStoreId();
    } catch (RpcException e) {
      throw new DatastoreFatalException("Failed to create datastore for config " + config.toString(), e);
    }
  }

  public ByteString get(String storeId, ByteString key) throws RpcException {
    final GetRequest.Builder builder = GetRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.addKeys(key);
    ReceivedResponseMessage<GetResponse> response = rpcService.getGetEndpoint().send(builder.build());
    if (response.getBody().getValuesCount() > 0) {
      return response.getBody().getValues(0);
    }
    return null;
  }

  public List<ByteString> get(String storeId, List<ByteString> keys) throws RpcException {
    final GetRequest.Builder builder = GetRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.addAllKeys(keys);
    ReceivedResponseMessage<GetResponse> response = rpcService.getGetEndpoint().send(builder.build());
    return response.getBody().getValuesList();
  }

  public boolean contains(String storeId, ByteString key) throws RpcException {
    final ContainsRequest.Builder builder = ContainsRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    ReceivedResponseMessage<ContainsResponse> response = rpcService.getContainsEndpoint().send(builder.build());
    return response.getBody().getContains();
  }

  public Iterable<Map.Entry<ByteString, ByteString>>find(String storeId, FindByRange<ByteString> findByRange) throws RpcException {
    final FindRequest.Builder builder = FindRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setStart(findByRange.getStart());
    builder.setEnd(findByRange.getEnd());
    builder.setIncludeStart(findByRange.isStartInclusive());
    builder.setIncludeEnd(findByRange.isEndInclusive());
    ReceivedResponseMessage<FindResponse> response = rpcService.getFindEndpoint().send(builder.build());
    return toIterator(response.getBody().getKeysList(), response.getBody().getValuesList());
  }

  public Iterable<Map.Entry<ByteString, ByteString>>find(String storeId) throws RpcException {
    final FindRequest.Builder builder = FindRequest.newBuilder();
    builder.setStoreId(storeId);
    ReceivedResponseMessage<FindResponse> response = rpcService.getFindEndpoint().send(builder.build());
    return toIterator(response.getBody().getKeysList(), response.getBody().getValuesList());
  }

  public Long put(String storeId, ByteString key, ByteString value) throws RpcException {
    final PutRequest.Builder builder = PutRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    builder.setValue(value);
    ReceivedResponseMessage<PutResponse> response = rpcService.getPutEndpoint().send(builder.build());
    if (response.getBody().hasConcurrentModificationError()) {
      throw new ConcurrentModificationException(response.getBody().getConcurrentModificationError());
    }
    if (response.getBody().hasVersion()) {
      return response.getBody().getVersion();
    }
    return null;
  }

  public Pair<Boolean, Long> checkAndPut(String storeId, ByteString key, ByteString oldValue, ByteString newValue) throws RpcException {
    final CheckAndPutRequest.Builder builder = CheckAndPutRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    if (oldValue != null) {
      builder.setOldValue(oldValue);
    }
    builder.setNewValue(newValue);
    ReceivedResponseMessage<CheckAndPutResponse> response  = rpcService.getCheckAndPutEndpoint().send(builder.build());
    return new ImmutablePair<>(response.getBody().getInserted(), response.getBody().hasVersion()? response.getBody().getVersion() : null);
  }

  public void delete(String storeId, ByteString key) throws RpcException {
    final DeleteRequest.Builder builder = DeleteRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    rpcService.getDeleteEndpoint().send(builder.build());
  }

  public void delete(String storeId, ByteString key, long previousVersion) throws RpcException {
    final DeleteRequest.Builder builder = DeleteRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    builder.setPreviousVersion(previousVersion);
    ReceivedResponseMessage<DeleteResponse> response = rpcService.getDeleteEndpoint().send(builder.build());
    if (response.getBody().hasConcurrentModificationError()) {
      throw new ConcurrentModificationException(response.getBody().getConcurrentModificationError());
    }
  }

  public boolean checkAndDelete(String storeId, ByteString key, ByteString value) throws RpcException {
    final CheckAndDeleteRequest.Builder builder = CheckAndDeleteRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    builder.setValue(value);
    ReceivedResponseMessage<CheckAndDeleteResponse> response  = rpcService.getCheckAndDeleteEndpoint().send(builder.build());
    return response.getBody().getDeleted();
  }

  public Iterable<Map.Entry<ByteString, ByteString>>find(String storeId, FindByCondition findByCondition) throws IOException {
    final SearchRequest.Builder builder = SearchRequest.newBuilder();
    builder.setStoreId(storeId);
    if (!findByCondition.getSort().isEmpty()) {
      builder.addAllSort(findByCondition.getSort());
    }
    builder.setLimit(findByCondition.getLimit());
    builder.setOffset(findByCondition.getOffset());
    builder.setPageSize(findByCondition.getPageSize());
    builder.setQuery(findByCondition.getCondition());
    ReceivedResponseMessage<SearchResponse> response  = rpcService.getSearchEndpoint().send(builder.build());
    return toIterator(response.getBody().getKeyList(), response.getBody().getValueList());
  }

  public List<Integer> getCounts(String storeId, SearchQuery... conditions) throws IOException {
    final GetCountsRequest.Builder builder = GetCountsRequest.newBuilder();
    builder.setStoreId(storeId);
    for (SearchQuery condition: conditions) {
      builder.addQueries(condition);
    }
    ReceivedResponseMessage<GetCountsResponse> response  = rpcService.getGetCountsEndpoint().send(builder.build());
    return response.getBody().getCountsList();
  }

  private Iterable<Map.Entry<ByteString, ByteString>> toIterator(List<ByteString> keys, List<ByteString> values) {
    Preconditions.checkState(keys.size() == values.size());
    final List<Map.Entry<ByteString, ByteString>> entries = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); ++i) {
      entries.add(new AbstractMap.SimpleEntry<>(keys.get(i), values.get(i)));
    }
    return entries;
  }
}
