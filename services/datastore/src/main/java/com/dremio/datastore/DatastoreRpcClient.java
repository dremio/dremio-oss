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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;

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
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.KVStore;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.simple.ReceivedResponseMessage;
import com.google.common.base.Strings;
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

  /**
   * Retrieves storeId.
   *
   * @param name the name of the key-value store.
   * @return the storeId.
   */
  public String getStoreId(String name) {
    GetStoreRequest.Builder builder = GetStoreRequest.newBuilder()
      .setName(name);
    try {
      ReceivedResponseMessage<GetStoreResponse> response = rpcService.getGetStoreEndpoint().send(builder.build());
      return response.getBody().getStoreId();
    } catch (RpcException e) {
      throw new DatastoreFatalException("Failed to find datastore " + name, e);
    }
  }

  /**
   * Get method to retrieve a key-value store entry.
   *
   * @param storeId the storeId.
   * @param key the key of the key-value store entry to retrieve.
   * @return the document representation of the key-value store entry retrieved with the provided key. Can be {@code null}
   *         if no entry with the provided key is found.
   * @throws RpcException when RPC related exceptions are encountered.
   */
  public Document<ByteString, ByteString> get(String storeId, ByteString key) throws RpcException {
    final GetRequest.Builder builder = GetRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.addKeys(key);
    ReceivedResponseMessage<GetResponse> response = rpcService.getGetEndpoint().send(builder.build());
    if (response.getBody().getDocumentsCount() > 0) {
      DocumentResponse document = response.getBody().getDocuments(0);
      if (!document.equals(DocumentResponse.getDefaultInstance())) {
        return createImmutableDocumentFromKeyValueTag(document.getKey(), document.getValue(), document.getTag());
      }
    }
    return null;
  }

  /**
   * Get method to retrieve a list of key-value store entries.
   *
   * @param storeId the storeId.
   * @param keys the list of keys which entries are to be retrieved from the store.
   * @return a list of documents representing entries retrieved from the store. A document in the list can be {@code null}
   *         if no corresponding value is found.
   * @throws RpcException when RPC related exceptions are encountered.
   */
  public List<Document<ByteString, ByteString>> get(String storeId, List<ByteString> keys) throws RpcException {
    final GetRequest.Builder builder = GetRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.addAllKeys(keys);
    ReceivedResponseMessage<GetResponse> response = rpcService.getGetEndpoint().send(builder.build());
    return createListOfImmutableDocuments(response.getBody().getDocumentsList());
  }

  /**
   * Contains method to determine whether the store has an entry of the provided key.
   *
   * @param storeId the storeId.
   * @param key the key of the entry to lookup.
   * @return true if entry exists in the store, false otherwise.
   * @throws RpcException when RPC related errors are encountered.
   */
  public boolean contains(String storeId, ByteString key) throws RpcException {
    final ContainsRequest.Builder builder = ContainsRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);
    ReceivedResponseMessage<ContainsResponse> response = rpcService.getContainsEndpoint().send(builder.build());
    return response.getBody().getContains();
  }

  /**
   * Find method to retrieve documents satisfying provided range search criteria.
   *
   * @param request FindRequest to be sent.
   * @return the documents satisfying the search criteria, or {@code null} if no entries in the store satisfies the criteria.
   * @throws RpcException when RPC related errors are encountered.
   */
  public Iterable<Document<ByteString, ByteString>> find(FindRequest request) throws RpcException {
    ReceivedResponseMessage<FindResponse> response = rpcService.getFindEndpoint().send(request);
    return createListOfImmutableDocuments(response.getBody().getDocumentsList());
  }

  /**
   * Find method to retrieve all entries from the store.
   *
   * @param storeId the storeId.
   * @return all the documents from the store, or {@code null} if no entries exist in the store.
   * @throws RpcException when RPC related errors are encountered.
   */
  public Iterable<Document<ByteString, ByteString>> find(String storeId) throws RpcException {
    final FindRequest.Builder builder = FindRequest.newBuilder();
    builder.setStoreId(storeId);
    ReceivedResponseMessage<FindResponse> response = rpcService.getFindEndpoint().send(builder.build());
    return createListOfImmutableDocuments(response.getBody().getDocumentsList());
  }

  /**
   * Put method helper to delegate PutRequest.
   */
  private String put(PutRequest request) throws RpcException {
    final ReceivedResponseMessage<PutResponse> response = rpcService.getPutEndpoint().send(request);
    if (response.getBody().hasErrorMessage()) {
      if (response.getBody().getConcurrentModificationError()) {
        throw new ConcurrentModificationException(response.getBody().getErrorMessage());
      } else {
        throw new RpcException(response.getBody().getErrorMessage());
      }
    }

    if (response.getBody().hasTag()) {
      return response.getBody().getTag();
    }
    return null;
  }

  /**
   * Put method to store provided key value entry to the store.
   *
   * @param storeId the store ID.
   * @param key the key of the entry to be stored.
   * @param value the value of the key-value store entry to put.
   * @return the new tag of the key-value store entry.
   * @throws RpcException when RPC related errors are encountered.
   * @throws ConcurrentModificationException when PutResponse received has ConcurrentModificationError.
   */
  public String put(String storeId, ByteString key, ByteString value) throws RpcException {
    return put(PutRequest.newBuilder()
      .setStoreId(storeId)
      .setKey(key)
      .setValue(value)
      .build());
  }

  /**
   * Put method to store provided key value entry to the store, provided with options.
   *
   * @param storeId the storeId.
   * @param key the key of the entry to be stored.
   * @param value the value of the entry to be stored.
   * @param option the extra put options.
   * @return the new tag of the key-value store entry.
   * @throws RpcException
   */
  public String put(String storeId, ByteString key, ByteString value, KVStore.PutOption option) throws RpcException {

    final RemoteDataStoreProtobuf.PutOptionInfo optionInfo = option.getPutOptionInfo();

    return put(PutRequest.newBuilder()
      .setStoreId(storeId)
      .setKey(key)
      .setValue(value)
      .setOptionInfo(optionInfo)
      .build());
  }

  /**
   * Delete method to removed key-value store entry corresponding to the provided key from the store. Tag can be
   * {@code null} if no validation is required.
   *
   * @param storeId the store ID.
   * @param key the key of the key-value store entry to remove.
   * @param tag the tag of the key-value store entry to remove. Can be {@code null} if no validation is required.
   * @throws RpcException when RPC related errors are encountered.
   * @throws ConcurrentModificationException when DeleteResponse received has ConcurrentModificationError.
   */
  public void delete(String storeId, ByteString key, String tag) throws RpcException {
    final DeleteRequest.Builder builder = DeleteRequest.newBuilder();
    builder.setStoreId(storeId);
    builder.setKey(key);

    if (!Strings.isNullOrEmpty(tag)) {
      builder.setTag(tag);
    }

    ReceivedResponseMessage<DeleteResponse> response = rpcService.getDeleteEndpoint().send(builder.build());
    if (response.getBody().hasConcurrentModificationError()) {
      throw new ConcurrentModificationException(response.getBody().getConcurrentModificationError());
    }
  }

  /**
   * Find method to retrieve documents satisfying provided search conditions.
   *
   * @param storeId the storeId.
   * @param findByCondition the search condition.
   * @return the documents satisfying the search condition, or {@code null} if no entries in the store satisfies the condition.
   * @throws RpcException when RPC related errors are encountered.
   */
  public Iterable<Document<ByteString, ByteString>> find(String storeId, FindByCondition findByCondition) throws IOException {
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
    return createListOfImmutableDocuments(response.getBody().getDocumentsList());
  }

  /**
   * Counts the number of entries that satisfy the provided conditions.
   *
   * @param storeId the storeId.
   * @param conditions a list of conditions.
   * @return the number of entries that satisfy the provided conditions.
   * @throws IOException
   */
  public List<Integer> getCounts(String storeId, SearchQuery... conditions) throws IOException {
    final GetCountsRequest.Builder builder = GetCountsRequest.newBuilder();
    builder.setStoreId(storeId);
    for (SearchQuery condition: conditions) {
      builder.addQueries(condition);
    }
    ReceivedResponseMessage<GetCountsResponse> response  = rpcService.getGetCountsEndpoint().send(builder.build());
    return response.getBody().getCountsList();
  }

  private Document<ByteString, ByteString> createImmutableDocumentFromKeyValueTag(ByteString key, ByteString value, String tag) {
    ImmutableDocument.Builder<ByteString, ByteString> builder = new ImmutableDocument.Builder();
    builder.setKey(key);
    builder.setValue(value);
    if (!Strings.isNullOrEmpty(tag)) {
      builder.setTag(tag);
    }
    return builder.build();
  }

  /*
   * Converts a list of DocumentResponse into a list of ImmutableDocument.
   */
  private List<Document<ByteString, ByteString>> createListOfImmutableDocuments(List<DocumentResponse> documents) {
    ArrayList<Document<ByteString, ByteString>> list = new ArrayList<>();
    documents.forEach(document ->
      list.add((document.equals(DocumentResponse.getDefaultInstance())) ? null
          : createImmutableDocumentFromKeyValueTag(document.getKey(), document.getValue(), document.getTag())
      ));
    return Collections.unmodifiableList(list);
  }
}
