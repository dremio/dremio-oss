/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * Request handler for incoming datastore rpc.
 */
public class DefaultDataStoreRpcHandler {

  private final String hostName;

  public DefaultDataStoreRpcHandler(String hostName) {
    this.hostName = hostName;
  }

  public GetResponse get(GetRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public GetCountsResponse getCounts(GetCountsRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public FindResponse find(FindRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public ContainsResponse contains(ContainsRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public SearchResponse search(SearchRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public PutResponse put(PutRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public DeleteResponse delete(DeleteRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public CheckAndPutResponse checkAndPut(CheckAndPutRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public CheckAndDeleteResponse checkAndDelete(CheckAndDeleteRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }

  public GetStoreResponse getStore(GetStoreRequest request) {
    throw new UnsupportedOperationException("Remote datastore operations are not supported on this host " + hostName);
  }
}
