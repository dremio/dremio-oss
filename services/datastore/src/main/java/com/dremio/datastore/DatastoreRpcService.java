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

import org.apache.arrow.memory.BufferAllocator;

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
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.simple.AbstractReceiveHandler;
import com.dremio.services.fabric.simple.ProtocolBuilder;
import com.dremio.services.fabric.simple.SendEndpoint;
import com.dremio.services.fabric.simple.SendEndpointCreator;
import com.dremio.services.fabric.simple.SentResponseMessage;

import io.netty.buffer.ArrowBuf;

/**
 * Rpc Service. Registers protocol and creates endpoints to master node.
 */
public class DatastoreRpcService {

  private final NodeEndpoint master;

  private final SendEndpoint<GetRequest, GetResponse> getEndpoint;
  private final SendEndpoint<ContainsRequest, ContainsResponse> containsEndpoint;
  private final SendEndpoint<FindRequest, FindResponse> findEndpoint;
  private final SendEndpoint<GetCountsRequest, GetCountsResponse> getCountsEndpoint;
  private final SendEndpoint<SearchRequest, SearchResponse> searchEndpoint;

  private final SendEndpoint<PutRequest, PutResponse> putEndpoint;
  private final SendEndpoint<DeleteRequest, DeleteResponse> deleteEndpoint;
  private final SendEndpoint<CheckAndPutRequest, CheckAndPutResponse> checkAndPutEndpoint;
  private final SendEndpoint<CheckAndDeleteRequest, CheckAndDeleteResponse> checkAndDeleteEndpoint;
  private final SendEndpoint<GetStoreRequest, GetStoreResponse> getStoreEndpoint;

  public DatastoreRpcService(String masterHostName, int  masterPort,
                             FabricService fabricService, BufferAllocator allocator,
                             final DefaultDataStoreRpcHandler handler) throws RpcException {
    master = NodeEndpoint.newBuilder().setAddress(masterHostName).setFabricPort(masterPort).build();

    // Register endpoints for communicating with master
    final ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("datastore-rpc").protocolId(4).timeout(10*1000);
    int typeId = 1;

    final SendEndpointCreator<GetRequest, GetResponse> getEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<GetRequest, GetResponse>(GetRequest.getDefaultInstance(), GetResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetResponse> handle(GetRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.get(request));
        }
      });

    final SendEndpointCreator<ContainsRequest, ContainsResponse> containsEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<ContainsRequest, ContainsResponse>(ContainsRequest.getDefaultInstance(), ContainsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<ContainsResponse> handle(ContainsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.contains(request));
        }
      });

    final SendEndpointCreator<GetCountsRequest, GetCountsResponse> getCountsEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<GetCountsRequest, GetCountsResponse>(GetCountsRequest.getDefaultInstance(), GetCountsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetCountsResponse> handle(GetCountsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.getCounts(request));
        }
      });

    final SendEndpointCreator<FindRequest, FindResponse> findEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<FindRequest, FindResponse>(FindRequest.getDefaultInstance(), FindResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<FindResponse> handle(FindRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.find(request));
        }
      });
    final SendEndpointCreator<SearchRequest, SearchResponse> sendEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<SearchRequest, SearchResponse>(SearchRequest.getDefaultInstance(), SearchResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<SearchResponse> handle(SearchRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.search(request));
        }
      });

    final SendEndpointCreator<PutRequest, PutResponse> putEndpointCreator  = builder.register(typeId++,
      new AbstractReceiveHandler<PutRequest, PutResponse>(PutRequest.getDefaultInstance(), PutResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<PutResponse> handle(PutRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.put(request));
        }
      });

    final SendEndpointCreator<DeleteRequest, DeleteResponse>  deleteEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<DeleteRequest, DeleteResponse>(DeleteRequest.getDefaultInstance(), DeleteResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<DeleteResponse> handle(DeleteRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.delete(request));
        }
      });

    final SendEndpointCreator<CheckAndPutRequest, CheckAndPutResponse>  checkAndPutEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<CheckAndPutRequest, CheckAndPutResponse>(CheckAndPutRequest.getDefaultInstance(), CheckAndPutResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<CheckAndPutResponse> handle(CheckAndPutRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.checkAndPut(request));
        }
      });

    final SendEndpointCreator<CheckAndDeleteRequest, CheckAndDeleteResponse>  checkAndDeleteEndpointCreator  = builder.register(typeId++,
      new AbstractReceiveHandler<CheckAndDeleteRequest, CheckAndDeleteResponse>(CheckAndDeleteRequest.getDefaultInstance(), CheckAndDeleteResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<CheckAndDeleteResponse> handle(CheckAndDeleteRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.checkAndDelete(request));
        }
      });

    final SendEndpointCreator<GetStoreRequest, GetStoreResponse>  getStoreEndpointCreator  = builder.register(typeId++,
      new AbstractReceiveHandler<GetStoreRequest, GetStoreResponse>(GetStoreRequest.getDefaultInstance(), GetStoreResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetStoreResponse> handle(GetStoreRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.getStore(request));
        }
      });

    builder.register(fabricService);

    getEndpoint = getEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    getCountsEndpoint = getCountsEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    containsEndpoint = containsEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    findEndpoint = findEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    searchEndpoint = sendEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    putEndpoint = putEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    checkAndPutEndpoint = checkAndPutEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    deleteEndpoint = deleteEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    checkAndDeleteEndpoint = checkAndDeleteEndpointCreator.getEndpoint(master.getAddress(), masterPort);
    getStoreEndpoint = getStoreEndpointCreator.getEndpoint(master.getAddress(), masterPort);
  }

  public NodeEndpoint getMaster() {
    return master;
  }

  public SendEndpoint<GetRequest, GetResponse> getGetEndpoint() {
    return getEndpoint;
  }

  public SendEndpoint<ContainsRequest, ContainsResponse> getContainsEndpoint() {
    return containsEndpoint;
  }

  public SendEndpoint<FindRequest, FindResponse> getFindEndpoint() {
    return findEndpoint;
  }

  public SendEndpoint<GetCountsRequest, GetCountsResponse> getGetCountsEndpoint() {
    return getCountsEndpoint;
  }

  public SendEndpoint<SearchRequest, SearchResponse> getSearchEndpoint() {
    return searchEndpoint;
  }

  public SendEndpoint<PutRequest, PutResponse> getPutEndpoint() {
    return putEndpoint;
  }

  public SendEndpoint<DeleteRequest, DeleteResponse> getDeleteEndpoint() {
    return deleteEndpoint;
  }

  public SendEndpoint<CheckAndPutRequest, CheckAndPutResponse> getCheckAndPutEndpoint() {
    return checkAndPutEndpoint;
  }

  public SendEndpoint<CheckAndDeleteRequest, CheckAndDeleteResponse> getCheckAndDeleteEndpoint() {
    return checkAndDeleteEndpoint;
  }

  public SendEndpoint<GetStoreRequest, GetStoreResponse> getGetStoreEndpoint() {
    return getStoreEndpoint;
  }

}
