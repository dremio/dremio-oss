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

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

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
import com.google.protobuf.MessageLite;

/**
 * Rpc Service. Registers protocol and creates endpoints to master node.
 */
public class DatastoreRpcService {
  private static final int TYPE_GET = 1;
  private static final int TYPE_CONTAINS = 2;
  private static final int TYPE_GET_COUNTS = 3;
  private static final int TYPE_FIND = 4;
  private static final int TYPE_SEARCH = 5;
  private static final int TYPE_PUT = 6;
  private static final int TYPE_DELETE = 7;
  private static final int TYPE_GET_STORE = 10;

  private final Provider<NodeEndpoint> master;

  private final SendEndpointCreator<GetRequest, GetResponse> getEndpointCreator;
  private final SendEndpointCreator<ContainsRequest, ContainsResponse> containsEndpointCreator;
  private SendEndpointCreator<GetCountsRequest, GetCountsResponse> getCountsEndpointCreator;
  private SendEndpointCreator<FindRequest, FindResponse> findEndpointCreator;
  private SendEndpointCreator<SearchRequest, SearchResponse> searchEndpointCreator;
  private SendEndpointCreator<PutRequest, PutResponse> putEndpointCreator;
  private SendEndpointCreator<DeleteRequest, DeleteResponse> deleteEndpointCreator;
  private SendEndpointCreator<GetStoreRequest, GetStoreResponse> getStoreEndpointCreator;

  public DatastoreRpcService(Provider<NodeEndpoint> masterNode,
                             FabricService fabricService, BufferAllocator allocator,
                             final DefaultDataStoreRpcHandler handler,
                             final long timeoutInSecs) throws RpcException {
    master = masterNode;

    // Register endpoints for communicating with master
    final ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("datastore-rpc").protocolId(4).timeout(timeoutInSecs * 1000);

    getEndpointCreator = builder.register(TYPE_GET,
      new AbstractReceiveHandler<GetRequest, GetResponse>(GetRequest.getDefaultInstance(), GetResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetResponse> handle(GetRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.get(request));
        }
      });

    containsEndpointCreator = builder.register(TYPE_CONTAINS,
      new AbstractReceiveHandler<ContainsRequest, ContainsResponse>(ContainsRequest.getDefaultInstance(), ContainsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<ContainsResponse> handle(ContainsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.contains(request));
        }
      });

    getCountsEndpointCreator = builder.register(TYPE_GET_COUNTS,
      new AbstractReceiveHandler<GetCountsRequest, GetCountsResponse>(GetCountsRequest.getDefaultInstance(), GetCountsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetCountsResponse> handle(GetCountsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.getCounts(request));
        }
      });

    findEndpointCreator = builder.register(TYPE_FIND,
      new AbstractReceiveHandler<FindRequest, FindResponse>(FindRequest.getDefaultInstance(), FindResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<FindResponse> handle(FindRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.find(request));
        }
      });
    searchEndpointCreator = builder.register(TYPE_SEARCH,
      new AbstractReceiveHandler<SearchRequest, SearchResponse>(SearchRequest.getDefaultInstance(), SearchResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<SearchResponse> handle(SearchRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.search(request));
        }
      });

    putEndpointCreator = builder.register(TYPE_PUT,
      new AbstractReceiveHandler<PutRequest, PutResponse>(PutRequest.getDefaultInstance(), PutResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<PutResponse> handle(PutRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.put(request));
        }
      });

    deleteEndpointCreator = builder.register(TYPE_DELETE,
      new AbstractReceiveHandler<DeleteRequest, DeleteResponse>(DeleteRequest.getDefaultInstance(), DeleteResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<DeleteResponse> handle(DeleteRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.delete(request));
        }
      });

    getStoreEndpointCreator = builder.register(TYPE_GET_STORE,
      new AbstractReceiveHandler<GetStoreRequest, GetStoreResponse>(GetStoreRequest.getDefaultInstance(), GetStoreResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetStoreResponse> handle(GetStoreRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.getStore(request));
        }
      });

    builder.register(fabricService);
  }

  private <Req extends MessageLite, Resp extends MessageLite, T extends SendEndpointCreator<Req, Resp>>
  SendEndpoint<Req, Resp> newEndpoint(T creator) throws RpcException {
    NodeEndpoint masterNode = master.get();
    if (masterNode == null) {
      throw new RpcException("master node is down");
    }
    return creator.getEndpoint(masterNode.getAddress(), masterNode.getFabricPort());
  }

  public SendEndpoint<GetRequest, GetResponse> getGetEndpoint() throws RpcException {
    return newEndpoint(getEndpointCreator);
  }

  public SendEndpoint<ContainsRequest, ContainsResponse> getContainsEndpoint() throws RpcException {
    return newEndpoint(containsEndpointCreator);
  }

  public SendEndpoint<FindRequest, FindResponse> getFindEndpoint() throws RpcException {
    return newEndpoint(findEndpointCreator);
  }

  public SendEndpoint<GetCountsRequest, GetCountsResponse> getGetCountsEndpoint() throws RpcException {
    return newEndpoint(getCountsEndpointCreator);
  }

  public SendEndpoint<SearchRequest, SearchResponse> getSearchEndpoint() throws RpcException {
    return newEndpoint(searchEndpointCreator);
  }

  public SendEndpoint<PutRequest, PutResponse> getPutEndpoint() throws RpcException {
    return newEndpoint(putEndpointCreator);
  }

  public SendEndpoint<DeleteRequest, DeleteResponse> getDeleteEndpoint() throws RpcException {
    return newEndpoint(deleteEndpointCreator);
  }

  public SendEndpoint<GetStoreRequest, GetStoreResponse> getGetStoreEndpoint() throws RpcException {
    return newEndpoint(getStoreEndpointCreator);
  }

}
