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

import javax.inject.Provider;

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
import com.google.protobuf.MessageLite;

import io.netty.buffer.ArrowBuf;

/**
 * Rpc Service. Registers protocol and creates endpoints to master node.
 */
public class DatastoreRpcService {

  private final Provider<NodeEndpoint> master;

  private final SendEndpointCreator<GetRequest, GetResponse> getEndpointCreator;
  private final SendEndpointCreator<ContainsRequest, ContainsResponse> containsEndpointCreator;
  private SendEndpointCreator<GetCountsRequest, GetCountsResponse> getCountsEndpointCreator;
  private SendEndpointCreator<FindRequest, FindResponse> findEndpointCreator;
  private SendEndpointCreator<SearchRequest, SearchResponse> searchEndpointCreator;
  private SendEndpointCreator<PutRequest, PutResponse> putEndpointCreator;
  private SendEndpointCreator<DeleteRequest, DeleteResponse> deleteEndpointCreator;
  private SendEndpointCreator<CheckAndPutRequest, CheckAndPutResponse> checkAndPutEndpointCreator;
  private SendEndpointCreator<CheckAndDeleteRequest, CheckAndDeleteResponse> checkAndDeleteEndpointCreator;
  private SendEndpointCreator<GetStoreRequest, GetStoreResponse> getStoreEndpointCreator;

  public DatastoreRpcService(Provider<NodeEndpoint> masterNode,
                             FabricService fabricService, BufferAllocator allocator,
                             final DefaultDataStoreRpcHandler handler) throws RpcException {
    master = masterNode;

    // Register endpoints for communicating with master
    final ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("datastore-rpc").protocolId(4).timeout(10*1000);
    int typeId = 1;

    getEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<GetRequest, GetResponse>(GetRequest.getDefaultInstance(), GetResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetResponse> handle(GetRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.get(request));
        }
      });

    containsEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<ContainsRequest, ContainsResponse>(ContainsRequest.getDefaultInstance(), ContainsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<ContainsResponse> handle(ContainsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.contains(request));
        }
      });

    getCountsEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<GetCountsRequest, GetCountsResponse>(GetCountsRequest.getDefaultInstance(), GetCountsResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<GetCountsResponse> handle(GetCountsRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.getCounts(request));
        }
      });

    findEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<FindRequest, FindResponse>(FindRequest.getDefaultInstance(), FindResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<FindResponse> handle(FindRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.find(request));
        }
      });
    searchEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<SearchRequest, SearchResponse>(SearchRequest.getDefaultInstance(), SearchResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<SearchResponse> handle(SearchRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.search(request));
        }
      });

    putEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<PutRequest, PutResponse>(PutRequest.getDefaultInstance(), PutResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<PutResponse> handle(PutRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.put(request));
        }
      });

    deleteEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<DeleteRequest, DeleteResponse>(DeleteRequest.getDefaultInstance(), DeleteResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<DeleteResponse> handle(DeleteRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.delete(request));
        }
      });

    checkAndPutEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<CheckAndPutRequest, CheckAndPutResponse>(CheckAndPutRequest.getDefaultInstance(), CheckAndPutResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<CheckAndPutResponse> handle(CheckAndPutRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.checkAndPut(request));
        }
      });

    checkAndDeleteEndpointCreator = builder.register(typeId++,
      new AbstractReceiveHandler<CheckAndDeleteRequest, CheckAndDeleteResponse>(CheckAndDeleteRequest.getDefaultInstance(), CheckAndDeleteResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<CheckAndDeleteResponse> handle(CheckAndDeleteRequest request, ArrowBuf dBody) throws RpcException {
          return new SentResponseMessage<>(handler.checkAndDelete(request));
        }
      });

    getStoreEndpointCreator = builder.register(typeId++,
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

  public SendEndpoint<CheckAndPutRequest, CheckAndPutResponse> getCheckAndPutEndpoint() throws RpcException {
    return newEndpoint(checkAndPutEndpointCreator);
  }

  public SendEndpoint<CheckAndDeleteRequest, CheckAndDeleteResponse> getCheckAndDeleteEndpoint() throws RpcException {
    return newEndpoint(checkAndDeleteEndpointCreator);
  }

  public SendEndpoint<GetStoreRequest, GetStoreResponse> getGetStoreEndpoint() throws RpcException {
    return newEndpoint(getStoreEndpointCreator);
  }

}
