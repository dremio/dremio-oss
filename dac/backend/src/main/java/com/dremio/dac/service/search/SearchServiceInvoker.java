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
package com.dremio.dac.service.search;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.RemoteNamespaceException;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.simple.AbstractReceiveHandler;
import com.dremio.services.fabric.simple.ProtocolBuilder;
import com.dremio.services.fabric.simple.SendEndpoint;
import com.dremio.services.fabric.simple.SendEndpointCreator;
import com.dremio.services.fabric.simple.SentResponseMessage;
import com.google.protobuf.ByteString;

import io.protostuff.LinkedBuffer;

/**
 * Adapter that interacts with the {@link SearchService} running on master coordinator.
 */
public class SearchServiceInvoker implements SearchService {
  private static final int TYPE_SEARCH_QUERY = 0;

  private final boolean isMaster;
  private final Provider<NodeEndpoint> nodeEndpointProvider;
  private final Provider<Optional<NodeEndpoint>> taskLeaderProvider;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final SearchService searchService;

  private SendEndpointCreator<SearchRPC.SearchQueryRequest, SearchRPC.SearchQueryResponse> findEndpointCreator;

  public SearchServiceInvoker(
    boolean isMaster,
    Provider<NodeEndpoint> nodeEndpointProvider,
    final Provider<Optional<NodeEndpoint>> taskLeaderProvider,
    Provider<FabricService> fabricService,
    BufferAllocator allocator,
    SearchService searchService
  ) {
    this.isMaster = isMaster;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.taskLeaderProvider = taskLeaderProvider;
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.searchService = searchService;
  }

  @Override
  public void start() throws Exception {
    searchService.start();

    final ProtocolBuilder builder = ProtocolBuilder.builder()
      .protocolId(58)
      .allocator(allocator)
      .name("search-rpc")
      .timeout(10 * 1000);

    this.findEndpointCreator = builder.register(TYPE_SEARCH_QUERY,
      new AbstractReceiveHandler<SearchRPC.SearchQueryRequest, SearchRPC.SearchQueryResponse>(
        SearchRPC.SearchQueryRequest.getDefaultInstance(), SearchRPC.SearchQueryResponse.getDefaultInstance()) {
        @Override
        public SentResponseMessage<SearchRPC.SearchQueryResponse> handle(SearchRPC.SearchQueryRequest rpcRequest, ArrowBuf dBody) {
          final String query = rpcRequest.getQuery();
          final String username = rpcRequest.getUsername();

          final List<SearchContainer> search;
          try {
            search = search(query, username);
          } catch (NamespaceException e) {
            return new SentResponseMessage<>(
              SearchRPC.SearchQueryResponse.newBuilder().setFailureMessage(e.getMessage()).build()
            );
          }

          final List<SearchRPC.SearchQueryResponseEntity> searchRPCResults = search.stream().map(input -> {
            final LinkedBuffer buffer = LinkedBuffer.allocate();
            final ByteString bytes = input.getNamespaceContainer().clone(buffer);
            buffer.clear();

            final SearchRPC.SearchQueryResponseEntity.Builder rpcBuilder = SearchRPC.SearchQueryResponseEntity.newBuilder();
            rpcBuilder.setResponse(bytes);

            final CollaborationTag collaborationTag = input.getCollaborationTag();

            if (collaborationTag != null) {
              final SearchRPC.SearchQueryResponseTags searchQueryRequestTags = SearchRPC.SearchQueryResponseTags.newBuilder()
                .addAllTags(collaborationTag.getTagsList())
                .setEntityId(collaborationTag.getEntityId())
                .setId(collaborationTag.getId())
                .setLastModified(collaborationTag.getLastModified())
                .setVersion(collaborationTag.getTag())
                .build();

              rpcBuilder.setTags(searchQueryRequestTags);
            }
            return rpcBuilder.build();
          }).collect(Collectors.toList());

          return new SentResponseMessage<> (
            SearchRPC.SearchQueryResponse.newBuilder().addAllResults(searchRPCResults).build()
          );
        }
      });

    builder.register(fabricService.get());
  }

  @Override
  public void close() throws Exception {
    searchService.close();
  }

  @Override
  public List<SearchContainer> search(String query, String username) throws NamespaceException {
    // TODO DX-14433 - should have better way to deal with Local/Remote KVStore
    final NodeEndpoint master = taskLeaderProvider.get().orElse(null);
    final NodeEndpoint thisNode = nodeEndpointProvider.get();
    if (isMaster && thisNode.equals(master)) {
      return searchService.search(query, username);
    }

    try {
      return doRPCSearch(query, username);
    } catch (RpcException e) {
      throw new RemoteNamespaceException("search failed: " + e.getMessage());
    }
  }

  private List<SearchContainer> doRPCSearch(String query, String username) throws RpcException {
    final SearchRPC.SearchQueryRequest.Builder builder = SearchRPC.SearchQueryRequest.newBuilder();
    if (query != null) {
      builder.setQuery(query);
    }

    if (username != null) {
      builder.setUsername(username);
    }

    final SearchRPC.SearchQueryResponse body = newFindEndpoint().send(builder.build()).getBody();

    if (body.hasFailureMessage()) {
      throw new RpcException(body.getFailureMessage());
    }

    return body.getResultsList().stream().map(input -> {
      final SearchRPC.SearchQueryResponseTags tagsRPC = input.getTags();

      final CollaborationTag collaborationTag = new CollaborationTag();
      collaborationTag.setTagsList(tagsRPC.getTagsList());
      collaborationTag.setEntityId(tagsRPC.getEntityId());
      collaborationTag.setId(tagsRPC.getId());
      collaborationTag.setLastModified(tagsRPC.getLastModified());
      collaborationTag.setTag(tagsRPC.getVersion());

      final NameSpaceContainer nameSpaceContainer = NameSpaceContainer.from(input.getResponse());

      return new SearchContainer(nameSpaceContainer, collaborationTag);
    }).collect(Collectors.toList());
  }

  private SendEndpoint<SearchRPC.SearchQueryRequest, SearchRPC.SearchQueryResponse> newFindEndpoint() throws RpcException {
    final NodeEndpoint master = taskLeaderProvider.get().orElse(null);

    if (master == null) {
      throw new RpcException("master node is down");
    }

    return findEndpointCreator.getEndpoint(master.getAddress(), master.getFabricPort());
  }

  @Override
  public void wakeupManager(String reason) {
    // TODO DX-14433 - should have better way to deal with Local/Remote KVStore
    final NodeEndpoint master = taskLeaderProvider.get().orElse(null);
    final NodeEndpoint thisNode = nodeEndpointProvider.get();

    if (isMaster && thisNode.equals(master)) {
      searchService.wakeupManager(reason);
    }
  }
}
