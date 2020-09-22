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
package com.dremio.service.listing;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.namespace.DatasetListingRPC.DLGetSourceRequest;
import com.dremio.namespace.DatasetListingRPC.DLGetSourceResponse;
import com.dremio.namespace.DatasetListingRPC.DLGetSourcesRequest;
import com.dremio.namespace.DatasetListingRPC.DLGetSourcesResponse;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.RemoteNamespaceException;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.simple.AbstractReceiveHandler;
import com.dremio.services.fabric.simple.ProtocolBuilder;
import com.dremio.services.fabric.simple.SendEndpoint;
import com.dremio.services.fabric.simple.SendEndpointCreator;
import com.dremio.services.fabric.simple.SentResponseMessage;
import com.google.protobuf.ByteString;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

/**
 * Adapter that interacts with the {@link DatasetListingService} running on master coordinator.
 * <p>
 * If this service is running on either an executor or a non-master coordinator, this will makes blocking remote
 * procedure calls using the {@link FabricService} to communicate with the {@link DatasetListingService listing service}
 * running on the master coordinator node. Otherwise, i.e. on master coordinator node, this will interact with the
 * in situ {@link DatasetListingServiceImpl listing service}.
 */
public class DatasetListingInvoker implements DatasetListingService {
  private static final int TYPE_DL_FIND = 1; // no longer supported
  private static final int TYPE_DL_SOURCE = 2;
  private static final int TYPE_DL_SOURCES = 3;

  private final boolean isMaster;
  private final Provider<NodeEndpoint> masterEndpoint;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final DatasetListingService datasetListing;

  private SendEndpointCreator<DLGetSourceRequest, DLGetSourceResponse> getSourceEndpointCreator; // used on server and client side
  private SendEndpointCreator<DLGetSourcesRequest, DLGetSourcesResponse> getSourcesEndpointCreator; // used on server and client side

  public DatasetListingInvoker(
      final boolean isMaster,
      final Provider<NodeEndpoint> masterEndpoint,
      final Provider<FabricService> fabricService,
      final BufferAllocator allocator,
      final DatasetListingService datasetListing
  ) {
    this.isMaster = isMaster;
    this.masterEndpoint = masterEndpoint;
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.datasetListing = datasetListing;
  }

  @Override
  public void start() throws Exception {
    datasetListing.start();
    final ProtocolBuilder builder = ProtocolBuilder.builder()
        .protocolId(57)
        .allocator(allocator)
        .name("dataset-listing-rpc")
        .timeout(10 * 1000);

    this.getSourceEndpointCreator = builder.register(TYPE_DL_SOURCE,
        new AbstractReceiveHandler<DLGetSourceRequest, DLGetSourceResponse>(
            DLGetSourceRequest.getDefaultInstance(), DLGetSourceResponse.getDefaultInstance()) {
          @Override
          public SentResponseMessage<DLGetSourceResponse> handle(DLGetSourceRequest getSourceRequest, ArrowBuf dBody) {

            final SourceConfig sourceResults;
            try {
              sourceResults = datasetListing.getSource(getSourceRequest.getUsername(), getSourceRequest.getSourcename());
            } catch (NamespaceException e) {
              return new SentResponseMessage<>(
                  DLGetSourceResponse.newBuilder()
                      .setFailureMessage(e.getMessage())
                      .build());
            }

            LinkedBuffer buffer = LinkedBuffer.allocate();
            // TODO(DX-10857): change from opaque object to protobuf; avoid unnecessary copies
            ByteString bytes = ByteString.copyFrom(
              ProtobufIOUtil.toByteArray(sourceResults, SourceConfig.getSchema(), buffer));
            buffer.clear();

            return new SentResponseMessage<>(
                DLGetSourceResponse.newBuilder()
                    .setResponse(bytes)
                    .build());
          }
        });

    this.getSourcesEndpointCreator = builder.register(TYPE_DL_SOURCES,
        new AbstractReceiveHandler<DLGetSourcesRequest, DLGetSourcesResponse>(
            DLGetSourcesRequest.getDefaultInstance(), DLGetSourcesResponse.getDefaultInstance()) {
          @Override
          public SentResponseMessage<DLGetSourcesResponse> handle(DLGetSourcesRequest getSourcesRequest, ArrowBuf dBody) {

            final List<SourceConfig> sourcesResults;
            try {
              sourcesResults = datasetListing.getSources(getSourcesRequest.getUsername());
            } catch (NamespaceException e) {
              return new SentResponseMessage<>(
                  DLGetSourcesResponse.newBuilder()
                      .setFailureMessage(e.getMessage())
                      .build());
            }

            LinkedBuffer buffer = LinkedBuffer.allocate();
            List<ByteString> containersAsBytes = sourcesResults.stream().map(input -> {
                // TODO(DX-10857): change from opaque object to protobuf; avoid unnecessary copies
                final ByteString bytes = ByteString.copyFrom(
                  ProtobufIOUtil.toByteArray(input, SourceConfig.getSchema(), buffer));
                  buffer.clear();
                  return bytes;
              }).collect(Collectors.toList());

            return new SentResponseMessage<>(
                DLGetSourcesResponse.newBuilder()
                    .addAllResponse(containersAsBytes)
                    .build());
          }
        });

    builder.register(fabricService.get());
  }

  @Override
  public void close() throws Exception {
    datasetListing.close();
  }

  private SendEndpoint<DLGetSourcesRequest, DLGetSourcesResponse> newGetSourcesEndpoint() throws RpcException {
    final NodeEndpoint master = masterEndpoint.get();
    if (master == null) {
      throw new RpcException("master node is down");
    }
    // TODO(DX-10861): separate server-side and client-side code, when the ticket is resolved
    return getSourcesEndpointCreator.getEndpoint(master.getAddress(), master.getFabricPort());
  }

  private SendEndpoint<DLGetSourceRequest, DLGetSourceResponse> newGetSourceEndpoint() throws RpcException {
    final NodeEndpoint master = masterEndpoint.get();
    if (master == null) {
      throw new RpcException("master node is down");
    }
    // TODO(DX-10861): separate server-side and client-side code, when the ticket is resolved
    return getSourceEndpointCreator.getEndpoint(master.getAddress(), master.getFabricPort());
  }

  @Override
  public SourceConfig getSource(
    String username,
    String sourcename
  ) throws NamespaceException {
     if (isMaster) { // RPC calls unless running on master
      return datasetListing.getSource(username, sourcename);
    }

    final DLGetSourceRequest.Builder requestBuilder = DLGetSourceRequest.newBuilder();
    requestBuilder.setUsername(username);
    requestBuilder.setSourcename(sourcename);

    final DLGetSourceResponse getSourceResponse;
    try {
      getSourceResponse = newGetSourceEndpoint()
        .send(requestBuilder.build())
        .getBody();
    } catch (RpcException e) {
      throw new RemoteNamespaceException("dataset listing failed: " + e.getMessage());
    }
    if (getSourceResponse.hasFailureMessage()) {
      throw new RemoteNamespaceException(getSourceResponse.getFailureMessage());
    }

    final SourceConfig source = SourceConfig.getSchema().newMessage();
    ProtobufIOUtil.mergeFrom(getSourceResponse.getResponse().toByteArray(), source, SourceConfig.getSchema());

    return source;
  }

  @Override
  public List<SourceConfig> getSources(
    String username
  ) throws NamespaceException {
    if (isMaster) { // RPC calls unless running on master
      return datasetListing.getSources(username);
    }

    final DLGetSourcesRequest.Builder requestBuilder = DLGetSourcesRequest.newBuilder();
    requestBuilder.setUsername(username);

    final DLGetSourcesResponse getSourcesResponse;
    try {
      getSourcesResponse = newGetSourcesEndpoint()
        .send(requestBuilder.build())
        .getBody();
    } catch (RpcException e) {
      throw new RemoteNamespaceException("dataset listing failed: " + e.getMessage());
    }
    if (getSourcesResponse.hasFailureMessage()) {
      throw new RemoteNamespaceException(getSourcesResponse.getFailureMessage());
    }

    return getSourcesResponse.getResponseList().stream().map(input -> {
        // TODO(DX-10857): change from opaque object to protobuf; avoid unnecessary copies
        final SourceConfig source = SourceConfig.getSchema().newMessage();
        ProtobufIOUtil.mergeFrom(input.toByteArray(), source, SourceConfig.getSchema());
        return source;
      }).collect(Collectors.toList());
  }
}
