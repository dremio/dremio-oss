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
package com.dremio.exec.service.executor;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.EndpointListener;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.service.executor.ExecutorServiceClient;
import com.google.protobuf.Empty;
import com.google.protobuf.MessageLite;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

/**
 * Product version of the executor client.
 * Acts as a bridge from gRPC interface to the netty interface.
 */
public class ExecutorServiceProductClient implements ExecutorServiceClient {

  private final CoordToExecTunnelCreator tunnelCreator;
  private final CoordinationProtos.NodeEndpoint endpoint;

  public ExecutorServiceProductClient(CoordToExecTunnelCreator tunnelCreator,
                                      CoordinationProtos.NodeEndpoint endpoint) {
    this.tunnelCreator = tunnelCreator;
    this.endpoint = endpoint;
  }

  @Override
  public void startFragments(CoordExecRPC.InitializeFragments initializeFragments,
                             StreamObserver<Empty> responseObserver)  {
    EndpointListener listener = getEndpointListener(initializeFragments, responseObserver);
    tunnelCreator.getTunnel(endpoint).startFragments(listener, initializeFragments);
  }

  @Override
  public void activateFragments(CoordExecRPC.ActivateFragments activateFragments,
                                StreamObserver<Empty> responseObserver)  {
    EndpointListener listener = getEndpointListener(activateFragments, responseObserver);
    tunnelCreator.getTunnel(endpoint).activateFragments(listener, activateFragments);
  }

  @Override
  public void cancelFragments(CoordExecRPC.CancelFragments cancelFragments,
                              StreamObserver<Empty> responseObserver)  {
    EndpointListener listener = getEndpointListener(cancelFragments, responseObserver);
    tunnelCreator.getTunnel(endpoint).cancelFragments(listener, cancelFragments);
  }

  @Override
  public void reconcileActiveQueries(CoordExecRPC.ActiveQueryList activeQueryList,
                                     StreamObserver<Empty> responseObserver)  {
    EndpointListener listener = getEndpointListener(activeQueryList, responseObserver);
    tunnelCreator.getTunnel(endpoint).reconcileActiveQueries(listener, activeQueryList);
  }

  @Override
  public void propagatePluginChange(CoordExecRPC.SourceWrapper sourceWrapper,
                                     StreamObserver<Empty> responseObserver)  {
    EndpointListener listener = getEndpointListener(sourceWrapper, responseObserver);
    tunnelCreator.getTunnel(endpoint).propagatePluginChange(listener, sourceWrapper);
  }

  @Override
  public void getNodeStats(Empty empty, StreamObserver<CoordExecRPC.NodeStatResp> responseObserver) {
    RpcOutcomeListener outcomeListener = new RpcOutcomeListener<CoordExecRPC.NodeStatResp>() {
      @Override
      @SuppressWarnings("DremioGRPCStreamObserverOnError")
      public void failed(RpcException ex) {
        responseObserver.onError(ex);
      }

      @Override
      public void success(final CoordExecRPC.NodeStatResp nodeStats, ByteBuf buffer) {
        responseObserver.onNext(nodeStats);
        responseObserver.onCompleted();
      }

      @Override
      @SuppressWarnings("DremioGRPCStreamObserverOnError")
      public void interrupted(final InterruptedException ex) {
        responseObserver.onError(ex);
      }
    };
    tunnelCreator.getTunnel(endpoint).requestNodeStats(outcomeListener);
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private <T extends MessageLite> EndpointListener getEndpointListener(T value, StreamObserver<Empty> responseObserver) {
    return new EndpointListener<GeneralRPCProtos.Ack, MessageLite>(endpoint, value) {
      @Override
      @SuppressWarnings("DremioGRPCStreamObserverOnError")
      public void failed(RpcException ex) {
        responseObserver.onError(ex);
      }

      @Override
      public void success(final GeneralRPCProtos.Ack ack, ByteBuf buffer) {
        responseObserver.onCompleted();
      }

      @Override
      @SuppressWarnings("DremioGRPCStreamObserverOnError")
      public void interrupted(final InterruptedException ex) {
        responseObserver.onError(ex);
      }
    };
  }

}
