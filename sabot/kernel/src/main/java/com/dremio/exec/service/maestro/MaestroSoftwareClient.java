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
package com.dremio.exec.service.maestro;

import java.util.concurrent.ForkJoinPool;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;
import com.dremio.service.maestroservice.MaestroClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

/**
 * Software version of the maestro client. Uses fabric to communicate with maestro.
 */
public class MaestroSoftwareClient implements MaestroClient {

  private final ExecToCoordTunnel tunnel;

  public MaestroSoftwareClient(ExecToCoordTunnel tunnel) {
    this.tunnel = tunnel;
  }

  @Override
  public void screenComplete(CoordExecRPC.NodeQueryScreenCompletion request,
                             StreamObserver<Empty> responseObserver) {
    RpcFuture<GeneralRPCProtos.Ack> future = tunnel.sendNodeQueryScreenCompletion(request);
    addCallback(responseObserver, future);
  }

  @Override
  public void nodeQueryComplete(CoordExecRPC.NodeQueryCompletion request,
                                 StreamObserver<Empty> responseObserver) {
    RpcFuture<GeneralRPCProtos.Ack> future = tunnel.sendNodeQueryCompletion(request);
    addCallback(responseObserver, future);
  }

  @Override
  public void nodeFirstError(CoordExecRPC.NodeQueryFirstError request,
                              StreamObserver<Empty> responseObserver) {
    RpcFuture<GeneralRPCProtos.Ack> future = tunnel.sendNodeQueryError(request);
    addCallback(responseObserver, future);
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  public void addCallback(StreamObserver<Empty> responseObserver, RpcFuture<GeneralRPCProtos.Ack> future) {
    Futures.addCallback(
      future,
      new FutureCallback<GeneralRPCProtos.Ack>() {
        // we want this handler to run immediately after we push the big red button!
        public void onSuccess(GeneralRPCProtos.Ack explosion) {
          responseObserver.onNext(Empty.getDefaultInstance());
          responseObserver.onCompleted();
        }

        public void onFailure(Throwable thrown) {
          responseObserver.onError(thrown);
        }
      },
      ForkJoinPool.commonPool());
  }
}
