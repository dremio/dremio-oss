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

import java.util.function.Consumer;

import javax.inject.Provider;

import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.ExecToCoordStatusHandler;
import com.dremio.service.maestroservice.MaestroServiceGrpc;
import com.google.common.base.Throwables;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;

import io.grpc.stub.StreamObserver;

/**
 * Server implementation of maestro service.
 *
 * Provides a grpc server facade on top of the underlying ExecToCoordStatusHandler
 */
public class MaestroGrpcServerFacade extends MaestroServiceGrpc.MaestroServiceImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaestroGrpcServerFacade.class);

  private final Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider;

  public MaestroGrpcServerFacade(Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider) {
    this.execToCoordStatusHandlerProvider = execToCoordStatusHandlerProvider;
  }

  /**
   * Handles screen completion events from executors.
   */
  @Override
  public void screenComplete(com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion request,
                             io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(request, new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion>() {
      @Override
      public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion message) {
        try {
          execToCoordStatusHandlerProvider.get().screenCompleted(message);
        } catch (RpcException e) {
          Throwables.propagate(e);
        }
      }
    }, responseObserver);
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private <T extends Message>
  void handleMessage(T request, Consumer<T> consumer, StreamObserver<Empty> responseObserver) {
    try {
      consumer.accept(request);
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  /**
   * Handles node query complete events from Executors.
   */
  @Override
  public void nodeQueryComplete(com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion request,
                                io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(request, new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion>() {
      @Override
      public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion message) {
        try {
          execToCoordStatusHandlerProvider.get().nodeQueryCompleted(message);
        } catch (RpcException e) {
          Throwables.propagate(e);
        }
      }
    }, responseObserver);
  }

  /**
   * Handles first error while processing a query from executors.
   */
  @Override
  public void nodeFirstError(com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError request,
                             io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(request, new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError>() {
      @Override
      public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError message) {
        try {
          execToCoordStatusHandlerProvider.get().nodeQueryMarkFirstError(message);
        } catch (RpcException e) {
          Throwables.propagate(e);
        }
      }
    }, responseObserver);
  }
}
