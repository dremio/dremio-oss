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

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.ExecToCoordStatusHandler;
import com.dremio.service.maestroservice.MaestroServiceGrpc;
import com.dremio.telemetry.api.metrics.SimpleUpdatableTimer;
import com.google.common.base.Throwables;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.inject.Provider;

/**
 * Server implementation of maestro service.
 *
 * <p>Provides a grpc server facade on top of the underlying ExecToCoordStatusHandler
 */
public class MaestroGrpcServerFacade extends MaestroServiceGrpc.MaestroServiceImplBase {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaestroGrpcServerFacade.class);

  private final Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider;
  public static final String EXECUTOR_ADDRESS_LABEL = "executorAddress";
  public static final String UPDATE_SCREEN_OPERATOR_COMPLETE_MS_LABEL =
      "update_screen_operator_complete_ms";
  public static final String SCREEN_COMPLETE_RPC_MS_LABEL = "screen_complete_rpc_ms";
  public static final String NODE_COMPLETE_RPC_MS_LABEL = "node_complete_rpc_ms";
  public static final String NODE_FIRST_ERROR_RPC_MS_LABEL = "node_first_error_rpc_ms";
  private static final SimpleUpdatableTimer UPDATE_SCREEN_OPERATOR_COMPLETE_MS_TIMER =
      SimpleUpdatableTimer.of(UPDATE_SCREEN_OPERATOR_COMPLETE_MS_LABEL);
  private static final SimpleUpdatableTimer SCREEN_COMPLETE_RPC_MS_TIMER =
      SimpleUpdatableTimer.of(SCREEN_COMPLETE_RPC_MS_LABEL);
  private static final SimpleUpdatableTimer NODE_COMPLETE_RPC_MS_TIMER =
      SimpleUpdatableTimer.of(NODE_COMPLETE_RPC_MS_LABEL);
  private static final SimpleUpdatableTimer NODE_FIRST_ERROR_RPC_MS_TIMER =
      SimpleUpdatableTimer.of(NODE_FIRST_ERROR_RPC_MS_LABEL);

  public MaestroGrpcServerFacade(
      Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider) {
    this.execToCoordStatusHandlerProvider = execToCoordStatusHandlerProvider;
  }

  /** Handles screen completion events from executors. */
  @Override
  public void screenComplete(
      com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion request,
      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(
        request,
        new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion>() {
          @Override
          public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion message) {
            try {
              long rpcTime = System.currentTimeMillis() - message.getRpcStartedAt();
              SCREEN_COMPLETE_RPC_MS_TIMER.update(
                  rpcTime,
                  TimeUnit.MILLISECONDS,
                  Tags.of(EXECUTOR_ADDRESS_LABEL, message.getEndpoint().getAddress()));
            } catch (Exception e) {
              logger.warn("Failure updating {} metric", SCREEN_COMPLETE_RPC_MS_LABEL, e);
            }
            try {
              // Time taken before sending screenComplete RPC after screen operator completion
              long updateScreenOperatorCompleteTime =
                  message.getRpcStartedAt() - message.getScreenOperatorCompletionTime();
              UPDATE_SCREEN_OPERATOR_COMPLETE_MS_TIMER.update(
                  updateScreenOperatorCompleteTime,
                  TimeUnit.MILLISECONDS,
                  Tags.of(EXECUTOR_ADDRESS_LABEL, message.getEndpoint().getAddress()));
            } catch (Exception e) {
              logger.warn(
                  "Failure updating {} metric", UPDATE_SCREEN_OPERATOR_COMPLETE_MS_TIMER, e);
            }
            try {
              execToCoordStatusHandlerProvider.get().screenCompleted(message);
            } catch (RpcException e) {
              Throwables.propagate(e);
            }
          }
        },
        responseObserver,
        "screenComplete");
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  @WithSpan
  private <T extends Message> void handleMessage(
      T request, Consumer<T> consumer, StreamObserver<Empty> responseObserver, String rpcName) {
    Span currentSpan = Span.current();
    try {
      consumer.accept(request);
      long start = System.currentTimeMillis();
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
      currentSpan.setAttribute(
          String.join(".", rpcName, "ack_time_ms"), System.currentTimeMillis() - start);
    } catch (Exception e) {
      currentSpan.setStatus(StatusCode.ERROR, String.join(" ", rpcName, "rpc lifecycle failed"));
      currentSpan.recordException(e);
      responseObserver.onError(e);
    }
  }

  /** Handles node query complete events from Executors. */
  @Override
  @WithSpan(kind = SpanKind.SERVER)
  public void nodeQueryComplete(
      com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion request,
      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(
        request,
        new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion>() {
          @Override
          public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion message) {
            String queryId = QueryIdHelper.getQueryId(message.getId());
            try {
              long rpcTime = System.currentTimeMillis() - message.getRpcStartedAt();
              NODE_COMPLETE_RPC_MS_TIMER.update(
                  rpcTime,
                  TimeUnit.MILLISECONDS,
                  Tags.of(EXECUTOR_ADDRESS_LABEL, message.getEndpoint().getAddress()));
            } catch (Exception e) {
              logger.warn("Failure updating {} metric", NODE_COMPLETE_RPC_MS_LABEL, e);
            }
            Span currentSpan = Span.current();
            currentSpan.setAttribute("query_id", queryId);
            currentSpan.setAttribute("executor_address", message.getEndpoint().getAddress());
            currentSpan.setAttribute(
                "target_coordinator_address", message.getForeman().getAddress());
            try {
              execToCoordStatusHandlerProvider.get().nodeQueryCompleted(message);
            } catch (RpcException e) {
              currentSpan.setStatus(
                  StatusCode.ERROR, "nodeQueryComplete rpc server handling failed");
              currentSpan.recordException(e);
              Throwables.propagate(e);
            }
          }
        },
        responseObserver,
        "nodeQueryComplete");
  }

  /** Handles first error while processing a query from executors. */
  @Override
  public void nodeFirstError(
      com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError request,
      io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
    handleMessage(
        request,
        new Consumer<com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError>() {
          @Override
          public void accept(com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError message) {
            try {
              long rpcTime = System.currentTimeMillis() - message.getRpcStartedAt();
              NODE_FIRST_ERROR_RPC_MS_TIMER.update(
                  rpcTime,
                  TimeUnit.MILLISECONDS,
                  Tags.of(EXECUTOR_ADDRESS_LABEL, message.getEndpoint().getAddress()));
            } catch (Exception e) {
              logger.warn("Failure updating {} metric", NODE_FIRST_ERROR_RPC_MS_LABEL, e);
            }
            try {
              execToCoordStatusHandlerProvider.get().nodeQueryMarkFirstError(message);
            } catch (RpcException e) {
              Throwables.propagate(e);
            }
          }
        },
        responseObserver,
        "nodeFirstError");
  }
}
