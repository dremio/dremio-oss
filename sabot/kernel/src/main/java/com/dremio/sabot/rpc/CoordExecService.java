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
package com.dremio.sabot.rpc;

import static com.dremio.exec.rpc.RpcBus.get;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordExecRPC.ActivateFragments;
import com.dremio.exec.proto.CoordExecRPC.CancelFragments;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeStatReq;
import com.dremio.exec.proto.CoordExecRPC.NodeStatResp;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.exec.service.executor.ExecutorService;
import com.dremio.service.Service;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.dremio.service.jobtelemetry.PutExecutorProfileRequest;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.MessageLite;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

/**
 * Provides support for communication between coordination and executor nodes. Run on both types of nodes but only one handler may be valid.
 */
public class CoordExecService implements Service {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordExecService.class);
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final BufferAllocator allocator;
  private final Provider<ExecutorService> executorService;
  private final Provider<ExecToCoordResultsHandler> execResults;
  private final Provider<ExecToCoordStatusHandler> execStatus;
  private final Provider<FabricService> fabricService;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpoint;
  private final Provider<JobTelemetryClient> jobTelemetryClient;
  private final RpcConfig config;

  /**
   * Create a new exec service. Note that at start time, the provider to one of
   * the handlers may be a noop implementation. This is allowed if this is a
   * single role node.
   *  @param config
   * @param allocator
   * @param fabricService
   * @param executorService
   * @param execResults
   * @param execStatus
   */
  public CoordExecService(
    SabotConfig config,
    BufferAllocator allocator,
    Provider<FabricService> fabricService,
    Provider<ExecutorService> executorService,
    Provider<ExecToCoordResultsHandler> execResults,
    Provider<ExecToCoordStatusHandler> execStatus,
    Provider<CoordinationProtos.NodeEndpoint> selfEndpoint,
    Provider<JobTelemetryClient> jobTelemetryClient) {

    super();

    this.fabricService = fabricService;
    this.allocator =  allocator.newChildAllocator("coord-exec-rpc",
            config.getLong("dremio.exec.rpc.bit.server.memory.data.reservation"),
            config.getLong("dremio.exec.rpc.bit.server.memory.data.maximum"));
    this.executorService = executorService;
    this.execResults = execResults;
    this.execStatus = execStatus;
    this.selfEndpoint = selfEndpoint;
    this.jobTelemetryClient = jobTelemetryClient;
    this.config = getMapping(config);
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  @Override
  public void start() throws Exception {
    fabricService.get().registerProtocol(new CoordExecProtocol());

    final String prefix = "rpc";
    Metrics.newGauge(prefix + "bit.control.current", allocator::getAllocatedMemory);
    Metrics.newGauge(prefix + "bit.control.peak", allocator::getPeakMemoryAllocation);
  }


  private final class CoordExecProtocol implements FabricProtocol {

    public CoordExecProtocol() {
      super();
    }

    @Override
    public int getProtocolId() {
      return Protocols.COORD_TO_EXEC;
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public RpcConfig getConfig() {
      return config;
    }

    @Override
    public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
      switch (rpcType) {
        case RpcType.ACK_VALUE:
          return Ack.getDefaultInstance();
        case RpcType.RESP_NODE_STATS_VALUE:
          return NodeStatResp.getDefaultInstance();
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public void handle(
            PhysicalConnection connection,
            int rpcType,
            ByteString pBody,
            ByteBuf dBody,
            ResponseSender sender) throws RpcException {

      if (RpcConstants.EXTRA_DEBUGGING) {
        logger.debug("Received exec > coord message of type {}", rpcType);
      }

      StreamObserver<Empty> responseObserver = new StreamObserver<Empty>() {
        @Override
        public void onNext(Empty empty) {
          // no-op
        }

        @Override
        public void onError(Throwable throwable) {
          sender.sendFailure(mapToUserRpcException(throwable));
        }

        @Override
        public void onCompleted() {
          sender.send(OK);
        }
      };

      switch (rpcType) {

        // coordinator > executor
        case RpcType.REQ_CANCEL_FRAGMENTS_VALUE: {
          final CancelFragments fragments = get(pBody, CancelFragments.PARSER);
          executorService.get().cancelFragments(fragments, responseObserver);
          break;
        }

        // coordinator > executor
        case RpcType.REQ_START_FRAGMENTS_VALUE: {
          final InitializeFragments fragments = get(pBody, InitializeFragments.PARSER);
          executorService.get().startFragments(fragments, responseObserver);
          break;
        }

        // coordinator > executor
        case RpcType.REQ_ACTIVATE_FRAGMENTS_VALUE: {
          final ActivateFragments fragments = get(pBody, ActivateFragments.PARSER);
          executorService.get().activateFragment(fragments, responseObserver);
          break;
        }

        case RpcType.REQ_NODE_STATS_VALUE:
          StreamObserver<NodeStatResp> responseObserverStats = new StreamObserver<NodeStatResp>() {
            private NodeStatResp nodeStatResp;
            @Override
            public void onNext(NodeStatResp response) {
              nodeStatResp = response;
            }

            @Override
            public void onError(Throwable throwable) {
              sender.sendFailure(mapToUserRpcException(throwable));
            }

            @Override
            public void onCompleted() {
              sender.send(new Response(RpcType.RESP_NODE_STATS, nodeStatResp));
            }
          };
          executorService.get().getNodeStats(Empty.newBuilder().build(), responseObserverStats);
          break;

        // executor > coordinator
        case RpcType.REQ_QUERY_DATA_VALUE:
          QueryData header = get(pBody, QueryData.PARSER);
          execResults.get().dataArrived(header, dBody, sender);
          break;

        case RpcType.REQ_NODE_QUERY_SCREEN_COMPLETION_VALUE:
          NodeQueryScreenCompletion completion = get(pBody, NodeQueryScreenCompletion.PARSER);
          execStatus.get().screenCompleted(completion);
          sender.send(OK);
          break;

        case RpcType.REQ_NODE_QUERY_COMPLETION_VALUE:
          NodeQueryCompletion screenCompletion = get(pBody, NodeQueryCompletion.PARSER);
          execStatus.get().nodeQueryCompleted(screenCompletion);
          sender.send(OK);
          break;

        case RpcType.REQ_NODE_QUERY_ERROR_VALUE:
          NodeQueryFirstError firstError = get(pBody, NodeQueryFirstError.PARSER);
          execStatus.get().nodeQueryMarkFirstError(firstError);
          sender.send(OK);
          break;
        case RpcType.REQ_NODE_QUERY_PROFILE_VALUE:
          ExecutorQueryProfile profile = get(pBody, ExecutorQueryProfile.PARSER);
          // propagate to job-telemetry service (in-process server).
          JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub stub = jobTelemetryClient.get().getBlockingStub();
          if (stub == null) {
           // telemetry client/service has not been fully started. a message can still arrive
           // if coordinator has been restarted while active queries are running in executor.
           logger.info("Dropping a profile message from end point : " + profile.getEndpoint() +
             ". This is harmless since the query will be terminated shortly due to coordinator " +
             "restarting");
          } else {
            stub.putExecutorProfile(
              PutExecutorProfileRequest
                .newBuilder()
                .setProfile(profile)
                .build()
            );
          }
          sender.send(OK);
          break;

        default:
          throw new RpcException("Message received that is not yet supported. Message type: " + rpcType);
      }
    }
  }

  private UserRpcException mapToUserRpcException(Throwable t) {
    if (t instanceof UserRpcException) {
      return (UserRpcException)t;
    } else {
      return new UserRpcException(selfEndpoint.get(),
        "failure while processing message from coordinator", t);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
            .name("CoordToExec")
            .timeout(config.getInt(RpcConstants.BIT_RPC_TIMEOUT))
            .add(RpcType.REQ_START_FRAGMENTS, InitializeFragments.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_ACTIVATE_FRAGMENTS, ActivateFragments.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_CANCEL_FRAGMENTS, CancelFragments.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_QUERY_DATA, QueryData.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_NODE_QUERY_SCREEN_COMPLETION, NodeQueryScreenCompletion.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_NODE_QUERY_COMPLETION, NodeQueryCompletion.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_NODE_QUERY_ERROR, NodeQueryFirstError.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_NODE_QUERY_PROFILE, ExecutorQueryProfile.class, RpcType.ACK, Ack.class)
            .add(RpcType.REQ_NODE_STATS, NodeStatReq.class, RpcType.RESP_NODE_STATS, NodeStatResp.class)
            .build();
  }

  public static final class NoExecToCoordResultsHandler implements ExecToCoordResultsHandler {

    @Inject
    public NoExecToCoordResultsHandler(){}

    @Override
    public void dataArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }
  }

  public static final class NoExecToCoordStatusHandler implements ExecToCoordStatusHandler {

    @Inject
    public NoExecToCoordStatusHandler(){}

    @Override
    public void screenCompleted(NodeQueryScreenCompletion completion) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }

    @Override
    public void nodeQueryCompleted(NodeQueryCompletion completion) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }

    @Override
    public void nodeQueryMarkFirstError(NodeQueryFirstError error) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }
  }
}
