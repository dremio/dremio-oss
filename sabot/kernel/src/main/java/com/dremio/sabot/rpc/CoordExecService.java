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
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.metrics.Metrics;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * Provides support for communication between coordination and executor nodes. Run on both types of nodes but only one handler may be valid.
 */
public class CoordExecService implements Service {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordExecService.class);
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final BufferAllocator allocator;
  private final Provider<CoordToExecHandler> coordToExec;
  private final Provider<ExecToCoordHandler> execToCoord;
  private final Provider<FabricService> fabricService;
  private final RpcConfig config;

  /**
   * Create a new exec service. Note that at start time, the provider to one of
   * the handlers may be a noop implementation. This is allowed if this is a
   * single role node.
   *
   * @param config
   * @param allocator
   * @param bindingCreator
   * @param fabricService
   * @param coordToExec
   * @param execToCoord
   */
  public CoordExecService(
      SabotConfig config,
      BufferAllocator allocator,
      BindingCreator bindingCreator,
      Provider<FabricService> fabricService,
      Provider<CoordToExecHandler> coordToExec,
      Provider<ExecToCoordHandler> execToCoord
      ) {
    super();
    // as part of construction, register default noop implementations for both handler.
    bindingCreator.bind(CoordToExecHandler.class, NoCoordToExecHandler.class);
    bindingCreator.bind(ExecToCoordHandler.class, NoExecToCoordHandler.class);

    this.fabricService = fabricService;
    this.allocator =  allocator.newChildAllocator("coord-exec-rpc",
        config.getLong("dremio.exec.rpc.bit.server.memory.data.reservation"),
        config.getLong("dremio.exec.rpc.bit.server.memory.data.maximum"));
    this.coordToExec = coordToExec;
    this.execToCoord = execToCoord;
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

      switch (rpcType) {

      // coordinator > executor
      case RpcType.REQ_CANCEL_FRAGMENTS_VALUE: {
        final CancelFragments fragments = get(pBody, CancelFragments.PARSER);
        coordToExec.get().cancelFragments(fragments);
        sender.send(OK);
        break;
      }

      // coordinator > executor
      case RpcType.REQ_START_FRAGMENTS_VALUE: {
        final InitializeFragments fragments = get(pBody, InitializeFragments.PARSER);
        coordToExec.get().startFragments(fragments, sender);
        break;
      }

      // coordinator > executor
      case RpcType.REQ_ACTIVATE_FRAGMENTS_VALUE: {
        final ActivateFragments fragments = get(pBody, ActivateFragments.PARSER);
        coordToExec.get().activateFragments(fragments);
        sender.send(OK);
        break;
      }

      // executor > coordinator
      case RpcType.REQ_FRAGMENT_STATUS_VALUE:
        FragmentStatus status = get(pBody, FragmentStatus.PARSER);
        execToCoord.get().fragmentStatusUpdate(status);
        sender.send(OK);
        break;

      case RpcType.REQ_QUERY_DATA_VALUE:
        QueryData header = get(pBody, QueryData.PARSER);
        execToCoord.get().dataArrived(header, dBody, sender);
        break;

      case RpcType.REQ_NODE_QUERY_STATUS_VALUE:
        NodeQueryStatus nodeQueryStatus = get(pBody, NodeQueryStatus.PARSER);
        execToCoord.get().nodeQueryStatusUpdate(nodeQueryStatus);
        sender.send(OK);
        break;

      default:
        throw new RpcException("Message received that is not yet supported. Message type: " + rpcType);
      }
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("CoordToExec")
        .timeout(config.getInt(RpcConstants.BIT_RPC_TIMEOUT))
        .add(RpcType.REQ_START_FRAGMENTS, InitializeFragments.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_ACTIVATE_FRAGMENTS, ActivateFragments.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_CANCEL_FRAGMENTS, CancelFragments.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_FRAGMENT_STATUS, FragmentStatus.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_QUERY_DATA, QueryData.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_NODE_QUERY_STATUS, NodeQueryStatus.class, RpcType.ACK, Ack.class)
        .build();
  }

  public static final class NoCoordToExecHandler implements CoordToExecHandler {

    @Inject
    public NoCoordToExecHandler(){}

    @Override
    public void startFragments(InitializeFragments fragments, ResponseSender sender) throws RpcException {
      throw new RpcException("This daemon doesn't support execution operations.");
    }

    @Override
    public void activateFragments(ActivateFragments fragments) throws RpcException {
      throw new RpcException("This daemon doesn't support execution operations.");
    }

    @Override
    public void cancelFragments(CancelFragments fragments) throws RpcException {
      throw new RpcException("This daemon doesn't support execution operations.");
    }

  }

  public static final class NoExecToCoordHandler implements ExecToCoordHandler {

    @Inject
    public NoExecToCoordHandler(){}

    @Override
    public void fragmentStatusUpdate(FragmentStatus update) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }

    @Override
    public void dataArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }

    @Override
    public void nodeQueryStatusUpdate(NodeQueryStatus update) throws RpcException {
      throw new RpcException("This daemon doesn't support coordination operations.");
    }

  }
}
