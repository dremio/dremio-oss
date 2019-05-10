/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.accelerator;

import static com.dremio.exec.rpc.RpcConstants.BIT_RPC_TIMEOUT;
import static com.dremio.sabot.rpc.Protocols.REFLECTIONS_EXEC_TO_COORD;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * ReflectionProtocol to get data that is unavailable on Executor from Coordinator
 */
class ReflectionProtocol implements FabricProtocol {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionProtocol.class);

  private static final Response OK = new Response(ReflectionRPC.RpcType.ACK, Acks.OK);
  private static final Response FAIL = new Response(ReflectionRPC.RpcType.ACK, Acks.FAIL);

  private final BufferAllocator allocator;
  private final RpcConfig config;
  private final ReflectionStatusService reflectionStatusService;
  private final ReflectionService reflectionService;
  private final MaterializationStore materializationStore;

  public ReflectionProtocol(BufferAllocator allocator, ReflectionStatusService reflectionStatusService,
                            ReflectionService reflectionService, MaterializationStore materializationStore,
                            SabotConfig config) {
    this.allocator = allocator;
    this.config = getMapping(config);
    this.reflectionStatusService = reflectionStatusService;
    this.reflectionService = reflectionService;
    this.materializationStore = materializationStore;
  }

  @Override
  public int getProtocolId() {
    return REFLECTIONS_EXEC_TO_COORD;
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
      case ReflectionRPC.RpcType.ACK_VALUE:
        return GeneralRPCProtos.Ack.getDefaultInstance();

      case ReflectionRPC.RpcType.RESP_REFLECTION_INFO_VALUE:
        return ReflectionRPC.ReflectionInfoResp.getDefaultInstance();

      case ReflectionRPC.RpcType.RESP_REFRESH_INFO_VALUE:
        return ReflectionRPC.RefreshInfoResp.getDefaultInstance();

      case ReflectionRPC.RpcType.RESP_DEPENDENCY_INFO_VALUE:
        return ReflectionRPC.DependencyInfoResp.getDefaultInstance();

      case ReflectionRPC.RpcType.RESP_MATERIALIZATION_INFO_VALUE:
        return ReflectionRPC.MaterializationInfoResp.getDefaultInstance();

      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void handle(PhysicalConnection connection, int rpcType, ByteString pBody, ByteBuf dBody, ResponseSender sender) throws RpcException {
    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Received exec > coord message of type {}", rpcType);
    }

    switch (rpcType) {
      case ReflectionRPC.RpcType.REQ_REFLECTION_INFO_VALUE: {
        Iterable<AccelerationListManager.ReflectionInfo> reflections = reflectionStatusService.getReflections();
        FluentIterable<ReflectionRPC.ReflectionInfo> reflectionsProto = FluentIterable.from(reflections)
          .transform(new Function<AccelerationListManager.ReflectionInfo, ReflectionRPC.ReflectionInfo>() {
            @Nullable
            @Override
            public ReflectionRPC.ReflectionInfo apply(@Nullable AccelerationListManager.ReflectionInfo reflectionInfo) {
              return reflectionInfo.toProto();
            }
          });

        ReflectionRPC.ReflectionInfoResp response = ReflectionRPC.ReflectionInfoResp.newBuilder()
        .addAllReflectionInfo(reflectionsProto).build();
        sender.send(new Response(ReflectionRPC.RpcType.RESP_REFLECTION_INFO, response));
        break;
      }
      case ReflectionRPC.RpcType.REQ_REFRESH_INFO_VALUE: {
        Iterable<ReflectionRPC.RefreshInfo> refreshInfos = reflectionStatusService.getRefreshInfos();
        ReflectionRPC.RefreshInfoResp response = ReflectionRPC.RefreshInfoResp.newBuilder()
          .addAllRefreshInfo(refreshInfos).build();
        sender.send(new Response(ReflectionRPC.RpcType.RESP_REFRESH_INFO, response));
        break;
      }
      case ReflectionRPC.RpcType.REQ_DEPENDENCY_INFO_VALUE: {
        Iterable<AccelerationListManager.DependencyInfo> dependencyInfos = reflectionService.getReflectionDependencies();
        FluentIterable<ReflectionRPC.DependencyInfo> dependenciesProto = FluentIterable.from(dependencyInfos)
          .transform(new Function<AccelerationListManager.DependencyInfo, ReflectionRPC.DependencyInfo>() {
            @Override
            public ReflectionRPC.DependencyInfo apply(AccelerationListManager.DependencyInfo dependencyInfo) {
              return dependencyInfo.toProto();
            }
          });

        ReflectionRPC.DependencyInfoResp response = ReflectionRPC.DependencyInfoResp.newBuilder()
          .addAllDependencyInfo(dependenciesProto).build();
        sender.send(new Response(ReflectionRPC.RpcType.RESP_DEPENDENCY_INFO, response));
        break;
      }
      case ReflectionRPC.RpcType.REQ_MATERIALIZATION_INFO_VALUE: {
        Iterable<AccelerationListManager.MaterializationInfo> materializationInfos =
          AccelerationMaterializationUtils.getMaterializationsFromStore(materializationStore);
        FluentIterable<ReflectionRPC.MaterializationInfo> materializationProto = FluentIterable.from(materializationInfos)
          .transform(new Function<AccelerationListManager.MaterializationInfo, ReflectionRPC.MaterializationInfo>() {
            @Override
            public ReflectionRPC.MaterializationInfo apply(AccelerationListManager.MaterializationInfo materializationInfo) {
              return materializationInfo.toProto();
            }
          });

        ReflectionRPC.MaterializationInfoResp response = ReflectionRPC.MaterializationInfoResp.newBuilder()
          .addAllMaterializationInfo(materializationProto).build();
        sender.send(new Response(ReflectionRPC.RpcType.RESP_MATERIALIZATION_INFO, response));
        break;
      }
      default:
        throw new RpcException("Message received that is not yet supported. Message type: " + rpcType);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
      .name("ReflectionExecToCoord")
      .timeout(config.getInt(BIT_RPC_TIMEOUT))
      .add(ReflectionRPC.RpcType.REQ_REFLECTION_INFO, ReflectionRPC.ReflectionInfoReq.class,
        ReflectionRPC.RpcType.RESP_REFLECTION_INFO, ReflectionRPC.ReflectionInfoResp.class)
      .add(ReflectionRPC.RpcType.REQ_REFRESH_INFO, ReflectionRPC.RefreshInfoReq.class,
        ReflectionRPC.RpcType.RESP_REFRESH_INFO, ReflectionRPC.RefreshInfoResp.class)
      .add(ReflectionRPC.RpcType.REQ_DEPENDENCY_INFO, ReflectionRPC.DependencyInfoReq.class,
        ReflectionRPC.RpcType.RESP_DEPENDENCY_INFO, ReflectionRPC.DependencyInfoResp.class)
      .add(ReflectionRPC.RpcType.REQ_MATERIALIZATION_INFO, ReflectionRPC.MaterializationInfoReq.class,
        ReflectionRPC.RpcType.RESP_MATERIALIZATION_INFO, ReflectionRPC.MaterializationInfoResp.class)
      .build();
  }
}
