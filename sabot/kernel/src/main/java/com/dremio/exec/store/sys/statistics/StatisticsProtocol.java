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
package com.dremio.exec.store.sys.statistics;

import static com.dremio.exec.rpc.RpcConstants.BIT_RPC_TIMEOUT;
import static com.dremio.sabot.rpc.Protocols.STATISTICS_EXEC_TO_COORD;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.StatisticsRPC;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;

public class StatisticsProtocol implements FabricProtocol {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(StatisticsProtocol.class);

  private final BufferAllocator allocator;
  private final RpcConfig config;
  private final StatisticsService statisticsService;

  public StatisticsProtocol(
      BufferAllocator allocator, SabotConfig config, StatisticsService statisticsService) {
    this.allocator = allocator;
    this.config = getMapping(config);
    this.statisticsService = statisticsService;
  }

  @Override
  public int getProtocolId() {
    return STATISTICS_EXEC_TO_COORD;
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
      case StatisticsRPC.RpcType.ACK_VALUE:
        return GeneralRPCProtos.Ack.getDefaultInstance();
      case StatisticsRPC.RpcType.RESP_STATISTICS_INFO_VALUE:
        return StatisticsRPC.StatisticsInfoResp.getDefaultInstance();
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
      ResponseSender sender)
      throws RpcException {
    if (RpcConstants.EXTRA_DEBUGGING) {
      logger.debug("Received exec > coord message of type {}", rpcType);
    }

    switch (rpcType) {
      case StatisticsRPC.RpcType.REQ_STATISTICS_INFO_VALUE:
        {
          Iterable<StatisticsListManager.StatisticsInfo> statisticsInfos =
              statisticsService.getStatisticsInfos();
          StatisticsRPC.StatisticsInfoResp resp =
              StatisticsRPC.StatisticsInfoResp.newBuilder()
                  .addAllStatisticsInfo(
                      StreamSupport.stream(statisticsInfos.spliterator(), false)
                          .map(StatisticsListManager.StatisticsInfo::toProto)
                          .collect(Collectors.toList()))
                  .build();
          sender.send(new Response(StatisticsRPC.RpcType.RESP_STATISTICS_INFO, resp));
          break;
        }
      default:
        throw new RpcException(
            "Message received that is not yet supported. Message type: " + rpcType);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("StatisticsExecToCoord")
        .timeout(config.getInt(BIT_RPC_TIMEOUT))
        .add(
            StatisticsRPC.RpcType.REQ_STATISTICS_INFO,
            StatisticsRPC.StatisticsInfoReq.class,
            StatisticsRPC.RpcType.RESP_STATISTICS_INFO,
            StatisticsRPC.StatisticsInfoResp.class)
        .build();
  }
}
