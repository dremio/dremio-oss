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
package com.dremio.exec.store.sys.udf;

import static com.dremio.exec.rpc.RpcConstants.BIT_RPC_TIMEOUT;
import static com.dremio.sabot.rpc.Protocols.UDF_EXEC_TO_CORD;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.FunctionRPC;
import com.dremio.exec.proto.GeneralRPCProtos;
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

public class FunctionProtocol implements FabricProtocol {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FunctionProtocol.class);

  private final BufferAllocator allocator;
  private final RpcConfig config;
  private final UserDefinedFunctionService udfService;

  public FunctionProtocol(
      BufferAllocator allocator, SabotConfig config, UserDefinedFunctionService udfService) {
    this.allocator = allocator;
    this.config = getMapping(config);
    this.udfService = udfService;
  }

  @Override
  public int getProtocolId() {
    return UDF_EXEC_TO_CORD;
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
      case FunctionRPC.RpcType.ACK_VALUE:
        return GeneralRPCProtos.Ack.getDefaultInstance();
      case FunctionRPC.RpcType.RESP_FUNCTION_INFO_VALUE:
        return FunctionRPC.FunctionInfoResp.getDefaultInstance();
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
      case FunctionRPC.RpcType.REQ_FUNCTION_INFO_VALUE:
        {
          Iterable<UserDefinedFunctionService.FunctionInfo> FunctionInfos =
              this.udfService.getFunctions();
          FunctionRPC.FunctionInfoResp resp =
              FunctionRPC.FunctionInfoResp.newBuilder()
                  .addAllFunctionInfo(
                      StreamSupport.stream(FunctionInfos.spliterator(), false)
                          .map(UserDefinedFunctionService.FunctionInfo::toProto)
                          .collect(Collectors.toList()))
                  .build();
          sender.send(new Response(FunctionRPC.RpcType.RESP_FUNCTION_INFO, resp));
          break;
        }
      default:
        throw new RpcException(
            "Message received that is not yet supported. Message type: " + rpcType);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("UdfExecToCoord")
        .timeout(config.getInt(BIT_RPC_TIMEOUT))
        .add(
            FunctionRPC.RpcType.REQ_FUNCTION_INFO,
            FunctionRPC.FunctionInfoReq.class,
            FunctionRPC.RpcType.RESP_FUNCTION_INFO,
            FunctionRPC.FunctionInfoResp.class)
        .build();
  }
}
