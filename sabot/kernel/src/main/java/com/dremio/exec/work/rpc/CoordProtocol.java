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
package com.dremio.exec.work.rpc;

import static com.dremio.exec.rpc.RpcBus.get;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.CoordRPC;
import com.dremio.exec.proto.CoordRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

public class CoordProtocol implements FabricProtocol {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordProtocol.class);

  private static final Response OK = new Response(RpcType.ACK, Acks.OK);
  private static final Response FAIL = new Response(RpcType.ACK, Acks.FAIL);

  private final BufferAllocator allocator;
  private final ForemenTool tool;
  private final RpcConfig config;

  public CoordProtocol(BufferAllocator allocator, ForemenTool tool, SabotConfig config) {
    this.allocator = allocator;
    this.tool = tool;
    this.config = getMapping(config);
  }

  @Override
  public int getProtocolId() {
    return Protocols.COORD_TO_COORD;
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

    case RpcType.RESP_QUERY_PROFILE_VALUE:
      return QueryProfile.getDefaultInstance();

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

    case RpcType.REQ_QUERY_CANCEL_VALUE: {
      final CoordRPC.JobCancelRequest jobCancelRequest = get(pBody, CoordRPC.JobCancelRequest.PARSER);
      final ExternalId id = jobCancelRequest.getExternalId();
      final String cancelreason = jobCancelRequest.getCancelReason();
      boolean canceled = tool.cancel(id, cancelreason);
      final Response outcome = canceled ? OK : FAIL;
      sender.send(outcome);
      break;
    }

    case RpcType.REQ_QUERY_PROFILE_VALUE: {
      ExternalId id = get(pBody, ExternalId.PARSER);
      Optional<QueryProfile> profile = tool.getProfile(id);
      if(!profile.isPresent()){
        throw new RpcException("Unable to get profile for query with id: " + ExternalIdHelper.toString(id));
      }

      sender.send(new Response(RpcType.RESP_QUERY_PROFILE, profile.get()));
      break;
    }
    default:
      throw new RpcException("Message received that is not yet supported. Message type: " + rpcType);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("CoordToCoord")
        .timeout(config.getInt(RpcConstants.BIT_RPC_TIMEOUT))
        .add(RpcType.REQ_QUERY_CANCEL, CoordRPC.JobCancelRequest.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_QUERY_PROFILE, ExternalId.class, RpcType.RESP_QUERY_PROFILE, QueryProfile.class)
        .build();
  }
}
