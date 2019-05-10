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
package com.dremio.sabot.exec.fragment;

import java.util.List;

import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.ExecRPC.OOBMessage;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public class OutOfBandMessage {

  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final QueryId queryId;
  private final int majorFragmentId;
  private final List<Integer> targetMinorFragmentIds;
  private final int operatorId;
  private final int sendingMinorFragmentId;
  private final Payload payload;
  private final boolean isOptional;

  public QueryId getQueryId() {
    return queryId;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public List<Integer> getTargetMinorFragmentIds() {
    return targetMinorFragmentIds;
  }

  public int getSendingMinorFragmentId() {
    return sendingMinorFragmentId;
  }

  public <T> T getPayload(Parser<T> parser) {
    try {
      T obj = parser.parseFrom(payload.bytes);
      if(!obj.getClass().getName().equals(payload.type)) {
        throw new IllegalArgumentException();
      }
      return obj;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public Payload getPayload() {
    return payload;
  }

  public boolean getIsOptional() { return isOptional; }

  public OutOfBandMessage(OOBMessage message) {
    queryId = message.getQueryId();
    operatorId = message.getReceivingOperatorId();
    majorFragmentId = message.getReceivingMajorFragmentId();
    sendingMinorFragmentId = message.getSendingMinorFragmentId();
    targetMinorFragmentIds = message.getReceivingMinorFragmentIdList();

    payload = new Payload(message.getType(), message.getData().toByteArray());
    isOptional = message.hasIsOptional() ? message.getIsOptional() : true;
  }

  public OOBMessage toProtoMessage() {
    OOBMessage.Builder builder = OOBMessage.newBuilder();

    builder.setQueryId(queryId);
    builder.setReceivingOperatorId(operatorId);
    builder.setReceivingMajorFragmentId(majorFragmentId);
    builder.setSendingMinorFragmentId(sendingMinorFragmentId);
    builder.addAllReceivingMinorFragmentId(targetMinorFragmentIds);

    builder.setData(ByteString.copyFrom(payload.bytes));
    builder.setType(payload.type);
    builder.setIsOptional(isOptional);
    return builder.build();
  }

  public OutOfBandMessage(QueryId queryId, int majorFragmentId, List<Integer> targetMinorFragmentIds, int operatorId,
      int sendingMinorFragmentId, Payload payload, boolean isOptional) {
    super();
    this.queryId = queryId;
    this.majorFragmentId = majorFragmentId;
    this.targetMinorFragmentIds = targetMinorFragmentIds;
    this.operatorId = operatorId;
    this.sendingMinorFragmentId = sendingMinorFragmentId;
    this.payload = payload;
    this.isOptional = isOptional;
  }

  public static class Payload {

    private final String type;
    private final byte[] bytes;

    public Payload(MessageLite item) {
      this.type = item.getClass().getName();
      this.bytes = item.toByteArray();
    }

    public Payload(String type, byte[] bytes) {
      super();
      this.type = type;
      this.bytes = bytes;
    }

    public String getType() {
      return type;
    }

    public byte[] getBytes() {
      return bytes;
    }

  }

}
