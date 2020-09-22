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

import io.netty.buffer.ByteBuf;

public class OutOfBandMessage {
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final QueryId queryId;
  private final int majorFragmentId;
  private final List<Integer> targetMinorFragmentIds;
  private final int operatorId;
  private final int sendingMinorFragmentId;
  private final int sendingMajorFragmentId;
  private final int sendingOperatorId;
  private final Payload payload;
  private final boolean isOptional;
  private volatile ByteBuf buffer;

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

  public int getSendingMajorFragmentId() {
    return sendingMajorFragmentId;
  }

  public int getSendingOperatorId() {
    return sendingOperatorId;
  }

  public ByteBuf getBuffer() {
    return buffer;
  }

  public void retainBufferIfPresent() {
    if (buffer != null && buffer.capacity() > 0) {
      buffer.retain();
    }
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

  public OutOfBandMessage(final OOBMessage message, final ByteBuf body) {
    queryId = message.getQueryId();
    operatorId = message.getReceivingOperatorId();
    majorFragmentId = message.getReceivingMajorFragmentId();
    sendingMajorFragmentId = message.getSendingMajorFragmentId();
    sendingMinorFragmentId = message.getSendingMinorFragmentId();
    sendingOperatorId = message.getSendingOperatorId();
    targetMinorFragmentIds = message.getReceivingMinorFragmentIdList();

    payload = new Payload(message.getType(), message.getData().toByteArray());
    isOptional = message.hasIsOptional() ? message.getIsOptional() : true;
    buffer = body;
  }

  public OOBMessage toProtoMessage() {
    OOBMessage.Builder builder = OOBMessage.newBuilder();

    builder.setQueryId(queryId);
    builder.setReceivingOperatorId(operatorId);
    builder.setReceivingMajorFragmentId(majorFragmentId);
    if (sendingMajorFragmentId != -1) {
      builder.setSendingMajorFragmentId(sendingMajorFragmentId);
    }
    builder.setSendingMinorFragmentId(sendingMinorFragmentId);
    if (sendingOperatorId != -1) {
      builder.setSendingOperatorId(sendingOperatorId);
    }
    builder.addAllReceivingMinorFragmentId(targetMinorFragmentIds);

    builder.setData(ByteString.copyFrom(payload.bytes));
    builder.setType(payload.type);
    builder.setIsOptional(isOptional);
    return builder.build();
  }

  public OutOfBandMessage(QueryId queryId, int majorFragmentId, List<Integer> targetMinorFragmentIds, int operatorId,
                          int sendingMinorFragmentId, Payload payload, boolean isOptional) {
    this(queryId, majorFragmentId, targetMinorFragmentIds, operatorId,
      -1, sendingMinorFragmentId, -1, payload, null, isOptional);
  }

  public OutOfBandMessage(QueryId queryId, int majorFragmentId, List<Integer> targetMinorFragmentIds, int operatorId,
      int sendingMajorFragmentId, int sendingMinorFragmentId, int sendingOperatorId, Payload payload, ByteBuf buffer, boolean isOptional) {
    super();
    this.queryId = queryId;
    this.majorFragmentId = majorFragmentId;
    this.targetMinorFragmentIds = targetMinorFragmentIds;
    this.operatorId = operatorId;
    this.sendingMajorFragmentId = sendingMajorFragmentId;
    this.sendingMinorFragmentId = sendingMinorFragmentId;
    this.sendingOperatorId = sendingOperatorId;
    this.payload = payload;
    this.isOptional = isOptional;
    this.buffer = buffer;

    // Caller is expected to release its own copy
    if (this.buffer != null) {
      this.buffer.retain();
    }
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
