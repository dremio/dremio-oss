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

import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.ExecRPC.OOBMessage;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;

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
  private final boolean isShrinkMemoryRequest;
  private volatile ArrowBuf[] buffers;
  // OutOfBandMessage may have data buffers, those buffers can be merged into one
  // bigger buffer when transmitting with RPC-based OOB. Buffers are unmerged if
  // transmitting with in-process-tunnel based OOB.
  // To restore original buffers, record each buffer length at construction time.
  private final List<Integer> originalBufferLengths;

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

  public ArrowBuf[] getBuffers() {
    return buffers;
  }

  public List<ArrowBuf> getOriginalBuffers() {
    List<ArrowBuf> result = new ArrayList<ArrowBuf>();
    if (buffers == null || buffers.length == 0) {
      return result;
    }

    Preconditions.checkState(originalBufferLengths != null && originalBufferLengths.size() != 0);

    // multi buffer case
    if (buffers.length > 1) {
      Preconditions.checkState(buffers.length == originalBufferLengths.size());
      result.addAll(Arrays.asList(buffers));
      return result;
    }

    // single buffer case
    long offset = 0L;
    ArrowBuf buffer = buffers[0];
    for (Integer length : originalBufferLengths) {
      Preconditions.checkState(buffer.capacity() >= offset + length);
      result.add(buffer.slice(offset, length));
      offset += length;
    }
    return result;
  }

  public <T> T getPayload(Parser<T> parser) {
    try {
      T obj = parser.parseFrom(payload.bytes);
      if (!obj.getClass().getName().equals(payload.type)) {
        throw new IllegalArgumentException();
      }
      return obj;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isShrinkMemoryRequest() {
    return isShrinkMemoryRequest;
  }

  public boolean getIsOptional() {
    return isOptional;
  }

  public OutOfBandMessage(final OOBMessage message, final ArrowBuf[] bodyBufs) {
    queryId = message.getQueryId();
    operatorId = message.getReceivingOperatorId();
    majorFragmentId = message.getReceivingMajorFragmentId();
    sendingMajorFragmentId = message.getSendingMajorFragmentId();
    sendingMinorFragmentId = message.getSendingMinorFragmentId();
    sendingOperatorId = message.getSendingOperatorId();
    targetMinorFragmentIds = message.getReceivingMinorFragmentIdList();
    isShrinkMemoryRequest = message.getShrinkMemoryRequest();

    payload = new Payload(message.getType(), message.getData().toByteArray());
    isOptional = message.hasIsOptional() ? message.getIsOptional() : true;
    buffers = bodyBufs;
    originalBufferLengths = message.getOriginalBufferLengthList();
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
    builder.setShrinkMemoryRequest(isShrinkMemoryRequest);
    builder.addAllOriginalBufferLength(originalBufferLengths);
    return builder.build();
  }

  public OutOfBandMessage(
      QueryId queryId,
      int majorFragmentId,
      List<Integer> targetMinorFragmentIds,
      int operatorId,
      int sendingMinorFragmentId,
      Payload payload,
      boolean isOptional) {
    this(
        queryId,
        majorFragmentId,
        targetMinorFragmentIds,
        operatorId,
        -1,
        sendingMinorFragmentId,
        -1,
        payload,
        null,
        null,
        isOptional,
        false);
  }

  static List<Integer> createMinorFragmentList(int sendingMinorFragmentId) {
    List<Integer> targetMinorFragmentIds = new ArrayList<>();
    targetMinorFragmentIds.add(sendingMinorFragmentId);
    return targetMinorFragmentIds;
  }

  /** This is used to send an OOB message to shrinkMemory */
  public OutOfBandMessage(
      QueryId queryId,
      int sendingMajorFragmentId,
      int sendingMinorFragmentId,
      int operatorId,
      Payload payload) {
    this(
        queryId,
        sendingMajorFragmentId,
        createMinorFragmentList(sendingMinorFragmentId),
        operatorId,
        sendingMajorFragmentId,
        sendingMinorFragmentId,
        -1,
        payload,
        null,
        null,
        false,
        true);
  }

  public OutOfBandMessage(
      QueryId queryId,
      int majorFragmentId,
      List<Integer> targetMinorFragmentIds,
      int operatorId,
      int sendingMajorFragmentId,
      int sendingMinorFragmentId,
      int sendingOperatorId,
      Payload payload,
      ArrowBuf[] buffers,
      List<Integer> originalBufferLengths,
      boolean isOptional) {
    this(
        queryId,
        majorFragmentId,
        targetMinorFragmentIds,
        operatorId,
        sendingMajorFragmentId,
        sendingMinorFragmentId,
        sendingOperatorId,
        payload,
        buffers,
        originalBufferLengths,
        isOptional,
        false);
  }

  public OutOfBandMessage(
      QueryId queryId,
      int majorFragmentId,
      List<Integer> targetMinorFragmentIds,
      int operatorId,
      int sendingMajorFragmentId,
      int sendingMinorFragmentId,
      int sendingOperatorId,
      Payload payload,
      ArrowBuf[] buffers,
      List<Integer> originalBufferLengths,
      boolean isOptional,
      boolean isShrinkMemoryRequest) {
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
    this.isShrinkMemoryRequest = isShrinkMemoryRequest;
    this.buffers = buffers;
    this.originalBufferLengths =
        originalBufferLengths == null ? new ArrayList<>() : originalBufferLengths;

    // Caller is expected to release its own copy
    if (this.buffers != null && this.buffers.length > 0) {
      Arrays.stream(this.buffers).forEach(arrowBuf -> arrowBuf.getReferenceManager().retain());
    } else {
      this.buffers = new ArrowBuf[0];
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
