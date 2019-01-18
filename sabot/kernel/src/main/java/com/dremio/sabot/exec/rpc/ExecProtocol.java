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
package com.dremio.sabot.exec.rpc;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FinishedReceiver;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.ExecRPC.OOBMessage;
import com.dremio.exec.proto.ExecRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcBus;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * Protocol between executors
 */
public class ExecProtocol implements FabricProtocol {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecProtocol.class);

  public static final Response OK = new Response(RpcType.ACK, Acks.OK);
  public static final Response FAIL = new Response(RpcType.ACK, Acks.FAIL);

  private final FragmentExecutors fragmentsManager;
  private final BufferAllocator allocator;
  private final RpcConfig config;

  public ExecProtocol(SabotConfig config, BufferAllocator allocator, FragmentExecutors fragmentsManager) {
    this.allocator = allocator;
    this.config = getMapping(config);
    this.fragmentsManager = fragmentsManager;
  }

  public void handle(PhysicalConnection connection, int rpcType, ByteString pBody, ByteBuf body, ResponseSender sender) throws RpcException {
    switch(rpcType){
    case RpcType.REQ_RECORD_BATCH_VALUE: {
      final FragmentRecordBatch fragmentBatch = RpcBus.get(pBody, FragmentRecordBatch.PARSER);
      handleFragmentRecordBatch(fragmentBatch, body, sender);
      return;
    }

    case RpcType.REQ_STREAM_COMPLETE_VALUE: {
      final FragmentStreamComplete completion = RpcBus.get(pBody, FragmentStreamComplete.PARSER);
      handleFragmentStreamCompletion(completion);
      sender.send(OK);
      return;
    }

    case RpcType.REQ_RECEIVER_FINISHED_VALUE: {
      final FinishedReceiver completion = RpcBus.get(pBody, FinishedReceiver.PARSER);
      handleReceiverFinished(completion);
      sender.send(OK);
      return;
    }

    case RpcType.REQ_OOB_MESSAGE_VALUE: {
      final OOBMessage oobMessage = RpcBus.get(pBody, OOBMessage.PARSER);
      handleOobMessage(oobMessage);
      sender.send(OK);
      return;
    }

    default:
      throw new UnsupportedOperationException();
    }
  }

  private void handleOobMessage(final OOBMessage message) {
    fragmentsManager.handle(new OutOfBandMessage(message));
  }

  private void handleReceiverFinished(final FinishedReceiver finishedReceiver) throws RpcException {
    fragmentsManager.receiverFinished(finishedReceiver.getSender(), finishedReceiver.getReceiver());
  }

  private void handleFragmentStreamCompletion(final FragmentStreamComplete completion) throws RpcException {
    final int targetCount = completion.getReceivingMinorFragmentIdCount();
    for (int minor = 0; minor < targetCount; minor++) {
      fragmentsManager.handle(getHandle(completion, minor), completion);
    }
  }

  private void handleFragmentRecordBatch(FragmentRecordBatch fragmentBatch, ByteBuf body, ResponseSender sender)
      throws RpcException {

    final AckSender ack = new AckSender(sender);

    // increment so we don't get false returns.
    ack.increment();

    try {

      final IncomingDataBatch batch = new IncomingDataBatch(fragmentBatch, (ArrowBuf) body, ack);
      final int targetCount = fragmentBatch.getReceivingMinorFragmentIdCount();

      // randomize who gets first transfer (and thus ownership) so memory usage
      // is balanced when we're sharing amongst
      // multiple fragments.
      final int firstOwner = ThreadLocalRandom.current().nextInt(targetCount);
      submit(batch, firstOwner, targetCount);
      submit(batch, 0, firstOwner);

      // decrement the extra reference we grabbed at the top.
      ack.sendOk();

    } catch (IOException | FragmentSetupException e) {
      logger.error("Failure while getting fragment manager. {}",
          QueryIdHelper.getQueryIdentifiers(fragmentBatch.getQueryId(), fragmentBatch.getReceivingMajorFragmentId(),
              fragmentBatch.getReceivingMinorFragmentIdList()),
          e);
      ack.clear();
      sender.send(new Response(RpcType.ACK, Acks.FAIL));
    }
  }

  private static FragmentHandle getHandle(FragmentRecordBatch batch, int index) {
    return FragmentHandle.newBuilder()
        .setQueryId(batch.getQueryId())
        .setMajorFragmentId(batch.getReceivingMajorFragmentId())
        .setMinorFragmentId(batch.getReceivingMinorFragmentId(index))
        .build();
  }

  private static FragmentHandle getHandle(FragmentStreamComplete completion, int index){
    return FragmentHandle.newBuilder()
        .setQueryId(completion.getQueryId())
        .setMajorFragmentId(completion.getReceivingMajorFragmentId())
        .setMinorFragmentId(completion.getReceivingMinorFragmentId(index))
        .build();
  }

  private void submit(IncomingDataBatch batch, int minorStart, int minorStopExclusive)
      throws FragmentSetupException, IOException {
    for (int minor = minorStart; minor < minorStopExclusive; minor++) {
      // even though the below method may throw, we don't really care about aborting the loop as the query will fail anyway
      fragmentsManager.handle(getHandle(batch.getHeader(), minor), batch);
    }

  }

  @Override
  public int getProtocolId() {
    return Protocols.EXEC_TO_EXEC;
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
    case RpcType.REQ_RECORD_BATCH_VALUE:
      return FragmentRecordBatch.getDefaultInstance();
    case RpcType.REQ_STREAM_COMPLETE_VALUE:
      return FragmentStreamComplete.getDefaultInstance();

    default:
      throw new UnsupportedOperationException();
    }
  }


  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("DATA")
        .timeout(config.getInt(RpcConstants.BIT_RPC_TIMEOUT))
        .add(RpcType.REQ_RECORD_BATCH, FragmentRecordBatch.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_STREAM_COMPLETE, FragmentStreamComplete.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_RECEIVER_FINISHED, FinishedReceiver.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_OOB_MESSAGE, OOBMessage.class, RpcType.ACK, Ack.class)
        .build();
  }


}
