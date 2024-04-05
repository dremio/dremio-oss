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
package com.dremio.sabot.exec.rpc;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Optimised exec tunnel where both the sender & receiver are in the same process. Bypasses netty
 * channel & directly calls into the dst fragment.
 */
public class InProcessExecTunnel implements ExecTunnel {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(InProcessExecTunnel.class);
  private final FragmentExecutors fragmentExecutors;
  private final BufferAllocator allocator;

  public InProcessExecTunnel(FragmentExecutors fragmentExecutors, BufferAllocator allocator) {
    this.fragmentExecutors = fragmentExecutors;
    this.allocator = allocator;
  }

  @Override
  public void sendStreamComplete(
      RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
      ExecRPC.FragmentStreamComplete streamComplete) {
    try {
      for (Integer minorId : streamComplete.getReceivingMinorFragmentIdList()) {
        ExecProtos.FragmentHandle handle =
            ExecProtos.FragmentHandle.newBuilder()
                .setQueryId(streamComplete.getQueryId())
                .setMajorFragmentId(streamComplete.getReceivingMajorFragmentId())
                .setMinorFragmentId(minorId)
                .build();

        fragmentExecutors.handle(handle, streamComplete);
      }
      outcomeListener.success(Acks.OK, null);
    } catch (Exception e) {
      outcomeListener.failed(new RpcException(e));
    }
  }

  @Override
  public void sendRecordBatch(
      RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, FragmentWritableBatch batch) {
    Preconditions.checkNotNull(batch);
    Preconditions.checkNotNull(batch.getBuffers());

    final AckSenderImpl ack =
        new AckSenderImpl(
            () -> outcomeListener.success(Acks.OK, null), () -> outcomeListener.failed(null));

    // increment so we don't get false returns.
    ack.increment();
    ExecRPC.FragmentRecordBatch header = batch.getHeader();
    header =
        ExecRPC.FragmentRecordBatch.newBuilder(header)
            .setRecvEpochTimestamp(System.currentTimeMillis())
            .build();

    long dataBufLen = batch.getByteCount();
    try (ArrowBuf dBodyBuf = allocator.buffer(dataBufLen)) {
      // copy from all buffers to the single buf.
      // TODO: avoid this copy
      long offset = 0;
      for (final ByteBuf byteBuf : batch.getBuffers()) {
        int count = byteBuf.readableBytes();
        dBodyBuf.setBytes(offset, byteBuf.nioBuffer());
        offset += count;
      }
      final IncomingDataBatch incomingBatch = new IncomingDataBatch(header, dBodyBuf, ack);
      final int targetCount = header.getReceivingMinorFragmentIdCount();

      // randomize who gets first transfer (and thus ownership) so memory usage
      // is balanced when we're sharing amongst
      // multiple fragments.
      final int firstOwner = ThreadLocalRandom.current().nextInt(targetCount);
      submitToFragments(incomingBatch, firstOwner, targetCount);
      submitToFragments(incomingBatch, 0, firstOwner);

      // decrement the extra reference we grabbed at the top.
      ack.sendOk();

    } catch (Exception ex) {
      logger.error(
          "Failure while processing record batch. {}",
          QueryIdHelper.getQueryIdentifiers(
              header.getQueryId(),
              header.getReceivingMajorFragmentId(),
              header.getReceivingMinorFragmentIdList()),
          ex);
      ack.clear();
      outcomeListener.failed(new RpcException(ex));
    } finally {
      for (final ByteBuf byteBuf : batch.getBuffers()) {
        byteBuf.release();
      }
    }
  }

  private void submitToFragments(
      IncomingDataBatch incomingBatch, int minorStart, int minorStopExclusive)
      throws FragmentSetupException, IOException {
    ExecRPC.FragmentRecordBatch header = incomingBatch.getHeader();

    for (int index = minorStart; index < minorStopExclusive; ++index) {
      // even though the below method may throw, we don't really care about aborting the loop as the
      // query will fail anyway
      ExecProtos.FragmentHandle handle =
          ExecProtos.FragmentHandle.newBuilder()
              .setQueryId(header.getQueryId())
              .setMajorFragmentId(header.getReceivingMajorFragmentId())
              .setMinorFragmentId(header.getReceivingMinorFragmentId(index))
              .build();
      fragmentExecutors.handle(handle, incomingBatch);
    }
  }

  @Override
  public void sendOOBMessage(
      RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, OutOfBandMessage message) {
    try {
      Preconditions.checkNotNull(message);

      fragmentExecutors.handle(message);
      outcomeListener.success(Acks.OK, null);
    } catch (Exception e) {
      outcomeListener.failed(new RpcException(e));
    } finally {
      if (message.getBuffers() != null) {
        for (ArrowBuf buf : message.getBuffers()) {
          buf.getReferenceManager().release();
        }
      }
    }
  }

  @Override
  public void informReceiverFinished(
      RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
      ExecRPC.FinishedReceiver finishedReceiver) {
    try {
      Preconditions.checkNotNull(finishedReceiver.getReceiver(), "must set receiver's handle");
      ExecTunnel.checkFragmentHandle(finishedReceiver.getReceiver());
      Preconditions.checkNotNull(finishedReceiver.getSender(), "must set sender's handle");
      ExecTunnel.checkFragmentHandle(finishedReceiver.getSender());

      fragmentExecutors.receiverFinished(
          finishedReceiver.getSender(), finishedReceiver.getReceiver());
      outcomeListener.success(Acks.OK, null);
    } catch (Exception e) {
      outcomeListener.failed(new RpcException(e));
    }
  }
}
