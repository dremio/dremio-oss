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

import java.util.Optional;

import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FinishedReceiver;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.ExecRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.rpc.ListeningCommand;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

public class ExecTunnel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecTunnel.class);

  private final FabricCommandRunner manager;

  public ExecTunnel(FabricCommandRunner runner) {
    this.manager = runner;
  }

  public void sendStreamComplete(RpcOutcomeListener<Ack> outcomeListener, FragmentStreamComplete streamComplete) {
    manager.runCommand(new SendStreamCompleteListen(outcomeListener, streamComplete));
  }

  public void sendRecordBatch(RpcOutcomeListener<Ack> outcomeListener, FragmentWritableBatch batch) {
    manager.runCommand(new SendBatchAsyncListen(outcomeListener, batch));
  }

  public void sendOOBMessage(RpcOutcomeListener<Ack> outcomeListener, OutOfBandMessage message) {
    manager.runCommand(new SendOOBMessage(outcomeListener, message));
  }

  private static void checkFragmentHandle(FragmentHandle handle) {
    Preconditions.checkState(handle.hasQueryId(), "must set query id");
    Preconditions.checkState(handle.hasMajorFragmentId(), "must set major fragment id");
    Preconditions.checkState(handle.hasMinorFragmentId(), "must set minor fragment id");
  }

  public void informReceiverFinished(RpcOutcomeListener<Ack> outcomeListener, FinishedReceiver finishedReceiver){
    Preconditions.checkNotNull(finishedReceiver.getReceiver(), "must set receiver's handle");
    checkFragmentHandle(finishedReceiver.getReceiver());
    Preconditions.checkNotNull(finishedReceiver.getSender(), "must set sender's handle");
    checkFragmentHandle(finishedReceiver.getSender());

    final ReceiverFinished b = new ReceiverFinished(outcomeListener, finishedReceiver);
    manager.runCommand(b);
  }

  private class SendStreamCompleteListen extends ListeningCommand<Ack, ProxyConnection> {
    final FragmentStreamComplete completion;

    public SendStreamCompleteListen(RpcOutcomeListener<Ack> listener, FragmentStreamComplete completion) {
      super(listener);
      this.completion = completion;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_STREAM_COMPLETE, completion, Ack.class);
    }

  }

  private class SendBatchAsyncListen extends ListeningCommand<Ack, ProxyConnection> {
    final FragmentWritableBatch batch;

    public SendBatchAsyncListen(RpcOutcomeListener<Ack> listener, FragmentWritableBatch batch) {
      super(listener);
      this.batch = batch;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_RECORD_BATCH, batch.getHeader(), Ack.class, batch.getBuffers());
    }

    @Override
    public String toString() {
      return "SendBatch [batch.header=" + batch.getHeader() + "]";
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      for(ByteBuf buffer : batch.getBuffers()) {
        buffer.release();
      }
      super.connectionFailed(type, t);
    }
  }

  private class SendOOBMessage extends ListeningCommand<Ack, ProxyConnection> {
    private final OutOfBandMessage message;

    public SendOOBMessage(RpcOutcomeListener<Ack> listener, OutOfBandMessage message) {
      super(listener);
      this.message = message;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      final int buffCount = Optional.ofNullable(message).map(m -> m.getBuffers().length).orElse(0);
      final ByteBuf[] buffers = new ByteBuf[buffCount];
      for (int i=0; i < buffCount; i++) {
        buffers[i] = NettyArrowBuf.unwrapBuffer(message.getBuffers()[i]);
      }
      connection.send(outcomeListener, RpcType.REQ_OOB_MESSAGE, message.toProtoMessage(), Ack.class, buffers);
    }

    @Override
    public String toString() {
      return "Send OOBMessage [" + message + "]";
    }
  }

  public static class ReceiverFinished extends ListeningCommand<Ack, ProxyConnection> {
    final FinishedReceiver finishedReceiver;

    public ReceiverFinished(RpcOutcomeListener<Ack> listener, FinishedReceiver finishedReceiver) {
      super(listener);
      this.finishedReceiver = finishedReceiver;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_RECEIVER_FINISHED, finishedReceiver, Ack.class);
    }
  }
}
