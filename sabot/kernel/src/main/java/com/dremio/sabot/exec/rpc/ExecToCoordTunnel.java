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

import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.ListeningCommand;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;

import io.netty.buffer.ByteBuf;

/**
 * Handler for messages going from coordinator to executor.
 */
public class ExecToCoordTunnel {

  private final FabricCommandRunner manager;
  private final NodeEndpoint endpoint;

  public ExecToCoordTunnel(NodeEndpoint endpoint, FabricCommandRunner runner) {
    this.manager = runner;
    this.endpoint = endpoint;
  }

  public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch data) {
    manager.runCommand(new SendBatch(outcomeListener, data));
  }

  public void sendFragmentStatus(RpcOutcomeListener<Ack> outcomeListener, FragmentStatus status){
    SendFragmentStatus b = new SendFragmentStatus(outcomeListener, status);
    manager.runCommand(b);
  }

  public RpcFuture<Ack> sendFragmentStatus(FragmentStatus status){
    SendFragmentStatusFuture b = new SendFragmentStatusFuture(status);
    manager.runCommand(b);
    return b.getFuture();
  }

  private static class SendFragmentStatus extends ListeningCommand<Ack, ProxyConnection> {
    final FragmentStatus status;

    public SendFragmentStatus(RpcOutcomeListener<Ack> outcomeListener, FragmentStatus status) {
      super(outcomeListener);
      this.status = status;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.sendUnsafe(outcomeListener, RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
    }

  }

  private static class SendFragmentStatusFuture extends FutureBitCommand<Ack, ProxyConnection> {
    final FragmentStatus status;

    public SendFragmentStatusFuture(FragmentStatus status) {
      this.status = status;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.sendUnsafe(outcomeListener, RpcType.REQ_FRAGMENT_STATUS, status, Ack.class);
    }

  }

  public RpcFuture<Ack> sendNodeQueryStatus(NodeQueryStatus status){
    SendNodeQueryStatusFuture b = new SendNodeQueryStatusFuture(status);
    manager.runCommand(b);
    return b.getFuture();
  }

  private static class SendNodeQueryStatusFuture extends FutureBitCommand<Ack, ProxyConnection> {
    final NodeQueryStatus status;

    public SendNodeQueryStatusFuture(NodeQueryStatus status) {
      this.status = status;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.sendUnsafe(outcomeListener, RpcType.REQ_NODE_QUERY_STATUS, status, Ack.class);
    }
  }

  private class SendBatch extends ListeningCommand<Ack, ProxyConnection> {
    private final QueryWritableBatch batch;

    public SendBatch(RpcOutcomeListener<Ack> listener, QueryWritableBatch batch) {
      super(listener);
      this.batch = batch;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_QUERY_DATA, batch.getHeader(), Ack.class, batch.getBuffers());
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      for(ByteBuf buffer : batch.getBuffers()) {
        buffer.release();
      }
      super.connectionFailed(type, t);
    }
  }


}
