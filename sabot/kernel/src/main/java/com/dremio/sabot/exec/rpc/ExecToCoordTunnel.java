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

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.ListeningCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.jobresults.common.JobResultsTunnel;

import io.netty.buffer.ByteBuf;

/**
 * Handler for messages going from coordinator to executor.
 */
public class ExecToCoordTunnel extends JobResultsTunnel {

  private final FabricCommandRunner manager;
  private final NodeEndpoint endpoint;

  public ExecToCoordTunnel(NodeEndpoint endpoint, FabricCommandRunner runner) {
    this.manager = runner;
    this.endpoint = endpoint;
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch data) {
    manager.runCommand(new SendBatch(outcomeListener, data));
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

  public RpcFuture<Ack> sendNodeQueryScreenCompletion(NodeQueryScreenCompletion completion) {
    FutureBitCommand<Ack, ProxyConnection> cmd = new FutureBitCommand<Ack, ProxyConnection>() {
      @Override
      public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
        connection.sendUnsafe(outcomeListener, RpcType.REQ_NODE_QUERY_SCREEN_COMPLETION, completion, Ack.class);
      }
    };
    manager.runCommand(cmd);
    return cmd.getFuture();
  }

  public RpcFuture<Ack> sendNodeQueryCompletion(NodeQueryCompletion completion) {
    FutureBitCommand<Ack, ProxyConnection> cmd = new FutureBitCommand<Ack, ProxyConnection>() {
      @Override
      public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
        connection.sendUnsafe(outcomeListener, RpcType.REQ_NODE_QUERY_COMPLETION, completion, Ack.class);
      }
    };
    manager.runCommand(cmd);
    return cmd.getFuture();
  }

  public RpcFuture<Ack> sendNodeQueryError(NodeQueryFirstError firstError) {
    FutureBitCommand<Ack, ProxyConnection> cmd = new FutureBitCommand<Ack, ProxyConnection>() {
      @Override
      public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
        connection.sendUnsafe(outcomeListener, RpcType.REQ_NODE_QUERY_ERROR, firstError, Ack.class);
      }
    };
    manager.runCommand(cmd);
    return cmd.getFuture();
  }

  public RpcFuture<Ack> sendNodeQueryProfile(ExecutorQueryProfile profile) {
    FutureBitCommand<Ack, ProxyConnection> cmd = new FutureBitCommand<Ack, ProxyConnection>() {
      @Override
      public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
        connection.sendUnsafe(outcomeListener, RpcType.REQ_NODE_QUERY_PROFILE, profile, Ack.class);
      }
    };
    manager.runCommand(cmd);
    return cmd.getFuture();
  }
}
