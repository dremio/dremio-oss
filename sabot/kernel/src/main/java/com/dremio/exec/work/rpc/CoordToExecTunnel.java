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

import com.dremio.exec.proto.CoordExecRPC.ActivateFragments;
import com.dremio.exec.proto.CoordExecRPC.CancelFragments;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.NodeStatReq;
import com.dremio.exec.proto.CoordExecRPC.NodeStatResp;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.ListeningCommand;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.google.protobuf.MessageLite;

/**
 * Send messages from coordinator to executor.
 */
public class CoordToExecTunnel {

  private final FabricCommandRunner manager;
  private final NodeEndpoint endpoint;

  public CoordToExecTunnel(NodeEndpoint endpoint, FabricCommandRunner runner) {
    this.manager = runner;
    this.endpoint = endpoint;
  }

  public void startFragments(RpcOutcomeListener<Ack> outcomeListener, InitializeFragments fragments){
    SendFragment b = new SendFragment(outcomeListener, RpcType.REQ_START_FRAGMENTS, fragments);
    manager.runCommand(b);
  }

  public void activateFragments(RpcOutcomeListener<Ack> outcomeListener, ActivateFragments fragments){
    SendFragment b = new SendFragment(outcomeListener, RpcType.REQ_ACTIVATE_FRAGMENTS, fragments);
    manager.runCommand(b);
  }

  public void cancelFragments(RpcOutcomeListener<Ack> outcomeListener, CancelFragments fragments){
    final SignalFragment b = new SignalFragment(outcomeListener, RpcType.REQ_CANCEL_FRAGMENTS, fragments);
    manager.runCommand(b);
  }

  public void requestNodeStats(RpcOutcomeListener<NodeStatResp> outcomeListener) {
    NodeStatReq nodeStatReq = NodeStatReq.newBuilder().build();
    RequestNodeStat b = new RequestNodeStat(nodeStatReq, outcomeListener);
    manager.runCommand(b);
  }

  private static class SignalFragment extends ListeningCommand<Ack, ProxyConnection> {
    final RpcType type;
    final MessageLite message;

    public SignalFragment(RpcOutcomeListener<Ack> listener, RpcType type, MessageLite message) {
      super(listener);
      this.type = type;
      this.message = message;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.sendUnsafe(outcomeListener, type, message, Ack.class);
    }

  }

  private static class SendFragment extends ListeningCommand<Ack, ProxyConnection> {
    final RpcType type;
    final MessageLite message;

    public SendFragment(RpcOutcomeListener<Ack> listener, RpcType type, MessageLite message) {
      super(listener);
      this.type = type;
      this.message = message;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, type, message, Ack.class);
    }

  }

  public static class RequestNodeStat extends ListeningCommand<NodeStatResp, ProxyConnection> {
    private final NodeStatReq nodeStatRequest;

    public RequestNodeStat(NodeStatReq nodeStatRequest, RpcOutcomeListener<NodeStatResp> outcomeListener) {
      super(outcomeListener);
      this.nodeStatRequest = nodeStatRequest;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<NodeStatResp> outcomeListener, ProxyConnection
      connection) {
      connection.send(outcomeListener, RpcType.REQ_NODE_STATS, nodeStatRequest, NodeStatResp.class);
    }
  }
}
