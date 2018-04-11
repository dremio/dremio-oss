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
package com.dremio.exec.work.rpc;

import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.ListeningCommand;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;

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

  public void sendFragments(RpcOutcomeListener<Ack> outcomeListener, InitializeFragments fragments){
    SendFragment b = new SendFragment(outcomeListener, fragments);
    manager.runCommand(b);
  }

  public void cancelFragment(RpcOutcomeListener<Ack> outcomeListener, FragmentHandle handle){
    final SignalFragment b = new SignalFragment(outcomeListener, handle, RpcType.REQ_CANCEL_FRAGMENTS);
    manager.runCommand(b);
  }

  private static class SignalFragment extends ListeningCommand<Ack, ProxyConnection> {
    final FragmentHandle handle;
    final RpcType type;

    public SignalFragment(RpcOutcomeListener<Ack> listener, FragmentHandle handle, RpcType type) {
      super(listener);
      this.handle = handle;
      this.type = type;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.sendUnsafe(outcomeListener, type, handle, Ack.class);
    }

  }

  private static class SendFragment extends ListeningCommand<Ack, ProxyConnection> {
    final InitializeFragments fragments;

    public SendFragment(RpcOutcomeListener<Ack> listener, InitializeFragments fragments) {
      super(listener);
      this.fragments = fragments;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_START_FRAGMENTS, fragments, Ack.class);
    }

  }
}
