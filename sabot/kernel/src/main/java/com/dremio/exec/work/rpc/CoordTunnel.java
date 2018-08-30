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

import com.dremio.exec.proto.CoordRPC.RpcType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;

public class CoordTunnel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordTunnel.class);

  private final NodeEndpoint ep;
  private final FabricCommandRunner manager;

  public CoordTunnel(NodeEndpoint ep, FabricCommandRunner manager) {
    super();
    this.ep = ep;
    this.manager = manager;
  }

  public static class RequestProfile extends FutureBitCommand<QueryProfile, ProxyConnection> {
    final ExternalId queryId;

    public RequestProfile(ExternalId queryId) {
      super();
      this.queryId = queryId;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<QueryProfile> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_QUERY_PROFILE, queryId, QueryProfile.class);
    }
  }

  public static class CancelQuery extends FutureBitCommand<Ack, ProxyConnection> {
    final ExternalId queryId;

    public CancelQuery(ExternalId queryId) {
      super();
      this.queryId = queryId;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<Ack> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.REQ_QUERY_CANCEL, queryId, Ack.class);
    }
  }

  public RpcFuture<Ack> requestCancelQuery(ExternalId queryId){
    CancelQuery c = new CancelQuery(queryId);
    manager.runCommand(c);
    return c.getFuture();
  }

  public RpcFuture<QueryProfile> requestQueryProfile(ExternalId queryId) {
    RequestProfile b = new RequestProfile(queryId);
    manager.runCommand(b);
    return b.getFuture();
  }
}
