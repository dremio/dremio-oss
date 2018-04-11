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
package com.dremio.service.accelerator;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;

/**
 * To access ReflectionService from executor(client)
 */
class ReflectionTunnel {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionTunnel.class);

  private final CoordinationProtos.NodeEndpoint ep;
  private final FabricCommandRunner manager;

  public ReflectionTunnel(CoordinationProtos.NodeEndpoint ep, FabricCommandRunner manager) {
    super();
    this.ep = ep;
    this.manager = manager;
  }

  /**
   * To get ReflectionCombinedStatus
   */
  public static class RequestReflectionInfo extends FutureBitCommand<ReflectionRPC.ReflectionInfoResp,
    ProxyConnection> {
    private final ReflectionRPC.ReflectionInfoReq reflectionStatusRequest;

    public RequestReflectionInfo(ReflectionRPC.ReflectionInfoReq reflectionStatusRequest) {
      super();
      this.reflectionStatusRequest = reflectionStatusRequest;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<ReflectionRPC.ReflectionInfoResp> outcomeListener, ProxyConnection
      connection) {
      connection.send(outcomeListener, ReflectionRPC.RpcType.REQ_REFLECTION_INFO, reflectionStatusRequest, ReflectionRPC
        .ReflectionInfoResp.class);
    }
  }

  public static class RequestRefreshInfos extends FutureBitCommand<ReflectionRPC.RefreshInfoResp, ProxyConnection> {
    private final ReflectionRPC.RefreshInfoReq refreshInfoRequest;

    public RequestRefreshInfos(ReflectionRPC.RefreshInfoReq refreshInfoRequest) {
      super();
      this.refreshInfoRequest = refreshInfoRequest;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<ReflectionRPC.RefreshInfoResp> outcomeListener, ProxyConnection
      connection) {
      connection.send(outcomeListener, ReflectionRPC.RpcType.REQ_REFRESH_INFO, refreshInfoRequest, ReflectionRPC
        .RefreshInfoResp.class);
    }
  }
  public RpcFuture<ReflectionRPC.ReflectionInfoResp> requestReflectionStatus() {
    ReflectionRPC.ReflectionInfoReq reflectionStatusRequest = ReflectionRPC.ReflectionInfoReq.newBuilder().build();
    ReflectionTunnel.RequestReflectionInfo b = new ReflectionTunnel.RequestReflectionInfo(reflectionStatusRequest);
    manager.runCommand(b);
    return b.getFuture();
  }

  public RpcFuture<ReflectionRPC.RefreshInfoResp> requestRefreshInfos() {
    ReflectionRPC.RefreshInfoReq refreshInfosRequest = ReflectionRPC.RefreshInfoReq.newBuilder().build();
    ReflectionTunnel.RequestRefreshInfos b = new ReflectionTunnel.RequestRefreshInfos(refreshInfosRequest);
    manager.runCommand(b);
    return b.getFuture();
  }

}
