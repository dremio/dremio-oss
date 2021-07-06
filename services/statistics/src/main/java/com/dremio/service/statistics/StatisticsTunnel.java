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
package com.dremio.service.statistics;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.StatisticsRPC;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;

/**
 * To access StatisticsService from executor(client)
 */
class StatisticsTunnel {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsTunnel.class);

  private final CoordinationProtos.NodeEndpoint ep;
  private final FabricCommandRunner manager;

  public StatisticsTunnel(CoordinationProtos.NodeEndpoint ep, FabricCommandRunner manager) {
    super();
    this.ep = ep;
    this.manager = manager;
  }

  /**
   * To get RequestStatisticsInfo
   */
  public static class RequestStatisticsInfo extends FutureBitCommand<StatisticsRPC.StatisticsInfoResp,
    ProxyConnection> {
    private final StatisticsRPC.StatisticsInfoReq statisticsInfoRequest;

    public RequestStatisticsInfo(StatisticsRPC.StatisticsInfoReq statisticsInfoRequest) {
      super();
      this.statisticsInfoRequest = statisticsInfoRequest;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<StatisticsRPC.StatisticsInfoResp> outcomeListener, ProxyConnection
      connection) {
      connection.send(outcomeListener, StatisticsRPC.RpcType.REQ_STATISTICS_INFO, statisticsInfoRequest, StatisticsRPC
        .StatisticsInfoResp.class);
    }
  }

  public RpcFuture<StatisticsRPC.StatisticsInfoResp> requestStatisticsInfos() {
    StatisticsRPC.StatisticsInfoReq statisticsInfoRequest = StatisticsRPC.StatisticsInfoReq.newBuilder().build();
    StatisticsTunnel.RequestStatisticsInfo b = new StatisticsTunnel.RequestStatisticsInfo(statisticsInfoRequest);
    manager.runCommand(b);
    return b.getFuture();
  }
}
