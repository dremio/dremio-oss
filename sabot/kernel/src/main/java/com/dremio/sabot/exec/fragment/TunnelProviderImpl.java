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
package com.dremio.sabot.exec.fragment;

import java.util.Map;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.exec.FragmentWorkManager.ExecConnectionCreator;
import com.dremio.sabot.exec.rpc.AccountingExecToCoordTunnel;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.SendingAccountor;
import com.dremio.sabot.threads.SendingMonitor;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.dremio.services.jobresults.common.JobResultsTunnel;
import com.google.common.collect.Maps;

/**
 * Provides tunnels to an execution pipeline.
 */
class TunnelProviderImpl implements TunnelProvider {

  private final Map<NodeEndpoint, AccountingExecTunnel> tunnels = Maps.newHashMap();

  private final SendingAccountor accountor;
  private final AccountingExecToCoordTunnel coordTunnel;
  private final ExecConnectionCreator connectionCreator;
  private final SharedResourceGroup resourceGroup;
  private final RpcOutcomeListener<Ack> statusHandler;

  public TunnelProviderImpl(
      SendingAccountor accountor,
      JobResultsTunnel tunnel,
      ExecConnectionCreator connectionCreator,
      RpcOutcomeListener<Ack> statusHandler,
      SharedResourceGroup resourceGroup) {
    super();
    this.accountor = accountor;
    this.statusHandler = statusHandler;
    final SharedResource resource = resourceGroup.createResource("user", SharedResourceType.SEND_MSG_COORDINATOR);
    final SendingMonitor monitor = new SendingMonitor(resource, accountor);
    this.coordTunnel = new AccountingExecToCoordTunnel(tunnel, monitor, monitor.wrap(statusHandler));

    this.connectionCreator = connectionCreator;
    this.resourceGroup = resourceGroup;
  }

  @Override
  public AccountingExecToCoordTunnel getCoordTunnel() {
    return coordTunnel;
  }

  public AccountingExecTunnel getExecTunnel(final NodeEndpoint endpoint) {
    AccountingExecTunnel tunnel = tunnels.get(endpoint);
    if (tunnel == null) {
      final SharedResource resource = resourceGroup.createResource("send-data-" + endpoint.getAddress(), SharedResourceType.SEND_MSG_DATA);
      SendingMonitor monitor = new SendingMonitor(resource, accountor);
      tunnel = new AccountingExecTunnel(connectionCreator.getTunnel(endpoint), monitor, monitor.wrap(statusHandler));
      tunnels.put(endpoint, tunnel);
    }
    return tunnel;
  }
}
