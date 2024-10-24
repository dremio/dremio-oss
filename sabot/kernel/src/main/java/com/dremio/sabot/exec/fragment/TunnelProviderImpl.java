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

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.exec.FragmentWorkManager.ExecConnectionCreator;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.exec.rpc.AccountingExecToCoordTunnel;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.AccountingFileTunnel;
import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.exec.rpc.FileTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.threads.SendingAccountor;
import com.dremio.sabot.threads.SendingMonitor;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.dremio.services.jobresults.common.JobResultsTunnel;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;

/** Provides tunnels to an execution pipeline. */
public class TunnelProviderImpl implements TunnelProvider {

  private final Map<NodeEndpoint, AccountingExecTunnel> tunnels = Maps.newHashMap();

  private final SendingAccountor accountor;
  private final AccountingExecToCoordTunnel coordTunnel;
  private final ExecConnectionCreator connectionCreator;
  private final SharedResourceGroup resourceGroup;
  private final RpcOutcomeListener<Ack> statusHandler;
  private final FileCursorManagerFactory cursorManagerFactory;
  private final int outstandingRPCsPerTunnel;

  public TunnelProviderImpl(
      SendingAccountor accountor,
      JobResultsTunnel tunnel,
      ExecConnectionCreator connectionCreator,
      RpcOutcomeListener<Ack> statusHandler,
      SharedResourceGroup resourceGroup,
      FileCursorManagerFactory cursorManagerFactory,
      int outstandingRPCsPerTunnel) {
    super();
    this.accountor = accountor;
    this.statusHandler = statusHandler;
    final SharedResource resource =
        resourceGroup.createResource("user", SharedResourceType.SEND_MSG_COORDINATOR);
    final SendingMonitor monitor =
        new SendingMonitor(resource, accountor, outstandingRPCsPerTunnel);
    this.coordTunnel =
        new AccountingExecToCoordTunnel(tunnel, monitor, monitor.wrap(statusHandler));

    this.connectionCreator = connectionCreator;
    this.resourceGroup = resourceGroup;
    this.cursorManagerFactory = cursorManagerFactory;
    this.outstandingRPCsPerTunnel = outstandingRPCsPerTunnel;
  }

  @Override
  public AccountingExecToCoordTunnel getCoordTunnel() {
    return coordTunnel;
  }

  @Override
  public AccountingExecTunnel getExecTunnel(final NodeEndpoint endpoint) {
    AccountingExecTunnel tunnel = tunnels.get(endpoint);
    if (tunnel == null) {
      final SharedResource resource =
          resourceGroup.createResource(
              "send-data-" + endpoint.getAddress(), SharedResourceType.SEND_MSG_DATA);
      SendingMonitor monitor = new SendingMonitor(resource, accountor, outstandingRPCsPerTunnel);
      tunnel =
          new AccountingExecTunnel(
              connectionCreator.getTunnel(endpoint), monitor, monitor.wrap(statusHandler));
      tunnels.put(endpoint, tunnel);
    }
    return tunnel;
  }

  @Override
  public AccountingExecTunnel getExecTunnelDlr(
      final NodeEndpoint endpoint, RpcOutcomeListener<Ack> handler) {
    AccountingExecTunnel tunnel = null;
    if (tunnel == null) {
      final SharedResource resource =
          resourceGroup.createResource(
              "send-data-dlr-" + endpoint.getAddress(), SharedResourceType.SEND_MSG_DATA);
      SendingMonitor monitor = new SendingMonitor(resource, accountor, outstandingRPCsPerTunnel);
      tunnel =
          new AccountingExecTunnel(
              connectionCreator.getTunnel(endpoint), monitor, monitor.wrap(handler));
    }
    return tunnel;
  }

  @Override
  public AccountingFileTunnel getFileTunnel(FileStreamManager streamManager, int maxBatchesPerFile)
      throws IOException {
    final SharedResource resource =
        resourceGroup.createResource(
            "writer-file-" + streamManager.getId(), SharedResourceType.SEND_MSG_DATA);
    final FileTunnel fileTunnel = new FileTunnel(streamManager, maxBatchesPerFile);
    return new AccountingFileTunnel(fileTunnel, cursorManagerFactory, resource);
  }
}
