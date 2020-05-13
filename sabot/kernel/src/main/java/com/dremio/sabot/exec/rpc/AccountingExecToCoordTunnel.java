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
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.threads.SendingMonitor;
import com.dremio.services.jobresults.common.JobResultsTunnel;

/**
 * Wrapper around a {@link com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnection} that tracks the status of batches
 * sent to User.
 */
public class AccountingExecToCoordTunnel {
  private final JobResultsTunnel tunnel;
  private final SendingMonitor sendMonitor;
  private final RpcOutcomeListener<Ack> statusHandler;

  public AccountingExecToCoordTunnel(JobResultsTunnel tunnel, SendingMonitor sendMonitor, RpcOutcomeListener<Ack> statusHandler) {
    this.tunnel = tunnel;
    this.sendMonitor = sendMonitor;
    this.statusHandler = statusHandler;
  }

  public void sendData(QueryWritableBatch data) {
    sendMonitor.increment();
    tunnel.sendData(statusHandler, data);
  }

  public JobResultsTunnel getTunnel() {
    return tunnel;
  }
}
