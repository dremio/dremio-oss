/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.proto.ExecRPC.FinishedReceiver;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.threads.SendingMonitor;

/**
 * Wrapper around a {@link com.dremio.sabot.exec.rpc.ExecTunnel} that tracks the status of batches sent to
 * to other SabotNodes.
 */
public class AccountingExecTunnel {
  private final ExecTunnel tunnel;
  private final SendingMonitor monitor;
  private final RpcOutcomeListener<Ack> statusHandler;

  public AccountingExecTunnel(ExecTunnel tunnel, SendingMonitor monitor, RpcOutcomeListener<Ack> statusHandler) {
    this.tunnel = tunnel;
    this.monitor = monitor;
    this.statusHandler = statusHandler;
  }

  public void sendStreamComplete(FragmentStreamComplete streamComplete) {
    monitor.increment();
    tunnel.sendStreamComplete(statusHandler, streamComplete);
  }

  public void sendRecordBatch(FragmentWritableBatch batch) {
    monitor.increment();
    tunnel.sendRecordBatch(statusHandler, batch);
  }

  public void informReceiverFinished(FinishedReceiver finishedReceiver) {
    monitor.increment();
    tunnel.informReceiverFinished(statusHandler, finishedReceiver);
  }

}
