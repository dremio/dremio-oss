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

import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.google.common.base.Preconditions;

/**
 * API for communication between executors.
 */
public interface ExecTunnel {
  void sendStreamComplete(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, ExecRPC.FragmentStreamComplete streamComplete);

  void sendRecordBatch(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, FragmentWritableBatch batch);

  void sendOOBMessage(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, OutOfBandMessage message);

  void informReceiverFinished(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, ExecRPC.FinishedReceiver finishedReceiver);

  static void checkFragmentHandle(ExecProtos.FragmentHandle handle) {
    Preconditions.checkState(handle.hasQueryId(), "must set query id");
    Preconditions.checkState(handle.hasMajorFragmentId(), "must set major fragment id");
    Preconditions.checkState(handle.hasMinorFragmentId(), "must set minor fragment id");
  }

}
