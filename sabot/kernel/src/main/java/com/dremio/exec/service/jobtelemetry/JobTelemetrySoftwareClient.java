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
package com.dremio.exec.service.jobtelemetry;

import java.util.concurrent.ForkJoinPool;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClient;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

/**
 * Software version of the job telemetry client. Uses fabric to communicate to the
 * in-process service running on coordinator node(s).
 */
public class JobTelemetrySoftwareClient implements JobTelemetryExecutorClient {
  private final ExecToCoordTunnel tunnel;

  public JobTelemetrySoftwareClient(ExecToCoordTunnel tunnel) {
    this.tunnel = tunnel;
  }

  @Override
  public ListenableFuture<Empty> putExecutorProfile(CoordExecRPC.ExecutorQueryProfile profile) {
    return Futures.transform(tunnel.sendNodeQueryProfile(profile),
      ack -> Empty.getDefaultInstance(),
      ForkJoinPool.commonPool());
  }
}
