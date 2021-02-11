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
package com.dremio.sabot.exec;

import java.util.Optional;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.jobtelemetry.client.JobTelemetryExecutorClient;
import com.dremio.service.maestroservice.MaestroClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

/**
 * A QueryTracker which does not send any messages to Coordinator
 */
public class NoOpQueryTracker implements QueryTracker {

  @Override
  public boolean tryStart(QueryTicket ticket, CoordinationProtos.NodeEndpoint foreman, MaestroClient maestroClient, JobTelemetryExecutorClient telemetryClient) {
    return true;
  }

  @Override
  public boolean isStarted() {
    return false; // this is used to check if query is already started
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public Optional<ListenableFuture<Empty>> sendQueryProfile() {
    return Optional.empty();
  }

  @Override
  public boolean isExpired() {
    return true; // okay if evicted from LoadingCache since creation logic is simple
  }

  @Override
  public boolean isTerminal() {
    return true;
  }

  @Override
  public CoordinationProtos.NodeEndpoint getForeman() {
    return null;
  }

  @Override
  public long getQuerySentTime() {
    return 0;
  }

  @Override
  public void setQuerySentTime(long querySentTime) {
  }
}
