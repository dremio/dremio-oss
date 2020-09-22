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
package com.dremio.exec.work;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.dremio.common.DeferredException;
import com.dremio.exec.proto.CoordExecRPC.NodeStatResp;
import com.dremio.exec.store.sys.NodeInstance;

import io.grpc.stub.StreamObserver;

/**
 * Allows a set of node statistic responses to be collected and considered upon completion.
 */
public class NodeStatsListener implements StreamObserver<NodeStatResp> {
  private final CountDownLatch latch;

  private final DeferredException ex;
  private final ConcurrentHashMap<String, NodeInstance> stats;

  public NodeStatsListener(int numEndPoints) {
    this.latch = new CountDownLatch(numEndPoints);
    this.ex = new DeferredException();
    this.stats = new ConcurrentHashMap<>();
  }

  public void waitForFinish() throws Exception {
    latch.await();
    ex.close();
  }

  public ConcurrentHashMap<String, NodeInstance> getResult() {
    return stats;
  }

  @Override
  public void onNext(NodeStatResp nodeStatResp) {
    stats.put(nodeStatResp.getNodeStats().getName() + ":" + nodeStatResp.getNodeStats().getPort(),
      NodeInstance.fromStats(nodeStatResp.getNodeStats(), nodeStatResp.getEndpoint()));
  }

  @Override
  public void onError(Throwable throwable) {
    ex.addException((Exception) throwable);
    latch.countDown();
  }

  @Override
  public void onCompleted() {
    latch.countDown();
  }
}
