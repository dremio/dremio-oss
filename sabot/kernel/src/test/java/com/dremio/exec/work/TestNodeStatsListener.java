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

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class TestNodeStatsListener {

  @Test
  public void testConcurrentUpdates() throws Exception {
    ExecutorService threadPool = Executors.newFixedThreadPool(3);

    CoordExecRPC.NodeStatResp[] responses = {
      getNodeStatsResponse("test-1"), getNodeStatsResponse("test-2"), getNodeStatsResponse("test-3")
    };

    NodeStatsListener nodeStatsListener = new NodeStatsListener(3);
    Random r = new Random();
    for (int j = 0; j < 3; j++) {
      final int counter = j;
      threadPool.submit(
          () -> {
            nodeStatsListener.onNext(responses[counter]);
            try {
              Thread.sleep(r.nextInt(100));
            } catch (InterruptedException e) {
              // ignore;
            }
            nodeStatsListener.onCompleted();
          });
    }
    nodeStatsListener.waitForFinish();
    int responseSize = nodeStatsListener.getResult().size();
    Assert.assertTrue("3 response should have been recorded", responseSize == 3);
  }

  @NotNull
  private CoordExecRPC.NodeStatResp getNodeStatsResponse(String s) {
    return CoordExecRPC.NodeStatResp.newBuilder()
        .setNodeStats(CoordExecRPC.NodeStats.newBuilder().setName(s).setPort(1234).build())
        .setEndpoint(CoordinationProtos.NodeEndpoint.getDefaultInstance())
        .build();
  }

  @Test(expected = TimeoutException.class)
  public void testTimeout() throws Exception {
    CoordExecRPC.NodeStatResp[] responses = {getNodeStatsResponse("test-1")};
    NodeStatsListener nodeStatsListener = new NodeStatsListener(2);
    nodeStatsListener.onNext(responses[0]);
    nodeStatsListener.onCompleted();
    nodeStatsListener.waitForFinish();
  }
}
