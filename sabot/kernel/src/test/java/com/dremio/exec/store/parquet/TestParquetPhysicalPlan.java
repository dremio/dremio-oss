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
package com.dremio.exec.store.parquet;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Stopwatch;

public class TestParquetPhysicalPlan extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetPhysicalPlan.class);

  public String fileName = "parquet/parquet_scan_filter_union_screen_physical.json";

  @Test
  @Ignore
  public void testParseParquetPhysicalPlan() throws Exception {
    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true); DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL, readResourceAsString(fileName));
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      int count = 0;
      for (QueryDataBatch b : results) {
        System.out.println(String.format("Got %d results", b.getHeader().getRowCount()));
        count += b.getHeader().getRowCount();
        loader.load(b.getHeader().getDef(), b.getData());
        for (VectorWrapper vw : loader) {
          System.out.print(vw.getValueVector().getField().getName() + ": ");
          ValueVector vv = vw.getValueVector();
          for (int i = 0; i < vv.getValueCount(); i++) {
            Object o = vv.getObject(i);
            if (o instanceof byte[]) {
              System.out.print(" [" + new String((byte[]) o) + "]");
            } else {
              System.out.print(" [" + vv.getObject(i) + "]");
            }
//            break;
          }
          System.out.println();
        }
        loader.clear();
        b.release();
      }
      client.close();
      System.out.println(String.format("Got %d total results", count));
    }
  }

  private class ParquetResultsListener implements UserResultsListener {
    AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void submissionFailed(UserException ex) {
      logger.error("submission failed", ex);
      latch.countDown();
    }

    @Override
    public void queryCompleted(QueryState state) {
      latch.countDown();
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      System.out.println(String.format("Result batch arrived. Number of records: %d", rows));
      count.addAndGet(rows);
      result.release();
    }

    public int await() throws Exception {
      latch.await();
      return count.get();
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
    }
  }

  @Test
  @Ignore
  public void testParseParquetPhysicalPlanRemote() throws Exception {
    try(DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG);) {
      client.connect();
      ParquetResultsListener listener = new ParquetResultsListener();
      Stopwatch watch = Stopwatch.createStarted();
      client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL, readResourceAsString(fileName), listener);
      System.out.println(String.format("Got %d total records in %d seconds", listener.await(), watch.elapsed(TimeUnit.SECONDS)));
      client.close();
    }
  }

}
