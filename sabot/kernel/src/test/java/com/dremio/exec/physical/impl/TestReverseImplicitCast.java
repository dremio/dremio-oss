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
package com.dremio.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.vector.ValueVector;
import org.junit.Test;

public class TestReverseImplicitCast extends PopUnitTestBase {

  @Test
  public void twoWayCast() throws Exception {

    // Function checks for casting from Float, Double to Decimal data types
    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        SabotNode bit =
            new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
        DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {
      // run query.
      bit.run();
      client.connect(
          new Properties() {
            {
              put("user", "anonymous");
            }
          });
      List<QueryDataBatch> results =
          client.runQuery(
              com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              readResourceAsString("/functions/cast/two_way_implicit_cast.json"));

      RecordBatchLoader batchLoader = new RecordBatchLoader(getTestAllocator());

      QueryDataBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      ValueVector intValueVector = itr.next().getValueVector();
      ValueVector varcharValueVector = itr.next().getValueVector();

      for (int i = 0; i < intValueVector.getValueCount(); i++) {
        System.out.println(intValueVector.getObject(i));
        assertEquals(intValueVector.getObject(i), 10);
        System.out.println(varcharValueVector.getObject(i));
        assertEquals(varcharValueVector.getObject(i).toString(), "101");
      }

      batchLoader.clear();
      for (QueryDataBatch result : results) {
        result.release();
      }
    }
  }
}
