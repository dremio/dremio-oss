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
package com.dremio.exec.physical.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestReverseImplicitCast extends PopUnitTestBase {

  @Test
  public void twoWayCast() throws Exception {

    // Function checks for casting from Float, Double to Decimal data types
    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT);
         DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {
      // run query.
      bit.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/functions/cast/two_way_implicit_cast.json"), Charsets.UTF_8));

      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());

      QueryDataBatch batch = results.get(0);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      ValueVector.Accessor intAccessor1 = itr.next().getValueVector().getAccessor();
      ValueVector.Accessor varcharAccessor1 = itr.next().getValueVector().getAccessor();

      for (int i = 0; i < intAccessor1.getValueCount(); i++) {
        System.out.println(intAccessor1.getObject(i));
        assertEquals(intAccessor1.getObject(i), 10);
        System.out.println(varcharAccessor1.getObject(i));
        assertEquals(varcharAccessor1.getObject(i).toString(), "101");
      }

      batchLoader.clear();
      for (QueryDataBatch result : results) {
        result.release();
      }
    }
  }
}
