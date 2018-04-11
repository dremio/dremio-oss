/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("DX-3872")
public class TestNewAggregateFunctions extends PopUnitTestBase {

  public void runTest(String physicalPlan, String inputDataFile,
      Object[] expected) throws Exception {
    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
         DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG,
            clusterCoordinator)) {

      // run query.
      bit.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(
          QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile(physicalPlan),
              Charsets.UTF_8).replace("#{TEST_FILE}",
              inputDataFile));

      try(RecordBatchLoader batchLoader = new RecordBatchLoader(bit
          .getContext().getAllocator())) {

        QueryDataBatch batch = results.get(1);
        assertTrue(batchLoader.load(batch.getHeader().getDef(),
            batch.getData()));

        int i = 0;
        for (VectorWrapper<?> v : batchLoader) {
          ValueVector vv = v.getValueVector();
          System.out.println((vv.getObject(0)));
          assertEquals(expected[i++], (vv.getObject(0)));
        }
      }
      for (QueryDataBatch b : results) {
        b.release();
      }
    }
  }

  @Test
  public void testBitwiseAggrFuncs() throws Exception {
    String physicalPlan = "/functions/test_logical_aggr.json";
    String inputDataFile = "/logical_aggr_input.json";
    Object[] expected = {0L, 4L, 4L, 7L, -2L, 1L, true, false};

    runTest(physicalPlan, inputDataFile, expected);

  }

}
