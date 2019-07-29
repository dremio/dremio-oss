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

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("DX-3872")
public class TestUnionExchange extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestUnionExchange.class);

  @Test
  public void twoBitTwoExchangeTwoEntryRun() throws Exception {
    try (ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
         SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, false);
         DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)) {

      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
          Files.toString(FileUtils.getResourceAsFile("/sender/union_exchange.json"),
              Charsets.UTF_8));
      int count = 0;
      for (QueryDataBatch b : results) {
        if (b.getHeader().getRowCount() != 0) {
          count += b.getHeader().getRowCount();
        }
        b.release();
      }
      assertEquals(150, count);
    }
  }

}
