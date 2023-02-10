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

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.pop.PopUnitTestBase;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.server.SabotNode;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;

@Ignore("DX-3872")
public class TestDistributedFragmentRun extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDistributedFragmentRun.class);

  @Test
  public void oneBitOneExchangeOneEntryRun() throws Exception{
    try(ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
        DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)){
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, readResourceAsString("/physical_single_exchange.json"));
      int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(100, count);
    }


  }


  @Test
  public void oneBitOneExchangeTwoEntryRun() throws Exception{
    try(ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
        DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)){
      bit1.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, readResourceAsString("/physical_single_exchange_double_entry.json"));
      int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(200, count);
    }


  }

  @Test
  public void twoBitOneExchangeTwoEntryRun() throws Exception{
    try(ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        SabotNode bit1 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true);
        SabotNode bit2 = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, false);
        DremioClient client = new DremioClient(DEFAULT_SABOT_CONFIG, clusterCoordinator)){
      bit1.run();
      bit2.run();
      client.connect();
      List<QueryDataBatch> results = client.runQuery(QueryType.PHYSICAL, readResourceAsString("/physical_single_exchange_double_entry.json"));
      int count = 0;
      for(QueryDataBatch b : results){
        count += b.getHeader().getRowCount();
        b.release();
      }
      assertEquals(200, count);
    }


  }
}
