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
package com.dremio.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.MetricValue;
import com.dremio.service.jobAnalysis.proto.Node;

public class TestQueryProfileUtil extends BaseTestServer {


  @Test
  public void testGetNodeDetails() throws Exception {
    List<Node> testNodeList;
    QueryProfileUtil details = new QueryProfileUtil(getTestProfile());
    testNodeList = details.getNodeDetails();

    Assert.assertEquals("01", testNodeList.get(0).getNodeId());
    Assert.assertEquals("00", testNodeList.get(0).getPhaseId());
    Assert.assertEquals(7, testNodeList.size());
  }

  @Test
  public void testGetNodeMetrics() throws Exception {
    QueryProfileUtil details = new QueryProfileUtil(getTestProfile());
    UserBitShared.MajorFragmentProfile phaseData = getTestProfile().getFragmentProfile(0);
    int operatorId = 0;
    UserBitShared.CoreOperatorTypeMetricsMap metricsMap = getTestProfile().getOperatorTypeMetricsMap();
    BaseMetrics baseMetrics = details.getNodeMetrics(phaseData, operatorId, metricsMap);
    Long memoryAllocated = baseMetrics.getPeakMemory();
    Long rowCount = baseMetrics.getRecordsProcessed();

    Assert.assertEquals(Long.valueOf(1L), rowCount);
    assertContains("ioWaitTime", baseMetrics.toString());
    assertContains("peakMemory", baseMetrics.toString());
    assertContains("processingTime", baseMetrics.toString());
    assertContains("setupTime",baseMetrics.toString());
    assertContains("bytesProcessed",baseMetrics.toString());
    assertContains("numThreads",baseMetrics.toString());
  }

  @Test
  public void testGetMetricsData() throws Exception {
    QueryProfileUtil details = new QueryProfileUtil(getTestProfile());
    UserBitShared.MajorFragmentProfile phaseData = getTestProfile().getFragmentProfile(0);
    UserBitShared.CoreOperatorTypeMetricsMap metricsMap = getTestProfile().getOperatorTypeMetricsMap();

    UserBitShared.OperatorProfile operatorData = phaseData.getMinorFragmentProfile(0).getOperatorProfile(0);
    List<MetricValue> metricsList;
    metricsList = details.getMetricsData(metricsMap, operatorData);

    int metricId = metricsList.get(0).getMetricId();
    String metricName = metricsList.get(0).getMetricName();
    Assert.assertEquals(0, metricId);
    Assert.assertEquals("BYTES_SENT", metricName);
  }
}
