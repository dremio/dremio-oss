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
package com.dremio.dac.model.job;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.service.jobAnalysis.proto.Node;
import com.dremio.service.jobAnalysis.proto.PhaseData;

public class TestJobProfileVisualizerUI extends BaseTestServer {

  @Test
  public void testGetNodeDetails() throws Exception {
    List<PhaseData> testPhaseList;
    JobProfileVisualizerUI details = new JobProfileVisualizerUI();
    testPhaseList = details.getPhaseDetail(getTestProfile());

    Assert.assertEquals("00", testPhaseList.get(0).getPhaseId());
    Assert.assertEquals("SCREEN", testPhaseList.stream().flatMap(c -> c.getOperatorDataList().getOperatorDataList().stream())
      .collect(Collectors.groupingBy(c -> c.getNodeId(), Collectors.toList())).get("00").get(0).getOperatorName());
    Assert.assertEquals(7, testPhaseList.stream().flatMap(c -> c.getOperatorDataList().getOperatorDataList().stream())
      .collect(Collectors.groupingBy(c -> c.getNodeId(), Collectors.toList())).size());
  }

  @Test
  public void testBytesProcessedSummary() throws Exception {
    List<Node> testNodeList;
    JobProfileVisualizerUI phasedetails = new JobProfileVisualizerUI();
    QueryProfileUtil details = new QueryProfileUtil(getTestProfile());
    testNodeList = details.getNodeDetails();

    Map<String, LongSummaryStatistics> byteprocessSummary = phasedetails.bytesProcessedSummary(testNodeList);
    Assert.assertEquals(7, byteprocessSummary.get("00").getCount());
    Assert.assertEquals(0, byteprocessSummary.get("00").getMin());
  }
}
