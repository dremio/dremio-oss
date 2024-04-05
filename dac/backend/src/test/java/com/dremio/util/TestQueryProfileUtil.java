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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.ThreadData;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class TestQueryProfileUtil extends BaseTestServer {

  UserBitShared.QueryProfile testProfile;
  UserBitShared.MajorFragmentProfile majorFragmentProfile;
  List<ThreadData> threadLevelMetricsList = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    testProfile = getTestProfile();
    majorFragmentProfile = testProfile.getFragmentProfileList().get(0);
  }

  @Test
  public void testGetStringIds() {
    assertEquals("01", QueryProfileUtil.getStringIds(01));
    assertEquals("01", QueryProfileUtil.getStringIds(1));
    assertEquals("201", QueryProfileUtil.getStringIds(201));
  }

  @Test
  public void testBuildBaseMetrics() {
    BaseMetrics baseMetrics = new BaseMetrics();
    QueryProfileUtil.buildTheadLevelMetrics(majorFragmentProfile, threadLevelMetricsList);
    QueryProfileUtil.buildBaseMetrics(threadLevelMetricsList, baseMetrics); // build BaseMetrics

    assertEquals(-1, baseMetrics.getNumThreads().intValue());
    assertEquals(5, baseMetrics.getFieldNumber("recordsProcessed"));
    assertEquals("setupTime", baseMetrics.getFieldName(4));
    assertTrue("ProcessingTime", baseMetrics.getProcessingTime() >= 0);
    assertEquals(17, baseMetrics.getRecordsProcessed().intValue());
    assertTrue("PeakMemory", baseMetrics.getPeakMemory() >= 0);
    assertTrue("IoWaitTime", baseMetrics.getIoWaitTime() >= 0);
    assertTrue("SetupTime", baseMetrics.getSetupTime() >= 0);
    assertEquals(null, baseMetrics.getBytesProcessed());
  }

  @Test
  public void testBuildTheadLevelMetrics() {
    QueryProfileUtil.buildTheadLevelMetrics(majorFragmentProfile, threadLevelMetricsList);

    assertEquals(9, threadLevelMetricsList.size());
    assertEquals(10, threadLevelMetricsList.get(1).getOperatorType().intValue());
    assertEquals(2, threadLevelMetricsList.get(1).getFieldNumber("operatorName"));
    assertEquals("ioWaitTime", threadLevelMetricsList.get(1).getFieldName(4));
    assertEquals("01", threadLevelMetricsList.get(1).getOperatorId());
    assertEquals(1, threadLevelMetricsList.get(1).getRecordsProcessed().intValue());
    assertTrue("ProcessingTime", threadLevelMetricsList.get(1).getProcessingTime() >= 0);
    assertTrue("IoWaitTime", threadLevelMetricsList.get(1).getIoWaitTime() >= 0);
    assertTrue("tPeakMemory", threadLevelMetricsList.get(1).getPeakMemory() >= 0);
    assertTrue("SetupTime", threadLevelMetricsList.get(1).getSetupTime() >= 0);
  }
}
