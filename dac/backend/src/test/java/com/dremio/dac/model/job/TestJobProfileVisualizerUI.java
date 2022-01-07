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

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.PhaseData;

public class TestJobProfileVisualizerUI extends BaseTestServer {

  @Test
  public void testGetJobProfileInfo() throws Exception {
    UserBitShared.QueryProfile profile = getTestProfile();
    JobProfileVisualizerUI jobProfileVisualizerUI = new JobProfileVisualizerUI(profile);
    List<PhaseData> phaseDataList = jobProfileVisualizerUI.getJobProfileInfo();

    Assert.assertEquals("00", phaseDataList.get(0).getPhaseId());
  }
}
