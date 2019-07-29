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
package com.dremio.dac.explore.bi;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;

/**
 * Smoke test for {@code QlikResource}
 */
public class TestQlikResource extends BaseTestServer {

  @Before
  public void setUp() throws Exception {
    BaseTestServer.populateInitialData();
  }

  @After
  public void tearDown() throws Exception {
    BaseTestServer.clearAllDataExceptUser();
  }

  @Test
  public void testQlikExportVirtualDataset() throws Exception {
    expectSuccess(getBuilder(getAPIv2().path("qlik/Prod-Sample.ds1")).accept("text/plain+qlik-app").buildGet());
  }

  @Test
  public void testQlikExportPhysicalDataset() throws Exception {
    // This dataset has already been queried and schema should be available
    expectSuccess(getBuilder(getAPIv2().path("qlik/LocalFS1.\"dac-sample1.json\"")).accept("text/plain+qlik-app").buildGet());
  }

  @Test
  public void testQlikExportAllTypesPhysicalDataset() throws Exception {
    expectSuccess(getBuilder(getAPIv2().path("dataset/LocalFS1.\"all_types_dremio.json\"/preview")).buildGet());
    expectSuccess(getBuilder(getAPIv2().path("qlik/LocalFS1.\"all_types_dremio.json\"")).accept("text/plain+qlik-app").buildGet());
  }
}
