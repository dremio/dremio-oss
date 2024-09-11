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

import com.dremio.dac.server.BaseTestServer;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Smoke test for {@code QlikResource} */
public class TestQlikResource extends BaseTestServer {

  @Before
  public void setUp() throws Exception {
    BaseTestServer.populateInitialData();

    final OptionManager optionManager = getOptionManager();

    optionManager.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, QlikResource.ALLOW_QLIK.getOptionName(), true));
    optionManager.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, QlikResource.CLIENT_TOOLS_QLIK.getOptionName(), true));
  }

  @After
  public void tearDown() throws Exception {
    BaseTestServer.clearAllDataExceptUser();
  }

  @Test
  public void testQlikExportVirtualDataset() throws Exception {
    final DatasetConfig ds =
        l(NamespaceService.class).getDataset(new NamespaceKey(Arrays.asList("Prod-Sample", "ds1")));
    expectSuccess(
        getBuilder(getAPIv2().path("qlik/" + String.join("/", ds.getFullPathList())))
            .accept("text/plain+qlik-app")
            .buildGet());
  }

  @Test
  public void testQlikExportPhysicalDataset() throws Exception {
    // This dataset has already been queried and schema should be available
    final DatasetConfig ds =
        l(NamespaceService.class)
            .getDataset(new NamespaceKey(Arrays.asList("LocalFS1", "dac-sample1.json")));
    expectSuccess(
        getBuilder(getAPIv2().path("qlik/" + String.join("/", ds.getFullPathList())))
            .accept("text/plain+qlik-app")
            .buildGet());
  }

  @Test
  public void testQlikExportAllTypesPhysicalDataset() throws Exception {
    expectSuccess(
        getBuilder(getAPIv2().path("dataset/LocalFS1.\"all_types_dremio.json\"/preview"))
            .buildGet());
    final DatasetConfig ds =
        l(NamespaceService.class)
            .getDataset(new NamespaceKey(Arrays.asList("LocalFS1", "all_types_dremio.json")));
    expectSuccess(
        getBuilder(getAPIv2().path("qlik/" + String.join("/", ds.getFullPathList())))
            .accept("text/plain+qlik-app")
            .buildGet());
  }
}
