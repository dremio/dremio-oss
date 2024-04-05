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
package com.dremio.dac.service.datasets;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDACViewCreatorFactory extends BaseTestServer {

  @BeforeClass
  public static void before() throws Exception {
    populateInitialData();

    NamespaceService namespaceService = newNamespaceService();

    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("Spacefoo");

    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceConfig.getName()), spaceConfig);

    namespaceService.addOrUpdateFolder(
        new FolderPath("Spacefoo.FolderBar").toNamespaceKey(),
        new FolderConfig().setName("FolderBar").setFullPathList(asList("Spacefoo", "FolderBar")));
  }

  @AfterClass
  public static void after() throws Exception {
    NamespaceService namespaceService = newNamespaceService();
    NamespaceKey namespaceKey = new NamespaceKey("Spacefoo");
    namespaceService.deleteSpace(namespaceKey, namespaceService.getSpace(namespaceKey).getTag());
  }

  @Test
  public void testCreateViewKeepsFolderCase() throws Exception {
    ViewCreatorFactory vcf = l(ViewCreatorFactory.class);
    List<String> path = Arrays.asList("SpaceFOO", "FolderBAR", "testView");
    NamespaceKey namespaceKey = new NamespaceKey(path);

    vcf.get(DEFAULT_USERNAME).createView(path, "select 1", Collections.emptyList(), false);

    DatasetConfig dataset = newNamespaceService().getDataset(namespaceKey);
    assertEquals(Arrays.asList("Spacefoo", "FolderBar", "testView"), dataset.getFullPathList());
  }

  @Test
  public void testUpdateViewKeepsDatasetCase() throws Exception {
    ViewCreatorFactory vcf = l(ViewCreatorFactory.class);
    List<String> path = Arrays.asList("Spacefoo", "FolderBar", "testView1");
    NamespaceKey namespaceKey = new NamespaceKey(path);

    vcf.get(DEFAULT_USERNAME).createView(path, "select 1", Collections.emptyList(), false);

    path = Arrays.asList("SPACEFOO", "FolderBAR", "TESTVIEW1");
    namespaceKey = new NamespaceKey(path);
    vcf.get(DEFAULT_USERNAME).updateView(path, "select 2", Collections.emptyList());

    DatasetConfig dataset = newNamespaceService().getDataset(namespaceKey);
    assertEquals(Arrays.asList("Spacefoo", "FolderBar", "testView1"), dataset.getFullPathList());
  }
}
