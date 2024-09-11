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
package com.dremio.dac.cmd;

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.DATASET;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

/** Test for Clean command */
public class TestCleanDatasetOrphans extends CleanBaseTest {

  private static final List<String> TABLE_IN_HOME = Arrays.asList("@user1", "test-table");
  private static final String SPACE = "space";
  private static final List<String> TABLE_IN_SPACE = Arrays.asList("space", "test-table");
  private static final String SOURCE1 = "test-source1";
  private static final List<String> TABLE1 = Arrays.asList("test-source1", "test-table1");
  private static final String SOURCE2 = "test-source2";
  private static final List<String> TABLE2 = Arrays.asList("test-source2", "test-table2");

  @Test
  public void testCleanDatasetOrphans() throws Exception {
    getCurrentDremioDaemon().close();

    Optional<LocalKVStoreProvider> providerOptional =
        CmdUtils.getKVStoreProvider(getDACConfig().getConfig());
    try (LocalKVStoreProvider provider = providerOptional.get()) {
      provider.start();
      NamespaceService namespaceService =
          new NamespaceServiceImpl(provider, new CatalogStatusEventsImpl());

      // Add a dataset to home space
      final NamespaceKey homeKey = new HomePath(HomeName.getUserHomePath("user1")).toNamespaceKey();
      final HomeConfig homeConfig = new HomeConfig().setOwner("user1");
      namespaceService.addOrUpdateHome(homeKey, homeConfig);

      NamespaceKey datasetPath1 = new NamespaceKey(TABLE_IN_HOME);
      final DatasetConfig datasetConfig1 = new DatasetConfig();
      datasetConfig1.setFullPathList(TABLE_IN_HOME);
      datasetConfig1.setName(datasetPath1.getName());
      datasetConfig1.setType(VIRTUAL_DATASET);
      VirtualDataset virtualDataset1 = new VirtualDataset();
      virtualDataset1.setSql("");
      datasetConfig1.setVirtualDataset(virtualDataset1);
      namespaceService.addOrUpdateDataset(datasetPath1, datasetConfig1);

      // Add a dataset to a space
      final NamespaceKey spaceKey = new NamespaceKey(SPACE);
      SpaceConfig spaceConfig = new SpaceConfig().setId(null).setName(SPACE).setDescription("");
      namespaceService.addOrUpdateSpace(spaceKey, spaceConfig);

      NamespaceKey datasetPath2 = new NamespaceKey(TABLE_IN_SPACE);
      final DatasetConfig datasetConfig2 = new DatasetConfig();
      datasetConfig2.setFullPathList(TABLE_IN_SPACE);
      datasetConfig2.setName(datasetPath1.getName());
      datasetConfig2.setType(VIRTUAL_DATASET);
      VirtualDataset virtualDataset2 = new VirtualDataset();
      virtualDataset2.setSql("");
      datasetConfig2.setVirtualDataset(virtualDataset2);
      namespaceService.addOrUpdateDataset(datasetPath2, datasetConfig2);

      // Add dataset test-table1 to source test-source1
      NamespaceKey sourceKey1 = new NamespaceKey(SOURCE1);
      SourceConfig sourceConfig1 = new SourceConfig().setName(SOURCE1).setCtime(100L);
      namespaceService.addOrUpdateSource(sourceKey1, sourceConfig1);

      NamespaceKey datasetPath3 = new NamespaceKey(TABLE1);
      final DatasetConfig datasetConfig3 = new DatasetConfig();
      datasetConfig3.setFullPathList(TABLE1);
      datasetConfig3.setName(datasetPath3.getName());
      datasetConfig3.setType(PHYSICAL_DATASET);
      final PhysicalDataset physicalDataset1 = new PhysicalDataset();
      datasetConfig3.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      datasetConfig3.setPhysicalDataset(physicalDataset1);
      namespaceService.tryCreatePhysicalDataset(datasetPath3, datasetConfig3);

      // Add dataset test-table2 to source test-source2
      NamespaceKey sourceKey2 = new NamespaceKey(SOURCE2);
      SourceConfig sourceConfig2 = new SourceConfig().setName(SOURCE2).setCtime(100L);
      namespaceService.addOrUpdateSource(sourceKey2, sourceConfig2);

      NamespaceKey datasetPath4 = new NamespaceKey(TABLE2);
      final DatasetConfig datasetConfig4 = new DatasetConfig();
      datasetConfig4.setFullPathList(TABLE2);
      datasetConfig4.setName(datasetPath3.getName());
      datasetConfig4.setType(PHYSICAL_DATASET);
      final PhysicalDataset physicalDataset2 = new PhysicalDataset();
      datasetConfig4.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      datasetConfig4.setPhysicalDataset(physicalDataset2);
      namespaceService.tryCreatePhysicalDataset(datasetPath4, datasetConfig4);

      // Delete source test-source1 without deleting children.  test-source1.test-table1 becomes an
      // orphan dataset
      namespaceService.deleteEntity(sourceKey1);

      int datasetsCount = getDatasetCount(provider);

      // Clean orphan dataset
      Clean.deleteDatasetOrphans(provider.asLegacy(), provider);

      int newDatasetsCount = getDatasetCount(provider);

      // Verify 1 and only one dataset is cleaned
      assertEquals(1, datasetsCount - newDatasetsCount);
    }
  }

  private static int getDatasetCount(LocalKVStoreProvider kvStoreProvider) {
    IndexedStore<String, NameSpaceContainer> namespace =
        kvStoreProvider.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);
    Iterable<Document<String, NameSpaceContainer>> containerEntries = namespace.find();

    int count = 0;
    for (final Document<String, NameSpaceContainer> entry : containerEntries) {
      final NameSpaceContainer container = entry.getValue();
      if (container.getType() == DATASET) {
        count++;
      }
    }

    return count;
  }
}
