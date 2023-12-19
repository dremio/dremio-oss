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
package com.dremio.service.namespace;

import static java.util.Arrays.asList;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusSubscriber;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.Lists;

/**
 * Test class for NamespaceServiceImpl
 */
public class TestNamespaceServiceImpl extends DremioTest {
  private LocalKVStoreProvider kvStoreProvider;
  private LegacyKVStoreProvider legacyKVStoreProvider;
  private NamespaceServiceImpl ns;
  private CatalogStatusEvents catalogStatusEvents;
  private TestDatasetDeletionSubscriber testDatasetDeletionSubscriber;

  @Before
  public void setup() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    legacyKVStoreProvider = kvStoreProvider.asLegacy();
    testDatasetDeletionSubscriber = new TestDatasetDeletionSubscriber();
    catalogStatusEvents = new CatalogStatusEventsImpl();
    catalogStatusEvents.subscribe(DatasetDeletionCatalogStatusEvent.getEventTopic(), testDatasetDeletionSubscriber);
    ns = new NamespaceServiceImpl(
      legacyKVStoreProvider,
      catalogStatusEvents
    );
  }

  @After
  public void shutdown() throws Exception {
    legacyKVStoreProvider.close();
  }

  @Test
  public void testDeleteSourceFiresDatasetDeletionEvent() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("s1");
    ns.addOrUpdateSource(sourceKey, newTestSource("s1"));

    final NamespaceKey folderKey1 = new NamespaceKey(asList("s1", "fld1"));
    ns.addOrUpdateFolder(folderKey1, newTestFolder(sourceKey, "fld1"));

    final NamespaceKey dataset1 = new NamespaceKey(asList("s1", "fld1", "ds1"));
    ns.addOrUpdateDataset(dataset1, newTestPhysicalDataset(folderKey1, "ds1"));

    final NamespaceKey folderKey2 = new NamespaceKey(asList("s1", "fld1", "fld2"));
    ns.addOrUpdateFolder(folderKey2, newTestFolder(folderKey1, "fld2"));

    final NamespaceKey dataset2 = new NamespaceKey(asList("s1", "fld1", "fld2", "ds2"));
    ns.addOrUpdateDataset(dataset2, newTestPhysicalDataset(folderKey2, "ds2"));

    final SourceConfig sourceConfig = ns.getSource(sourceKey);
    ns.deleteSource(sourceKey, sourceConfig.getTag());

    Assert.assertEquals("There should be two dataset deletion events fired.", 2, testDatasetDeletionSubscriber.getCount());
  }

  @Test
  public void testDeleteSpaceFiresDatasetDeletionEvent() throws Exception {
    final NamespaceKey spaceKey = new NamespaceKey("s1");
    ns.addOrUpdateSpace(spaceKey, newTestSpace("s1"));

    final NamespaceKey folderKey1 = new NamespaceKey(asList("s1", "fld1"));
    ns.addOrUpdateFolder(folderKey1, newTestFolder(spaceKey, "fld1"));

    final NamespaceKey dataset1 = new NamespaceKey(asList("s1", "fld1", "ds1"));
    ns.addOrUpdateDataset(dataset1, newTestVirtualDataset(folderKey1, "ds1"));

    final NamespaceKey folderKey2 = new NamespaceKey(asList("s1", "fld1", "fld2"));
    ns.addOrUpdateFolder(folderKey2, newTestFolder(folderKey1, "fld2"));

    final NamespaceKey dataset2 = new NamespaceKey(asList("s1", "fld1", "fld2", "ds2"));
    ns.addOrUpdateDataset(dataset2, newTestVirtualDataset(folderKey2, "ds2"));

    final SpaceConfig sourceConfig = ns.getSpace(spaceKey);
    ns.deleteSpace(spaceKey, sourceConfig.getTag());

    Assert.assertEquals("There should be two dataset deletion events fired.", 2, testDatasetDeletionSubscriber.getCount());
  }

  private SourceConfig newTestSource(String sourceName) {
    return new SourceConfig()
      .setName(sourceName)
      .setType("NAS")
      .setCtime(1000L);
  }

  private SpaceConfig newTestSpace(String spaceName) {
    return new SpaceConfig()
      .setName(spaceName);
  }

  private DatasetConfig newTestVirtualDataset(NamespaceKey parent, String name) {
    DatasetConfig ds = new DatasetConfig();
    VirtualDataset vds = new VirtualDataset();
    vds.setVersion(DatasetVersion.newVersion());
    ds.setType(DatasetType.VIRTUAL_DATASET);
    ds.setVirtualDataset(vds);
    ds.setFullPathList(path(parent, name));
    ds.setName(name);

    return ds;
  }

  private DatasetConfig newTestPhysicalDataset(NamespaceKey parent, String name) {
    DatasetConfig ds = new DatasetConfig();
    PhysicalDataset physicalDataset = new PhysicalDataset();
    physicalDataset.setFormatSettings(new FileConfig().setType(FileType.JSON));
    ds.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    ds.setPhysicalDataset(physicalDataset);
    ds.setFullPathList(path(parent, name));
    ds.setName(name);

    return ds;
  }

  private FolderConfig newTestFolder(NamespaceKey parent, String name) {
    return new FolderConfig().setName(name).setFullPathList(path(parent, name));
  }

  private List<String> path(NamespaceKey parent, String name) {
    List<String> path = Lists.newArrayList();
    path.addAll(parent.getPathComponents());
    path.add(name);
    return path;
  }

  private final class TestDatasetDeletionSubscriber implements CatalogStatusSubscriber {
    private int count = 0;
    @Override
    public void onCatalogStatusEvent(CatalogStatusEvent event) {
      if (!(event instanceof DatasetDeletionCatalogStatusEvent)) {
        return;
      }
      count++;
    }

    public int getCount() {
      return count;
    }
  }
}
