/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.service;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.file.FilePath;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.TestNamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.Lists;

/**
 * Tests the dataset service
 */
public class TestDatasetService extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  private Pair<String, Long> createDS(DatasetVersionMutator service, String path, String name, String table, String version, Pair<String, Long> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    return createDS(service, path, name, table, new DatasetVersion(version), idVersionPair);
  }

  private Pair<String, Long> createDS(DatasetVersionMutator service, String path, String name, String table, DatasetVersion version,
      Pair<String, Long> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    DatasetPath path1 = new DatasetPath(path);
    VirtualDatasetUI ds1 = new VirtualDatasetUI();
    ds1.setFullPathList(path1.toPathList());
    ds1.setVersion(version);
    ds1.setSavedVersion(idVersionPair == null ? null : idVersionPair.getValue());
    ds1.setName(name);
    ds1.setState(new VirtualDatasetState()
        .setFrom(new FromTable(path1.toPathString()).wrap()));
    ds1.getState().setColumnsList(asList(new Column("foo", new ExpColumnReference("bar").wrap())));
    ds1.setSql("select * from " + table);
    ds1.setId(idVersionPair == null ? null : idVersionPair.getKey());
    ViewFieldType type = new ViewFieldType("hello", "float");
    ds1.setSqlFieldsList(Collections.singletonList(type));
    ds1.setCalciteFieldsList(Collections.singletonList(type));
    service.put(ds1);
    service.putVersion(ds1);
    VirtualDatasetUI dsOut = service.get(path1);
    return Pair.of(dsOut.getId(), dsOut.getSavedVersion());
  }

  private long createPhysicalDS(NamespaceService ns, String path, DatasetType datasetType) throws NamespaceException{
    DatasetConfig datasetConfig = new DatasetConfig();
    PhysicalDatasetPath physicalDatasetPath = new PhysicalDatasetPath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(physicalDatasetPath.toPathList());
    datasetConfig.setName(physicalDatasetPath.getLeaf().getName());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setVersion(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(physicalDatasetPath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getVersion();
  }

  private long createPhysicalDSInHome(NamespaceService ns, String path, DatasetType datasetType) throws NamespaceException{
    DatasetConfig datasetConfig = new DatasetConfig();
    FilePath filePath  = new FilePath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(filePath.toPathList());
    datasetConfig.setName(filePath.getFileName().toString());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setVersion(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(filePath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getVersion();
  }

  @Test
  public void testDS() throws Exception {
    NamespaceService namespaceService = newNamespaceService();
    DatasetVersionMutator service = newDatasetVersionMutator();

    SpaceConfig config = new SpaceConfig();
    config.setName("a");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("b");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("c");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);

    Pair<String, Long> vds1 = createDS(service, "a.ds1", "ds1", "sky1", "11", null);
    createDS(service, "b.ds2", "ds2", "sky2", "11", null);
    createDS(service, "b.ds3", "ds3", "sky3", "11", null);
    createDS(service, "a.ds4", "ds4", "sky4", "11", null);
    createDS(service, "c.ds5", "ds5", "sky5", "11", null);

    List<NamespaceKey> spaceA = Lists.newArrayList(service.getNamespaceService()
        .getAllDatasets(new SpacePath(new SpaceName("a")).toNamespaceKey()));
    List<NamespaceKey> spaceB = Lists.newArrayList(service.getNamespaceService()
        .getAllDatasets(new SpacePath(new SpaceName("b")).toNamespaceKey()));
    List<NamespaceKey> spaceC = Lists.newArrayList(service.getNamespaceService()
        .getAllDatasets(new SpacePath(new SpaceName("c")).toNamespaceKey()));

    Assert.assertEquals(2, spaceA.size());
    Assert.assertEquals(2, spaceB.size());
    Assert.assertEquals(1, spaceC.size());


    final DatasetVersion v1 = DatasetVersion.newVersion();
    final DatasetVersion v2 = DatasetVersion.newVersion();
    final DatasetVersion v3 = DatasetVersion.newVersion();
    Pair<String, Long> vds1_1 = createDS(service, "a.ds1", "ds1", "sky1", "100", vds1);
    Pair<String, Long> vds1_2 = createDS(service, "a.ds1", "ds1", "sky1", v1, vds1_1);
    Pair<String, Long> vds1_3 = createDS(service, "a.ds1", "ds1", "sky1", v2, vds1_2);
    Pair<String, Long> vds1_4 = createDS(service, "a.ds1", "ds1", "sky1", "001", vds1_3);
    createDS(service, "a.ds1", "ds1", "sky1", v3, vds1_4);

    Set<DatasetVersion> versions = new HashSet<>();
    for (VirtualDatasetUI ds: service.getAllVersions(new DatasetPath("a.ds1"))) {
      Assert.assertEquals(ds.getName(), "ds1");
      versions.add(ds.getVersion());
    }
    Assert.assertTrue(versions.toString(), versions.contains(v1));
    Assert.assertTrue(versions.toString(), versions.contains(v2));
    Assert.assertTrue(versions.toString(), versions.contains(v3));
    Assert.assertTrue(versions.toString(), versions.contains(new DatasetVersion("11")));
    Assert.assertTrue(versions.toString(), versions.contains(new DatasetVersion("100")));
    Assert.assertTrue(versions.toString(), versions.contains(new DatasetVersion("001")));

    service.renameDataset(new DatasetPath("a.ds1"), new DatasetPath("a.ds1r"));
    versions = new HashSet<>();
    for (VirtualDatasetUI ds: service.getAllVersions(new DatasetPath("a.ds1r"))) {
      Assert.assertEquals(ds.getName(), "ds1r");
      versions.add(ds.getVersion());
    }
    Assert.assertTrue(versions.contains(v1));
    Assert.assertTrue(versions.contains(v2));
    Assert.assertTrue(versions.contains(v3));
    Assert.assertTrue(versions.contains(new DatasetVersion("11")));
    Assert.assertTrue(versions.contains(new DatasetVersion("100")));
    Assert.assertTrue(versions.contains(new DatasetVersion("001")));

    Assert.assertTrue(!service.getAllVersions(new DatasetPath("a.ds1")).iterator().hasNext());
  }

  @Test
  public void testSearchDatasets() throws Exception {
    NamespaceService namespaceService = newNamespaceService();
    DatasetVersionMutator service = newDatasetVersionMutator();
    SpaceConfig config = new SpaceConfig();
    config.setName("a");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("b");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("c");
    namespaceService.addOrUpdateSpace(new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);

    createDS(service, "a.ds1", "ds1", "sky1", "11", null);
    createDS(service, "a.ds2", "ds2", "sky2", "12", null);

    createDS(service, "b.ds3", "ds3", "sky1", "12", null);
    createDS(service, "b.ds4", "ds4", "sky2", "13", null);
    createDS(service, "b.ds5", "ds5", "sky3", "14", null);

    createDS(service, "c.ds6", "ds6", "sky1", "15", null);
    createDS(service, "c.ds7", "ds7", "sky2", "16", null);
    createDS(service, "c.ds8", "ds8", "sky3", "17", null);
    createDS(service, "c.ds9", "ds9", "sky1", "18", null);

    Assert.assertEquals(4, service.searchDatasets("sky1").size());
    Assert.assertEquals(3, service.searchDatasets("sky2").size());
    Assert.assertEquals(2, service.searchDatasets("sky3").size());
    Assert.assertEquals(1, service.searchDatasets("ds1").size());
    Assert.assertEquals(1, service.searchDatasets("ds9").size());
    Assert.assertEquals(1, service.searchDatasets("ds3").size());
    Assert.assertEquals(9, service.searchDatasets("sky").size());

    TestNamespaceService.addSource(namespaceService, "src1");
    TestNamespaceService.addSource(namespaceService, "src2");
    TestNamespaceService.addHome(namespaceService, DEFAULT_USERNAME);

    createPhysicalDS(namespaceService, "src1.foo1", DatasetType.PHYSICAL_DATASET);
    createPhysicalDSInHome(namespaceService, "@"+DEFAULT_USERNAME+".foo11", DatasetType.PHYSICAL_DATASET_HOME_FILE);
    createPhysicalDS(namespaceService, "src1.foo2", DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    createPhysicalDS(namespaceService, "src2.foo22", DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);

    Assert.assertEquals(2, service.searchDatasets("src1").size());
    Assert.assertEquals(1, service.searchDatasets("src2").size());
    Assert.assertEquals(4, service.searchDatasets("foo").size());
    Assert.assertEquals(2, service.searchDatasets("foo1").size());
    Assert.assertEquals(1, service.searchDatasets("foo11").size());
    Assert.assertEquals(2, service.searchDatasets("foo2").size());
    Assert.assertEquals(1, service.searchDatasets("foo22").size());

    createPhysicalDS(namespaceService, "src1.sky1", DatasetType.PHYSICAL_DATASET);
    Assert.assertEquals(10, service.searchDatasets("sky").size());
  }
}
