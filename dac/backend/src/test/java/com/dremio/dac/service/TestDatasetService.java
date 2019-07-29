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
package com.dremio.dac.service;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.Lists;

/**
 * Tests the dataset service
 */
public class TestDatasetService extends BaseTestServer {

  /**
   * Rule for tests that verify {@link com.dremio.common.exceptions.UserException} type and message. See
   * {@link UserExceptionMatcher} and e.g. {@link com.dremio.exec.server.TestOptions#checkValidationException}.
   * Tests that do not use this rule are not affected.
   */
  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  private Pair<String, String> createDS(DatasetVersionMutator service, String path, String name, String table, String version, Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    return createDS(service, path, name, table, new DatasetVersion(version), idVersionPair);
  }

  private Pair<String, String> createDS(DatasetVersionMutator service, String path, String name, String table, DatasetVersion version,
      Pair<String, String> idVersionPair)
      throws NamespaceException, DatasetNotFoundException {
    DatasetPath path1 = new DatasetPath(path);
    VirtualDatasetUI ds1 = new VirtualDatasetUI();
    ds1.setFullPathList(path1.toPathList());
    ds1.setVersion(version);
    ds1.setSavedTag(idVersionPair == null ? null : idVersionPair.getValue());
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
    return Pair.of(dsOut.getId(), dsOut.getSavedTag());
  }

  private String createPhysicalDS(NamespaceService ns, String path, DatasetType datasetType) throws NamespaceException{
    DatasetConfig datasetConfig = new DatasetConfig();
    PhysicalDatasetPath physicalDatasetPath = new PhysicalDatasetPath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(physicalDatasetPath.toPathList());
    datasetConfig.setName(physicalDatasetPath.getLeaf().getName());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setTag(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(physicalDatasetPath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getTag();
  }

  private String createPhysicalDSInHome(NamespaceService ns, String path, DatasetType datasetType) throws NamespaceException{
    DatasetConfig datasetConfig = new DatasetConfig();
    FilePath filePath  = new FilePath(path);
    datasetConfig.setType(datasetType);
    datasetConfig.setFullPathList(filePath.toPathList());
    datasetConfig.setName(filePath.getFileName().toString());
    datasetConfig.setCreatedAt(System.currentTimeMillis());
    datasetConfig.setTag(null);
    datasetConfig.setOwner("test_user");
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    ns.addOrUpdateDataset(filePath.toNamespaceKey(), datasetConfig);
    return datasetConfig.getTag();
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

    Pair<String, String> vds1 = createDS(service, "a.ds1", "ds1", "sky1", "11", null);
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
    Pair<String, String> vds1_1 = createDS(service, "a.ds1", "ds1", "sky1", "100", vds1);
    Pair<String, String> vds1_2 = createDS(service, "a.ds1", "ds1", "sky1", v1, vds1_1);
    Pair<String, String> vds1_3 = createDS(service, "a.ds1", "ds1", "sky1", v2, vds1_2);
    Pair<String, String> vds1_4 = createDS(service, "a.ds1", "ds1", "sky1", "001", vds1_3);
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
  public void testVersionDatasetKey() {
    DatasetVersionMutator.VersionDatasetKey versionDatasetKey = new DatasetVersionMutator.VersionDatasetKey("path1/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());

    versionDatasetKey = new DatasetVersionMutator.VersionDatasetKey("path1/path2/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1/path2"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());

    versionDatasetKey = new DatasetVersionMutator.VersionDatasetKey("path1/path2/path3/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1/path2/path3"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());
  }

  @Test
  public void testVersionDatasetKeyFail() {
    thrownException.expect(IllegalArgumentException.class);
    DatasetVersionMutator.VersionDatasetKey versionDatasetKey = new DatasetVersionMutator.VersionDatasetKey("path1");
    Assert.assertNotEquals(new DatasetPath("path1"), versionDatasetKey.getPath());
  }
}
