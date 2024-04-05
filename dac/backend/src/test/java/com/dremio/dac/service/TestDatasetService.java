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

import static com.dremio.dac.util.DatasetTestUtils.createDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpaceName;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tests the dataset service */
public class TestDatasetService extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testDS() throws Exception {
    NamespaceService namespaceService = newNamespaceService();
    DatasetVersionMutator service = newDatasetVersionMutator();

    SpaceConfig config = new SpaceConfig();
    config.setName("a");
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("b");
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);
    config = new SpaceConfig();
    config.setName("c");
    namespaceService.addOrUpdateSpace(
        new SpacePath(new SpaceName(config.getName())).toNamespaceKey(), config);

    Pair<String, String> vds1 =
        createDS(service, "a.ds1", "ds1", "information_schema.catalogs", "11", null);
    createDS(service, "b.ds2", "ds2", "information_schema.catalogs", "11", null);
    createDS(service, "b.ds3", "ds3", "information_schema.catalogs", "11", null);
    createDS(service, "a.ds4", "ds4", "information_schema.catalogs", "11", null);
    createDS(service, "c.ds5", "ds5", "information_schema.catalogs", "11", null);

    List<NamespaceKey> spaceA =
        Lists.newArrayList(
            service
                .getCatalog()
                .getAllDatasets(new SpacePath(new SpaceName("a")).toNamespaceKey()));
    List<NamespaceKey> spaceB =
        Lists.newArrayList(
            service
                .getCatalog()
                .getAllDatasets(new SpacePath(new SpaceName("b")).toNamespaceKey()));
    List<NamespaceKey> spaceC =
        Lists.newArrayList(
            service
                .getCatalog()
                .getAllDatasets(new SpacePath(new SpaceName("c")).toNamespaceKey()));

    Assert.assertEquals(2, spaceA.size());
    Assert.assertEquals(2, spaceB.size());
    Assert.assertEquals(1, spaceC.size());

    final DatasetVersion v1 = DatasetVersion.newVersion();
    final DatasetVersion v2 = DatasetVersion.newVersion();
    final DatasetVersion v3 = DatasetVersion.newVersion();
    Pair<String, String> vds1_1 =
        createDS(service, "a.ds1", "ds1", "information_schema.catalogs", "100", vds1);
    Pair<String, String> vds1_2 =
        createDS(service, "a.ds1", "ds1", "information_schema.catalogs", v1, vds1_1);
    Pair<String, String> vds1_3 =
        createDS(service, "a.ds1", "ds1", "information_schema.catalogs", v2, vds1_2);
    Pair<String, String> vds1_4 =
        createDS(service, "a.ds1", "ds1", "information_schema.catalogs", "001", vds1_3);
    createDS(service, "a.ds1", "ds1", "information_schema.catalogs", v3, vds1_4);

    Set<DatasetVersion> versions = new HashSet<>();
    for (VirtualDatasetUI ds : service.getAllVersions(new DatasetPath("a.ds1"))) {
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
    for (VirtualDatasetUI ds : service.getAllVersions(new DatasetPath("a.ds1r"))) {
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
    DatasetVersionMutator.VersionDatasetKey versionDatasetKey =
        new DatasetVersionMutator.VersionDatasetKey("path1/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());

    versionDatasetKey = new DatasetVersionMutator.VersionDatasetKey("path1/path2/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1/path2"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());

    versionDatasetKey =
        new DatasetVersionMutator.VersionDatasetKey("path1/path2/path3/0123456789123456");
    Assert.assertEquals(new DatasetPath("path1/path2/path3"), versionDatasetKey.getPath());
    Assert.assertEquals(new DatasetVersion("0123456789123456"), versionDatasetKey.getVersion());
  }

  @Test
  public void testVersionDatasetKeyFail() {
    assertThatThrownBy(() -> new DatasetVersionMutator.VersionDatasetKey("path1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version dataset key should include path and version");
  }
}
