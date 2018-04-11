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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.PartitionValueType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.LegacySourceType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.flatbuffers.FlatBufferBuilder;

import io.protostuff.ByteString;

/**
 * Test driver for spaces service.
 */
public class TestNamespaceService {
  private static final long REFRESH_PERIOD_MS = TimeUnit.HOURS.toMillis(24);
  private static final long GRACE_PERIOD_MS = TimeUnit.HOURS.toMillis(48);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSources() throws Exception {
    try(final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvstore.start();

      final NamespaceService namespaceService = new NamespaceServiceImpl(kvstore);

      final SourceConfig src1 = addSource(namespaceService, "src1");
      Assert.assertEquals(src1, namespaceService.getSource((new NamespaceKey(src1.getName()))));

      // Add some entries under "src1"
      addFolder(namespaceService, "src1.fld1");
      addDS(namespaceService, "src1.ds1");
      addDS(namespaceService, "src1.fld1.ds2");

      final SourceConfig src2 = addSource(namespaceService, "src2");
      Assert.assertEquals(src2, namespaceService.getSource((new NamespaceKey(src2.getName()))));

      // no match
      try {
        namespaceService.getSource(new NamespaceKey("src3"));
        fail("getSource didn't throw exception");
      } catch (NamespaceNotFoundException nfe) {
      } catch (Exception e) {
        fail("Got incorrect exception " + e);
      }
      // updates
      src1.setCtime(2001L);
      src2.setCtime(2001L);
      namespaceService.addOrUpdateSource(new NamespaceKey(src1.getName()), src1.setVersion(0L));
      namespaceService.addOrUpdateSource(new NamespaceKey(src2.getName()), src2.setVersion(0L));

      SourceConfig newSrc1 = namespaceService.getSource(new NamespaceKey(src1.getName()));
      SourceConfig newSrc2 = namespaceService.getSource(new NamespaceKey(src2.getName()));
      Assert.assertEquals(src1, newSrc1);
      Assert.assertEquals(src2, newSrc2);

      // deletes
      try {
        namespaceService.deleteSource(new NamespaceKey("src2"), 1234L);
        fail("deleteSource didn't throw exception");
      } catch (ConcurrentModificationException nfe) {
      }

      namespaceService.deleteSource(new NamespaceKey("src1"), newSrc1.getVersion());

      verifySourceNotInNamespace(namespaceService, new NamespaceKey("src1"));
      // Check entries under "src1" no longer exists in namespace
      verifyFolderNotInNamespace(namespaceService, new NamespaceKey("src1.fld1"));
      verifyDSNotInNamespace(namespaceService, new NamespaceKey("src1.ds1"));
      verifyDSNotInNamespace(namespaceService, new NamespaceKey("src1.fld1.ds2"));

      namespaceService.deleteSource(new NamespaceKey("src2"), newSrc2.getVersion());
      verifySourceNotInNamespace(namespaceService, new NamespaceKey("src2"));

      // Re-add a source with name "src1" and make sure it contains no child entries
      addSource(namespaceService, "src1");
      // Make sure it has no entries under it.
      assertEquals(0, namespaceService.list(new NamespaceKey("src1")).size());
    }
  }

  private void verifySourceNotInNamespace(NamespaceService ns, NamespaceKey nsKey) throws NamespaceException {
    try {
      ns.getSource(nsKey);
      fail("getSource didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
  }

  private void verifyFolderNotInNamespace(NamespaceService ns, NamespaceKey nsKey) throws NamespaceException {
    try {
      ns.getFolder(nsKey);
      fail("getFolder didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
  }

  private void verifyDSNotInNamespace(NamespaceService ns, NamespaceKey nsKey) throws NamespaceException {
    try {
      ns.getDataset(nsKey);
      fail("getDataset didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
  }

  @Test
  public void testSpaces() throws Exception {
    try(
        final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
        ) {
      kvstore.start();
      final NamespaceService namespaceService = new NamespaceServiceImpl(kvstore);
      final SpaceConfig space1 = new SpaceConfig();
      final SpaceConfig space2 = new SpaceConfig();

      space1.setName("space1");
      space1.setCtime(1000L);
      namespaceService.addOrUpdateSpace(new NamespaceKey(space1.getName()), space1);
      Assert.assertEquals(0L, space1.getVersion().longValue());

      SpaceConfig newSpace1 = namespaceService.getSpace(new NamespaceKey(space1.getName()));
      Assert.assertEquals(space1, newSpace1);
      Assert.assertEquals(0L, space1.getVersion().longValue());

      space2.setName("space2");
      space2.setCtime(2000L);
      namespaceService.addOrUpdateSpace(new NamespaceKey(space2.getName()), space2);
      Assert.assertEquals(0L, space2.getVersion().longValue());

      SpaceConfig newSpace2 = namespaceService.getSpace(new NamespaceKey(space2.getName()));
      Assert.assertEquals(space2, newSpace2);
      Assert.assertEquals(0L, space2.getVersion().longValue());

      // no match
      try {
        namespaceService.getSpace(new NamespaceKey("space3"));
        fail("getSource didn't throw exception");
      } catch (NamespaceNotFoundException nfe) {
      } catch (Exception e) {
        fail("Got incorrect exception " + e);
      }
      // updates
      space1.setCtime(2001L);
      space2.setCtime(2001L);
      namespaceService.addOrUpdateSpace(new NamespaceKey(space1.getName()), space1);
      Assert.assertEquals(1L, space1.getVersion().longValue());
      namespaceService.addOrUpdateSpace(new NamespaceKey(space2.getName()), space2);
      Assert.assertEquals(1L, space2.getVersion().longValue());

      assertEquals(space1, namespaceService.getSpace(new NamespaceKey("space1")));
      assertEquals(space2, namespaceService.getSpace(new NamespaceKey("space2")));

      // deletes
      try {
        namespaceService.deleteSpace(new NamespaceKey("space1"), 1234L);
        fail("deleteSpace didn't throw exception");
      } catch (ConcurrentModificationException nfe) {
      }

      namespaceService.deleteSpace(new NamespaceKey("space1"), space1.getVersion());
      try {
        namespaceService.getSpace(new NamespaceKey("space1"));
        fail("getSpace didn't throw exception");
      } catch (NamespaceNotFoundException nfe) {
      }
      namespaceService.deleteSpace(new NamespaceKey("space2"), space2.getVersion());
      try {
        namespaceService.getSpace(new NamespaceKey("space2"));
        fail("getSpace didn't throw exception");
      } catch (NamespaceNotFoundException nfe) {
      }
    }
  }
  public static SourceConfig addSource(NamespaceService ns, String name) throws Exception {
    return addSourceWithRefreshAndGracePeriod(ns, name, REFRESH_PERIOD_MS, GRACE_PERIOD_MS);
  }

  public static SourceConfig addSourceWithRefreshAndGracePeriod(NamespaceService ns, String name, long refreshPeriod,
                                                                long gracePeriod) throws Exception {
    final SourceConfig src = new SourceConfig()
      .setName(name)
      .setCtime(100L)
      .setLegacySourceTypeEnum(LegacySourceType.NAS)
      .setAccelerationRefreshPeriod(refreshPeriod)
      .setAccelerationGracePeriod(gracePeriod);
    ns.addOrUpdateSource(new NamespaceKey(name), src);
    return src;
  }

  public static void addSpace(NamespaceService ns, String name) throws Exception {
    final SpaceConfig space = new SpaceConfig();
    space.setName(name);
    ns.addOrUpdateSpace(new NamespaceKey(name), space);
  }

  public static void addFolder(NamespaceService ns, String name) throws Exception {
    final FolderConfig folder = new FolderConfig();
    final NamespaceKey folderPath = new NamespaceKey(PathUtils.parseFullPath(name));
    folder.setName(folderPath.getName());
    folder.setFullPathList(folderPath.getPathComponents());
    ns.addOrUpdateFolder(folderPath, folder);
  }

  public static void addDS(NamespaceService ns, String name) throws Exception {
    final NamespaceKey dsPath = new NamespaceKey(PathUtils.parseFullPath(name));
    final DatasetConfig ds = new DatasetConfig();
    final VirtualDataset vds = new VirtualDataset();
    vds.setVersion(DatasetVersion.newVersion());
    ds.setType(DatasetType.VIRTUAL_DATASET);
    ds.setVirtualDataset(vds);
    ds.setFullPathList(dsPath.getPathComponents());
    ds.setName(dsPath.getName());
    ns.addOrUpdateDataset(dsPath, ds);
  }

  public static void addFile(NamespaceService ns, List<String> path) throws Exception {
    NamespaceKey filePath = new NamespaceKey(path);
    final boolean isHome = path.get(0).startsWith("@");
    final DatasetConfig ds = new DatasetConfig()
        .setType(isHome ? DatasetType.PHYSICAL_DATASET_HOME_FILE : DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
        .setPhysicalDataset(new PhysicalDataset()
            .setFormatSettings(new FileConfig()));
    ns.addOrUpdateDataset(filePath, ds);
  }

  public static void addHome(NamespaceService ns, String name) throws Exception {
    final HomeConfig homeConfig = new HomeConfig();
    homeConfig.setOwner(name);
    ns.addOrUpdateHome(new NamespaceKey("@" + name), homeConfig);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath) throws Exception {
    addPhysicalDS(ns, filePath, null);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath, byte[] datasetSchema) throws Exception {
    addPhysicalDS(ns, filePath, PHYSICAL_DATASET, datasetSchema);
  }

  public static void addPhysicalDS(NamespaceService ns, String filePath, DatasetType type, byte[] datasetSchema) throws Exception {
    NamespaceKey datasetPath = new NamespaceKey(PathUtils.parseFullPath(filePath));
    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(datasetPath.getName());
    datasetConfig.setType(type);

    final PhysicalDataset physicalDataset = new PhysicalDataset();
    if (datasetSchema != null) {
      datasetConfig.setRecordSchema(io.protostuff.ByteString.copyFrom(datasetSchema));
    }
    datasetConfig.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
    datasetConfig.setPhysicalDataset(physicalDataset);
    ns.tryCreatePhysicalDataset(datasetPath, datasetConfig);
  }

  public static Map<String, NameSpaceContainer> listFolder(NamespaceService ns, String parent) throws Exception {
    Map<String, NameSpaceContainer> children = new HashMap<>();
    for (NameSpaceContainer container : ns.list(new NamespaceKey(PathUtils.parseFullPath(parent)))) {
      children.put(PathUtils.constructFullPath(container.getFullPathList()), container);
    }
    return children;
  }

  public static Map<String, NameSpaceContainer> listHome(NamespaceService ns, String parent) throws Exception {
    Map<String, NameSpaceContainer> children = new HashMap<>();
    for (NameSpaceContainer container : ns.list(new NamespaceKey(parent))) {
      children.put(PathUtils.constructFullPath(container.getFullPathList()), container);
    }
    return children;
  }


  // TODO add more tests after more checks are added to add folder/add space code.
  @Test
  public void testNamespaceTree() throws Exception {
    try (
      final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      addSource(ns, "src1"); // src1
      addSpace(ns, "a"); // space1
      addSpace(ns, "b"); // space2
      addHome(ns, "user1"); // @user1
      addDS(ns, "b.ds1");
      addDS(ns, "b.ds2");
      addDS(ns, "b.ds3");
      addFolder(ns, "b.b1");
      addFolder(ns, "b.b2");
      addFolder(ns, "b.b3");
      addFolder(ns, "b.b4");
      addDS(ns, "b.b4.ds1");
      addSource(ns, "c"); //src2
      addSpace(ns, "a1"); //space3
      addFolder(ns, "a1.a11");
      addSpace(ns, "zz"); //space4
      addSpace(ns, "zz1"); //space5
      addFolder(ns, "a.b");
      addFolder(ns, "a.c");
      addFolder(ns, "a.b.c");
      addDS(ns, "a.b.ds1");
      addDS(ns, "a.b.c.ds1");
      addSource(ns, "zzz1"); // src3
      addFolder(ns, "@user1.foo");
      addFolder(ns, "@user1.foo.bar");
      addFolder(ns, "@user1.foo1");
      addFolder(ns, "@user1.foo2");
      addFolder(ns, "@user1.foo2.bar2");
      addDS(ns, "@user1.foo2.bar2.ds1");
      addFile(ns, asList("@user1", "file1"));
      addFile(ns, asList("@user1", "foo", "file1"));
      addFile(ns, asList("@user1", "foo2", "bar2", "file2"));

      System.out.println("listing sources");
      List<SourceConfig> sources = ns.getSources();
      System.out.println(sources);
      assertEquals(3, sources.size());

      System.out.println("listing spaces");
      List<SpaceConfig> spaces = ns.getSpaces();
      System.out.println(spaces);
      assertEquals(5, spaces.size());


      System.out.println("listing /b");
      Map<String, NameSpaceContainer> items = listFolder(ns, "b");
      //System.out.println(items.keySet());
      assertEquals(7, items.size());

      System.out.println("listing /a1");
      items = listFolder(ns, "a1");
      //System.out.println(items.keySet());
      assertEquals(1, items.size());

      System.out.println("listing /a");
      items = listFolder(ns, "a");
      //System.out.println(items.keySet());
      assertEquals(2, items.size());


      System.out.println("listing /a.b");
      items = listFolder(ns, "a.b");
      //System.out.println(items.keySet());
      assertEquals(2, items.size());

      System.out.println("listing /a.b.c");
      items = listFolder(ns, "a.b.c");
      //System.out.println(items.keySet());
      assertEquals(1, items.size());

      System.out.println("listing /zz1");
      items = listFolder(ns, "zz1");
      //System.out.println(items.keySet());
      assertEquals(0, items.size());


      System.out.println("listing home @user1");
      items = listHome(ns, "@user1");
      //System.out.println(items.keySet());
      assertEquals(4, items.size());

      System.out.println("listing home as folder @user1");
      items = listFolder(ns, "@user1");
      //System.out.println(items.keySet());
      assertEquals(4, items.size());

      System.out.println("listing @user1.foo");
      items = listFolder(ns, "@user1.foo");
      //System.out.println(items.keySet());
      assertEquals(2, items.size());

      System.out.println("listing @user1.foo2.bar2");
      items = listFolder(ns, "@user1.foo2.bar2");
      //System.out.println(items.keySet());
      assertEquals(2, items.size());
    }
  }

  @Test
  public void testDatasetUnderFolderOrSpace() throws Exception {
    try (
        final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      addSpace(ns, "a");
      addFolder(ns, "a.foo");
      addFolder(ns, "a.foo.bar1");
      addFolder(ns, "a.foo.bar2");
      addFolder(ns, "a.foo.bar1.bar3");
      addDS(ns, "a.ds0");
      addDS(ns, "a.foo.ds1");
      addDS(ns, "a.foo.ds2");
      addDS(ns, "a.foo.bar1.ds3");
      addDS(ns, "a.foo.bar2.ds4");
      addDS(ns, "a.foo.bar1.bar3.ds5");
      addDS(ns, "a.foo.bar1.bar3.ds6");

      assertEquals(7, ns.getAllDatasets(new NamespaceKey(asList("a"))).size());
      assertEquals(6, ns.getAllDatasets(new NamespaceKey(asList("a", "foo"))).size());
      assertEquals(3, ns.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar1"))).size());
      assertEquals(1, ns.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar2"))).size());
      assertEquals(2, ns.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar1", "bar3"))).size());
    }
  }

  @Test
  public void testDatasetsUnderHome() throws Exception {
    try (
      final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      addHome(ns, "a");
      addFolder(ns, "@a.foo");
      addFolder(ns, "@a.foo.bar1");
      addFolder(ns, "@a.foo.bar2");
      addFolder(ns, "@a.foo.bar1.bar3");
      addDS(ns, "@a.ds0");
      addDS(ns, "@a.foo.ds1");
      addDS(ns, "@a.foo.ds2");
      addDS(ns, "@a.foo.bar1.ds3");
      addDS(ns, "@a.foo.bar2.ds4");
      addDS(ns, "@a.foo.bar1.bar3.ds5");
      addDS(ns, "@a.foo.bar1.bar3.ds6");

      assertEquals(7, ns.getAllDatasets(new NamespaceKey(asList("@a"))).size());
      assertEquals(6, ns.getAllDatasets(new NamespaceKey(asList("@a", "foo"))).size());
      assertEquals(3, ns.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar1"))).size());
      assertEquals(1, ns.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar2"))).size());
      assertEquals(2, ns.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar1", "bar3"))).size());
    }
  }

  // rewrite this as a reflection test
/*
  @Test
  public void testPhysicalDataset() throws Exception {
    try(
        final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
        ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      // physical dataset tests
      NamespaceKey p1 = new NamespaceKey(asList("src1", "foo", "bar"));
      Assert.assertEquals("src1.foo.bar", p1.toString());
      Assert.assertEquals("src1.foo", p1.getParent().toString());

      NamespaceKey p2 = new NamespaceKey(asList("src2", "foo", "bar", "tee.json"));
      Assert.assertEquals("src2.foo.bar.\"tee.json\"", p2.toString());
      Assert.assertEquals("src2.foo.bar", p2.getParent().toString());

      final long sourceOneRefresh = TimeUnit.HOURS.toMillis(100);
      final long sourceOneGrace = TimeUnit.HOURS.toMillis(200);
      final long sourceTwoRefresh = TimeUnit.HOURS.toMillis(10);
      final long sourceTwoGrace = TimeUnit.HOURS.toMillis(20);
      addSourceWithRefreshAndGracePeriod(ns, "src1", sourceOneRefresh, sourceOneGrace);
      addSourceWithRefreshAndGracePeriod(ns, "src2", sourceTwoRefresh, sourceTwoGrace);
      addPhysicalDS(ns, "src1.foo.bar", PHYSICAL_DATASET_SOURCE_FOLDER, null);
      addPhysicalDS(ns, "src1.foo.bar.\"a.json\"");
      addPhysicalDS(ns, "src1.\"b.json\"");
      addPhysicalDS(ns, "src1.\"c.json");
      assertEquals(1, ns.getAllDatasets(new NamespaceKey(PathUtils.parseFullPath("src1.foo"))).size());
      addPhysicalDS(ns, "src1.foo", PHYSICAL_DATASET_SOURCE_FOLDER, null); // convert "src1.foo" to a dataset. All datasets under "src1.foo" are not invisible
      assertEquals(0, ns.getAllDatasets(new NamespaceKey(PathUtils.parseFullPath("src1.foo"))).size());

      addPhysicalDS(ns, "src2.\"a.json\"");
      addPhysicalDS(ns, "src2.\"c.json\"");
      addPhysicalDS(ns, "src2.bar.foo.foo.bar");
      addPhysicalDS(ns, "src2.foo.bar");

      assertEquals("bar", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src1.foo.bar"))).getName());
      assertEquals("a.json", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src1.foo.bar.\"a.json\""))).getName());
      assertEquals("b.json", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src1.\"b.json\""))).getName());
      assertEquals("c.json", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src1.\"c.json\""))).getName());

      assertEquals("a.json", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src2.\"a.json\""))).getName());
      assertEquals("c.json", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src2.\"c.json\""))).getName());
      assertEquals("bar", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src2.bar.foo.foo.bar"))).getName());
      assertEquals("bar", ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("src2.foo.bar"))).getName());

      assertEquals(3, ns.getAllDatasets(new NamespaceKey("src1")).size());
      // delete folder datasets so that the dataset underneath them are now visible
      ns.deleteDataset(new NamespaceKey(asList("src1", "foo")), ns.getDataset(new NamespaceKey(asList("src1", "foo"))).getVersion());
      assertEquals(3, ns.getAllDatasets(new NamespaceKey("src1")).size());
      //Make sure datasets under "src1.foo" are uncovered
      assertEquals(1, ns.getAllDatasets(new NamespaceKey(PathUtils.parseFullPath("src1.foo"))).size());

      ns.deleteDataset(new NamespaceKey(asList("src1", "foo", "bar")), ns.getDataset(new NamespaceKey(asList("src1", "foo", "bar"))).getVersion());
      assertEquals(3, ns.getAllDatasets(new NamespaceKey("src1")).size());

      final List<NamespaceKey> sourceTwoDatasets = ns.getAllDatasets(new NamespaceKey("src2"));
      assertEquals(4, sourceTwoDatasets.size());

      for (final NamespaceKey key : ns.getAllDatasets(new NamespaceKey("src1"))) {
        final DatasetConfig config = ns.getDataset(key);
        assertEquals((Long) sourceOneRefresh, config.getPhysicalDataset().getAccelerationSettings().getRefreshPeriod());
        assertEquals((Long) sourceOneGrace, config.getPhysicalDataset().getAccelerationSettings().getGracePeriod());
      }

      for (final NamespaceKey key : sourceTwoDatasets) {
        final DatasetConfig config = ns.getDataset(key);
        assertEquals((Long) sourceTwoRefresh, config.getPhysicalDataset().getAccelerationSettings().getRefreshPeriod());
        assertEquals((Long) sourceTwoGrace, config.getPhysicalDataset().getAccelerationSettings().getGracePeriod());
      }

//      assertEquals(0, ns.listPhysicalDatasets(new NamespaceKey(PathUtils.parseFullPath("src2.\"a.json\""))).size());
    }
  }
*/

  @Test
  public void testDataSetSchema() throws Exception {
    try(
            final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      Field field1 = new Field("a", true, new Int(32, true), null);
      Field child1 = new Field("c", true, Utf8.INSTANCE, null);
      Field field2 = new Field("b", true, Struct.INSTANCE, ImmutableList.of(child1));
      Schema schema = new Schema(ImmutableList.of(field1, field2));
      FlatBufferBuilder builder = new FlatBufferBuilder();
      schema.getSchema(builder);
      builder.finish(schema.getSchema(builder));
      addSource(ns, "s");
      addPhysicalDS(ns, "s.foo", builder.sizedByteArray());
      ByteBuffer bb = ByteBuffer.wrap(DatasetHelper.getSchemaBytes(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("s.foo")))).toByteArray());
      Schema returnedSchema = Schema.convertSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(bb));
      assertEquals(schema, returnedSchema);
    }
  }

  @Test
  public void testRename() throws Exception {
    try(
        final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    ) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      addHome(ns, "blue");
      addSpace(ns, "s");
      addFolder(ns, "s.a");
      addFolder(ns, "s.b");
      addFolder(ns, "s.a.b");
      addFolder(ns, "s.a.c");
      addDS(ns, "s.a.ds1");
      addDS(ns, "s.a.c.ds3");
      addFolder(ns, "s.c");
      addFolder(ns, "s.c.c");
      addFolder(ns, "s.c.c.c");
      addFolder(ns, "s.c.c.c.c");
      addDS(ns, "s.c.c.c.c.ds4");
      addDS(ns, "s.b.ds2");
      addFolder(ns, "@blue.a");
      addFolder(ns, "@blue.a.c");
      addFile(ns, asList("@blue", "a", "c", "file1"));
      addSpace(ns, "L");
      addFolder(ns, "L.F");
      addFolder(ns, "L.F.folder");
      addDS(ns, "L.F.ds");

      Map<String, NameSpaceContainer> items;
      /**
      Map<String, NameSpaceContainer> items = listFolder(ns, "s.a");
      //System.out.println("s.a--->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s.a.c"));
      assertTrue(items.containsKey("s.a.b"));
      assertTrue(items.containsKey("s.a.ds1"));

      ns.renameFolder(new NamespaceKey(PathUtils.parseFullPath("s.a.c")), new NamespaceKey(PathUtils.parseFullPath("s.a.c1")));
      items = listFolder(ns, "s.a.c1");
      assertEquals(2, items.size());
      assertTrue(items.keySet().toString(), items.containsKey("s.a.c1.ds3"));
      assertTrue(items.keySet().toString(), items.containsKey("s.a.c1.file1"));

      items = listFolder(ns, "s.a");
      //System.out.println("s.a--->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s.a.c1"));
      assertFalse(items.containsKey("s.a.c"));
      assertTrue(items.containsKey("s.a.b"));
      assertTrue(items.containsKey("s.a.ds1"));

      ns.renameFolder(new NamespaceKey(PathUtils.parseFullPath("s.a")), new NamespaceKey(PathUtils.parseFullPath("s.a1")));
      items = listFolder(ns, "s");
      //System.out.println("s--->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s.a1"));
      assertTrue(items.containsKey("s.b"));
      assertTrue(items.containsKey("s.c"));

      items = listFolder(ns, "s.a1");
      //System.out.println("s-->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s.a1.c1"));
      assertFalse(items.containsKey("s.a1.c"));
      assertTrue(items.containsKey("s.a1.b"));
      assertTrue(items.containsKey("s.a1.ds1"));


      ns.renameSpace(new NamespaceKey("s"), new NamespaceKey("s1"));
      items = listFolder(ns, "s1");
      //System.out.println("s1--->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s1.a1"));
      assertTrue(items.containsKey("s1.b"));
      assertTrue(items.containsKey("s1.c"));

      items = listFolder(ns, "s1.a1");
      //System.out.println("s1-->" + items.keySet());
      assertEquals(3, items.size());
      assertTrue(items.containsKey("s1.a1.c1"));
      assertFalse(items.containsKey("s1.a1.c"));
      assertTrue(items.containsKey("s1.a1.b"));
      assertTrue(items.containsKey("s1.a1.ds1"));

      items = listFolder(ns, "s1.c.c.c.c");
      assertEquals(1, items.size());
      assertTrue(items.containsKey("s1.c.c.c.c.ds4"));

      items = listFolder(ns, "s1.b");
      assertEquals(1, items.size());
      assertTrue(items.containsKey("s1.b.ds2"));
      */

      ns.renameDataset(new NamespaceKey(PathUtils.parseFullPath("s.b.ds2")), new NamespaceKey(PathUtils.parseFullPath("s.b.ds22")));
      items = listFolder(ns, "s.b");
      assertEquals(1, items.size());
      assertTrue(items.containsKey("s.b.ds22"));

      ns.renameDataset(new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds3")), new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds33")));
      items = listFolder(ns, "s.a.c");
      assertEquals(1, items.size());
      assertTrue(items.containsKey("s.a.c.ds33"));

      try {
        ns.renameDataset(new NamespaceKey(PathUtils.parseFullPath(("@blue.a.c.file1"))),
            new NamespaceKey(PathUtils.parseFullPath(("s.a.c.file1DOTjson"))));
        Assert.fail("renames on physical datasets should not be allowed");
      } catch (final UserException ex) {
        // pass
      }

      items = listFolder(ns, "L.F");
      assertEquals(2, items.size());
      assertTrue(items.containsKey("L.F.ds"));
      assertTrue(items.containsKey("L.F.folder"));

      // Move dataset
      ns.renameDataset(new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds33")), new NamespaceKey(PathUtils.parseFullPath("L.F.ds33r")));
      items = listFolder(ns, "@blue.a.c");
      assertEquals(1, items.size());
      assertTrue(items.containsKey("\"@blue\".a.c.file1"));

      items = listFolder(ns, "L.F");
      assertEquals(3, items.size());
      assertTrue(items.containsKey("L.F.ds"));
      assertTrue(items.containsKey("L.F.folder"));
      assertTrue(items.containsKey("L.F.ds33r"));
      //System.out.println("L.F->" + items.keySet());
    }
  }

  @Test
  public void insertingDifferentEntityTypesAtSamePath() throws Exception {
    try (final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      addSpace(ns, "a");

      thrown.expect(ConcurrentModificationException.class);
      addSource(ns, "a");

      addFolder(ns, "a.foo");

      // Try to add dataset with path "a.foo"
      try {
        addDS(ns, "a.foo");
        fail("Expected the above call to fail");
      } catch (UserException ex) {
        assertTrue(ex.getMessage().contains("There already exists an entity of type [FOLDER] at given path [a.foo]"));
      }

      // Try to add folder with path "a.foo". There already a folder at "a.foo"
      try {
        addFolder(ns, "a.foo");
        fail("Expected the above call to fail");
      } catch (UserException ex) {
        assertTrue(ex.getMessage().contains("There already exists an entity of type [FOLDER] at given path [a.foo]"));
      }
    }
  }

  @Test
  public void testDatasetSplitsUpdates() throws Exception {
    try (final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvstore.start();
      final NamespaceService ns = new NamespaceServiceImpl(kvstore);
      Long lastSplitVersion = System.currentTimeMillis();

      DatasetConfig datasetConfig = new DatasetConfig();
      ReadDefinition readDefinition = new ReadDefinition();

      readDefinition.setSplitVersion(lastSplitVersion);

      datasetConfig.setType(PHYSICAL_DATASET);
      datasetConfig.setVersion(null);
      datasetConfig.setId(new EntityId().setId(UUID.randomUUID().toString()));
      datasetConfig.setName("testDatasetSplitsInsert");
      datasetConfig.setFullPathList(Lists.newArrayList("test", "testDatasetSplitsInsert"));
      datasetConfig.setAccelerationId("accl");
      datasetConfig.setOwner("dremio");
      datasetConfig.setReadDefinition(readDefinition);

      List<DatasetSplit> splits = Lists.newArrayList();

      for (int i = 0; i < 10; i++) {
        splits.add(new DatasetSplit()
          .setRowCount((long) i)
          .setVersion(0L)
          .setSize((long) i)
          .setAffinitiesList(Lists.<Affinity>newArrayList(new Affinity().setHost("node" + i)))
          .setPartitionValuesList(Lists.newArrayList(new PartitionValue().setColumn("column" + i).setIntValue(i).setType(PartitionValueType.IMPLICIT)))
          .setExtendedProperty(ByteString.copyFrom(String.valueOf(i).getBytes()))
          .setSplitKey(String.valueOf(i))
          .setSplitVersion(1L));
      }

      addSource(ns, "test");
      ns.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig, splits);

      assertEquals(10, ns.getSplitCount(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))));
      assertTrue(!NamespaceServiceImpl.compareSplits(datasetConfig, splits,
        ns.findSplits(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)))));
      Long newSplitVersion = datasetConfig.getReadDefinition().getSplitVersion();
      assertTrue(newSplitVersion > lastSplitVersion);
      lastSplitVersion = newSplitVersion;

      // insert same splits again and make sure version does't change
      ns.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig, splits);
      assertEquals(10, ns.getSplitCount(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))));
      assertTrue(!NamespaceServiceImpl.compareSplits(datasetConfig, splits,
        ns.findSplits(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)))));
      assertEquals(newSplitVersion, datasetConfig.getReadDefinition().getSplitVersion());

      // change row count for the first split
      splits.get(0).setRowCount(11L);
      ns.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig, splits);
      assertEquals(10, ns.getSplitCount(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))));
      assertTrue(!NamespaceServiceImpl.compareSplits(datasetConfig, splits,
        ns.findSplits(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)))));
      newSplitVersion = datasetConfig.getReadDefinition().getSplitVersion();
      assertTrue(newSplitVersion > lastSplitVersion);
      lastSplitVersion = newSplitVersion;

      // remove 8th split
      splits.remove(8);
      ns.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig, splits);
      assertEquals(9, ns.getSplitCount(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))));
      assertTrue(!NamespaceServiceImpl.compareSplits(datasetConfig, splits,
        ns.findSplits(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)))));
      newSplitVersion = datasetConfig.getReadDefinition().getSplitVersion();
      assertTrue(newSplitVersion > lastSplitVersion);
      lastSplitVersion = newSplitVersion;

      // add another split
      splits.add(new DatasetSplit()
        .setRowCount(11L)
        .setVersion(0L)
        .setSize(11L)
        .setAffinitiesList(Lists.<Affinity>newArrayList(new Affinity().setHost("node" + 11)))
        .setPartitionValuesList(Lists.newArrayList(new PartitionValue().setColumn("column" + 11).setIntValue(11).setType(PartitionValueType.IMPLICIT)))
        .setExtendedProperty(ByteString.copyFrom(String.valueOf(11).getBytes()))
        .setSplitKey(String.valueOf(11))
        .setSplitVersion(lastSplitVersion));

      ns.addOrUpdateDataset(new NamespaceKey(datasetConfig.getFullPathList()), datasetConfig, splits);
      assertEquals(10, ns.getSplitCount(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig))));
      assertTrue(!NamespaceServiceImpl.compareSplits(datasetConfig, splits,
        ns.findSplits(new IndexedStore.FindByCondition().setCondition(DatasetSplitId.getSplitsQuery(datasetConfig)))));
      newSplitVersion = datasetConfig.getReadDefinition().getSplitVersion();
      assertTrue(newSplitVersion > lastSplitVersion);
    }
  }

  @Test
  public void testDeleteEntityNotFound() throws Exception {
    try (final KVStoreProvider kvstore = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvstore.start();
      final NamespaceServiceImpl ns = new NamespaceServiceImpl(kvstore);

      try {
        ns.deleteEntity(new NamespaceKey(Arrays.asList("does", "not", "exist")), NameSpaceContainer.Type.FOLDER, 123L, true);
        fail("deleteEntity should have failed.");
      } catch(NamespaceNotFoundException e) {
        // Expected
      }
    }
  }
}
