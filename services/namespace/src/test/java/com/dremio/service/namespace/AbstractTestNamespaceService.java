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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Test driver for spaces service.
 */
public abstract class AbstractTestNamespaceService {

  private NamespaceServiceImpl namespaceService;
  private LegacyKVStoreProvider provider;
  protected abstract LegacyKVStoreProvider createKVStoreProvider() throws Exception;
  protected abstract void closeResources() throws Exception;

  @Before
  public void before() throws Exception {
    provider = createKVStoreProvider();
    provider.start();
    namespaceService = new NamespaceServiceImpl(provider, new CatalogStatusEventsImpl());
  }

  @After
  public void after() throws Exception {
    namespaceService = null;
    provider.close();
    closeResources();
  }

  @Test
  public void testSources() throws Exception {
    final SourceConfig src1 = NamespaceTestUtils.addSource(namespaceService, "src1");
    Assert.assertEquals(src1, namespaceService.getSource((new NamespaceKey(src1.getName()))));
    Assert.assertEquals(src1.getConfigOrdinal().longValue(), 0L);

    // Add some entries under "src1"
    NamespaceTestUtils.addFolder(namespaceService, "src1.fld1");
    NamespaceTestUtils.addDS(namespaceService, "src1.ds1");
    NamespaceTestUtils.addDS(namespaceService, "src1.fld1.ds2");

    final SourceConfig src2 = NamespaceTestUtils.addSource(namespaceService, "src2");
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
    namespaceService.addOrUpdateSource(new NamespaceKey(src1.getName()), src1);
    namespaceService.addOrUpdateSource(new NamespaceKey(src2.getName()), src2);

    SourceConfig newSrc1 = namespaceService.getSource(new NamespaceKey(src1.getName()));
    SourceConfig newSrc2 = namespaceService.getSource(new NamespaceKey(src2.getName()));
    Assert.assertEquals(src1, newSrc1);
    Assert.assertEquals(src2, newSrc2);
    Assert.assertEquals(newSrc1.getConfigOrdinal().longValue(), 1L);

    // deletes
    try {
      namespaceService.deleteSource(new NamespaceKey("src2"), "1234");
      fail("deleteSource didn't throw exception");
    } catch (ConcurrentModificationException nfe) {
    }

    namespaceService.deleteSource(new NamespaceKey("src1"), newSrc1.getTag());

    verifySourceNotInNamespace(namespaceService, new NamespaceKey("src1"));
    // Check entries under "src1" no longer exists in namespace
    verifyFolderNotInNamespace(namespaceService, new NamespaceKey("src1.fld1"));
    verifyDSNotInNamespace(namespaceService, new NamespaceKey("src1.ds1"));
    verifyDSNotInNamespace(namespaceService, new NamespaceKey("src1.fld1.ds2"));

    namespaceService.deleteSource(new NamespaceKey("src2"), newSrc2.getTag());
    verifySourceNotInNamespace(namespaceService, new NamespaceKey("src2"));

    // Re-add a source with name "src1" and make sure it contains no child entries
    NamespaceTestUtils.addSource(namespaceService, "src1");
    // Make sure it has no entries under it.
    assertEquals(0, namespaceService.list(new NamespaceKey("src1")).size());
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
    final SpaceConfig space1 = new SpaceConfig();
    final SpaceConfig space2 = new SpaceConfig();

    space1.setName("space1");
    space1.setCtime(1000L);
    namespaceService.addOrUpdateSpace(new NamespaceKey(space1.getName()), space1);

    SpaceConfig newSpace1 = namespaceService.getSpace(new NamespaceKey(space1.getName()));
    Assert.assertEquals(space1, newSpace1);

    space2.setName("space2");
    space2.setCtime(2000L);
    namespaceService.addOrUpdateSpace(new NamespaceKey(space2.getName()), space2);

    SpaceConfig newSpace2 = namespaceService.getSpace(new NamespaceKey(space2.getName()));
    Assert.assertEquals(space2, newSpace2);

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
    namespaceService.addOrUpdateSpace(new NamespaceKey(space2.getName()), space2);

    assertEquals(space1, namespaceService.getSpace(new NamespaceKey("space1")));
    assertEquals(space2, namespaceService.getSpace(new NamespaceKey("space2")));

    // deletes
    try {
      namespaceService.deleteSpace(new NamespaceKey("space1"), "1234");
      fail("deleteSpace didn't throw exception");
    } catch (ConcurrentModificationException nfe) {
    }

    namespaceService.deleteSpace(new NamespaceKey("space1"), space1.getTag());
    try {
      namespaceService.getSpace(new NamespaceKey("space1"));
      fail("getSpace didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
    namespaceService.deleteSpace(new NamespaceKey("space2"), space2.getTag());
    try {
      namespaceService.getSpace(new NamespaceKey("space2"));
      fail("getSpace didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
  }


  @Test
  public void testUdf() throws Exception {
    NamespaceTestUtils.addSpace(namespaceService, "sp1");
    NamespaceTestUtils.addSpace(namespaceService, "sp2");

    final FunctionConfig udf1 = new FunctionConfig();
    final FunctionConfig udf2 = new FunctionConfig();

    final NamespaceKey udf1Ns = new NamespaceKey(PathUtils.parseFullPath("sp1.udf1"));
    final NamespaceKey udf2Ns = new NamespaceKey(PathUtils.parseFullPath("sp2.udf2"));

    namespaceService.addOrUpdateFunction(udf1Ns, udf1);

    FunctionConfig newUdf1 = namespaceService.getFunction(udf1Ns);
    Assert.assertEquals(udf1, newUdf1);

    namespaceService.addOrUpdateFunction(udf2Ns, udf2);

    FunctionConfig newUdf2 = namespaceService.getFunction(udf2Ns);
    Assert.assertEquals(udf2, newUdf2);

    // no match
    try {
      namespaceService.getFunction(new NamespaceKey("sp1.udf3"));
      fail("getSource didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    } catch (Exception e) {
      fail("Got incorrect exception " + e);
    }
    // updates
    namespaceService.addOrUpdateFunction(udf1Ns, udf1);
    namespaceService.addOrUpdateFunction(udf2Ns, udf2);

    assertEquals(udf1, namespaceService.getFunction(udf1Ns));
    assertEquals(udf2, namespaceService.getFunction(udf2Ns));

    namespaceService.deleteFunction(udf1Ns);
    try {
      namespaceService.getFunction(udf1Ns);
      fail("get function didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
    namespaceService.deleteFunction(udf2Ns);
    try {
      namespaceService.getFunction(udf2Ns);
      fail("get function didn't throw exception");
    } catch (NamespaceNotFoundException nfe) {
    }
  }


  // TODO add more tests after more checks are added to add folder/add space code.
  @Test
  public void testNamespaceTree() throws Exception {
    NamespaceTestUtils.addSource(namespaceService, "src1"); // src1
    NamespaceTestUtils.addSpace(namespaceService, "a"); // space1
    NamespaceTestUtils.addSpace(namespaceService, "b"); // space2
    NamespaceTestUtils.addHome(namespaceService, "user1"); // @user1
    NamespaceTestUtils.addDS(namespaceService, "b.ds1");
    NamespaceTestUtils.addDS(namespaceService, "b.ds2");
    NamespaceTestUtils.addDS(namespaceService, "b.ds3");
    NamespaceTestUtils.addFolder(namespaceService, "b.例子");
    NamespaceTestUtils.addFolder(namespaceService, "b.b1");
    NamespaceTestUtils.addFolder(namespaceService, "b.b2");
    NamespaceTestUtils.addFolder(namespaceService, "b.b3");
    NamespaceTestUtils.addFolder(namespaceService, "b.b4");
    NamespaceTestUtils.addDS(namespaceService, "b.b4.ds1");
    NamespaceTestUtils.addSource(namespaceService, "c"); //src2
    NamespaceTestUtils.addSpace(namespaceService, "a1"); //space3
    NamespaceTestUtils.addFolder(namespaceService, "a1.a11");
    NamespaceTestUtils.addSpace(namespaceService, "zz"); //space4
    NamespaceTestUtils.addSpace(namespaceService, "zz1"); //space5
    NamespaceTestUtils.addFolder(namespaceService, "a.b");
    NamespaceTestUtils.addFolder(namespaceService, "a.c");
    NamespaceTestUtils.addFolder(namespaceService, "a.b.c");
    NamespaceTestUtils.addDS(namespaceService, "a.b.ds1");
    NamespaceTestUtils.addDS(namespaceService, "a.b.c.ds1");
    NamespaceTestUtils.addSource(namespaceService, "zzz1"); // src3
    NamespaceTestUtils.addFolder(namespaceService, "@user1.foo");
    NamespaceTestUtils.addFolder(namespaceService, "@user1.foo.bar");
    NamespaceTestUtils.addFolder(namespaceService, "@user1.foo1");
    NamespaceTestUtils.addFolder(namespaceService, "@user1.foo2");
    NamespaceTestUtils.addFolder(namespaceService, "@user1.foo2.bar2");
    NamespaceTestUtils.addDS(namespaceService, "@user1.foo2.bar2.ds1");
    NamespaceTestUtils.addFile(namespaceService, asList("@user1", "file1"));
    NamespaceTestUtils.addFile(namespaceService, asList("@user1", "foo", "file1"));
    NamespaceTestUtils.addFile(namespaceService, asList("@user1", "foo2", "bar2", "file2"));
    NamespaceTestUtils.addSource(namespaceService, "자원"); // src4
    NamespaceTestUtils.addSpace(namespaceService, "根"); // space6

    System.out.println("listing sources");
    List<SourceConfig> sources = namespaceService.getSources();
    System.out.println(sources);
    assertEquals(4, sources.size());

    System.out.println("listing spaces");
    List<SpaceConfig> spaces = namespaceService.getSpaces();
    System.out.println(spaces);
    assertEquals(6, spaces.size());


    System.out.println("listing /b");
    Map<String, NameSpaceContainer> items = NamespaceTestUtils.listFolder(namespaceService, "b");
    //System.out.println(items.keySet());
    assertEquals(8, items.size());

    System.out.println("listing /a1");
    items = NamespaceTestUtils.listFolder(namespaceService, "a1");
    //System.out.println(items.keySet());
    assertEquals(1, items.size());

    System.out.println("listing /a");
    items = NamespaceTestUtils.listFolder(namespaceService, "a");
    //System.out.println(items.keySet());
    assertEquals(2, items.size());


    System.out.println("listing /a.b");
    items = NamespaceTestUtils.listFolder(namespaceService, "a.b");
    //System.out.println(items.keySet());
    assertEquals(2, items.size());

    System.out.println("listing /a.b.c");
    items = NamespaceTestUtils.listFolder(namespaceService, "a.b.c");
    //System.out.println(items.keySet());
    assertEquals(1, items.size());

    System.out.println("listing /zz1");
    items = NamespaceTestUtils.listFolder(namespaceService, "zz1");
    //System.out.println(items.keySet());
    assertEquals(0, items.size());


    System.out.println("listing home @user1");
    items = NamespaceTestUtils.listHome(namespaceService, "@user1");
    //System.out.println(items.keySet());
    assertEquals(4, items.size());

    System.out.println("listing home as folder @user1");
    items = NamespaceTestUtils.listFolder(namespaceService, "@user1");
    //System.out.println(items.keySet());
    assertEquals(4, items.size());

    System.out.println("listing @user1.foo");
    items = NamespaceTestUtils.listFolder(namespaceService, "@user1.foo");
    //System.out.println(items.keySet());
    assertEquals(2, items.size());

    System.out.println("listing @user1.foo2.bar2");
    items = NamespaceTestUtils.listFolder(namespaceService, "@user1.foo2.bar2");
    //System.out.println(items.keySet());
    assertEquals(2, items.size());
  }

  @Test
  public void testDatasetUnderFolderOrSpace() throws Exception {
    NamespaceTestUtils.addSpace(namespaceService, "a");
    NamespaceTestUtils.addFolder(namespaceService, "a.foo");
    NamespaceTestUtils.addFolder(namespaceService, "a.foo.bar1");
    NamespaceTestUtils.addFolder(namespaceService, "a.foo.bar2");
    NamespaceTestUtils.addFolder(namespaceService, "a.foo.bar1.bar3");
    NamespaceTestUtils.addDS(namespaceService, "a.ds0");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.ds1");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.ds2");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.bar1.ds3");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.bar2.ds4");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.bar1.bar3.ds5");
    NamespaceTestUtils.addDS(namespaceService, "a.foo.bar1.bar3.ds6");

    assertEquals(7, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("a")))));
    assertEquals(6, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("a", "foo")))));
    assertEquals(3, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar1")))));
    assertEquals(1, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar2")))));
    assertEquals(2, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("a", "foo", "bar1", "bar3")))));
  }

  @Test
  public void testDatasetsUnderHome() throws Exception {
    NamespaceTestUtils.addHome(namespaceService, "a");
    NamespaceTestUtils.addFolder(namespaceService, "@a.foo");
    NamespaceTestUtils.addFolder(namespaceService, "@a.foo.bar1");
    NamespaceTestUtils.addFolder(namespaceService, "@a.foo.bar2");
    NamespaceTestUtils.addFolder(namespaceService, "@a.foo.bar1.bar3");
    NamespaceTestUtils.addDS(namespaceService, "@a.ds0");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.ds1");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.ds2");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.bar1.ds3");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.bar2.ds4");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.bar1.bar3.ds5");
    NamespaceTestUtils.addDS(namespaceService, "@a.foo.bar1.bar3.ds6");

    assertEquals(7, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("@a")))));
    assertEquals(6, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("@a", "foo")))));
    assertEquals(1, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar2")))));
    assertEquals(3, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar1")))));
    assertEquals(2, Iterables.size(namespaceService.getAllDatasets(new NamespaceKey(asList("@a", "foo", "bar1", "bar3")))));
  }

  @Test
  public void testGetDatasetCount() throws Exception {
    // create some nested datasets
    NamespaceTestUtils.addSource(namespaceService, "a");

    for (int i = 0; i < 10; i++) {
      NamespaceTestUtils.addDS(namespaceService, "a.foo" + i);
    }

    for (int i = 0; i < 50; i++) {
      NamespaceTestUtils.addDS(namespaceService, "a.foo0.bar" + i);
    }

    for (int i = 0; i < 50; i++) {
      NamespaceTestUtils.addDS(namespaceService, "a.baz" + i);
    }

    // test count bound
    BoundedDatasetCount boundedDatasetCount = namespaceService.getDatasetCount(new NamespaceKey("a"), 5000, 30);
    assertTrue(boundedDatasetCount.isCountBound());
    assertEquals(boundedDatasetCount.getCount(), 30);

    // test time bound - the code checks every 50 children visited to see if the time bound has been hit and we give it 0 ms
    boundedDatasetCount = namespaceService.getDatasetCount(new NamespaceKey("a"), 0, 1000);
    assertTrue(boundedDatasetCount.isTimeBound());
  }

  @Test
  public void testDataSetSchema() throws Exception {
    Field field1 = new Field("a", new FieldType(true, new Int(32, true), null), null);
    Field child1 = new Field("c", new FieldType(true,  Utf8.INSTANCE, null), null);
    Field field2 = new Field("b", new FieldType(true, Struct.INSTANCE, null), ImmutableList.of(child1));
    Schema schema = new Schema(ImmutableList.of(field1, field2));
    FlatBufferBuilder builder = new FlatBufferBuilder();
    schema.getSchema(builder);
    builder.finish(schema.getSchema(builder));
    NamespaceTestUtils.addSource(namespaceService, "s");
    NamespaceTestUtils.addPhysicalDS(namespaceService, "s.foo", builder.sizedByteArray());
    ByteBuffer bb = ByteBuffer.wrap(DatasetHelper.getSchemaBytes(namespaceService.getDataset(new NamespaceKey(PathUtils.parseFullPath("s.foo")))).toByteArray());
    Schema returnedSchema = Schema.convertSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(bb));
    assertEquals(schema, returnedSchema);
  }

  @Test
  public void testRename() throws Exception {
    NamespaceTestUtils.addHome(namespaceService, "blue");
    NamespaceTestUtils.addSpace(namespaceService, "s");
    NamespaceTestUtils.addFolder(namespaceService, "s.a");
    NamespaceTestUtils.addFolder(namespaceService, "s.b");
    NamespaceTestUtils.addFolder(namespaceService, "s.a.b");
    NamespaceTestUtils.addFolder(namespaceService, "s.a.c");
    NamespaceTestUtils.addDS(namespaceService, "s.a.ds1");
    NamespaceTestUtils.addDS(namespaceService, "s.a.c.ds3");
    NamespaceTestUtils.addFolder(namespaceService, "s.c");
    NamespaceTestUtils.addFolder(namespaceService, "s.c.c");
    NamespaceTestUtils.addFolder(namespaceService, "s.c.c.c");
    NamespaceTestUtils.addFolder(namespaceService, "s.c.c.c.c");
    NamespaceTestUtils.addDS(namespaceService, "s.c.c.c.c.ds4");
    NamespaceTestUtils.addDS(namespaceService, "s.b.ds2");
    NamespaceTestUtils.addFolder(namespaceService, "@blue.a");
    NamespaceTestUtils.addFolder(namespaceService, "@blue.a.c");
    NamespaceTestUtils.addFile(namespaceService, asList("@blue", "a", "c", "file1"));
    NamespaceTestUtils.addSpace(namespaceService, "L");
    NamespaceTestUtils.addFolder(namespaceService, "L.F");
    NamespaceTestUtils.addFolder(namespaceService, "L.F.folder");
    NamespaceTestUtils.addDS(namespaceService, "L.F.ds");

    Map<String, NameSpaceContainer> items;

    final NamespaceKey namespaceKey = new NamespaceKey(PathUtils.parseFullPath("s.b.ds2"));

    final DatasetConfig oldConfig = namespaceService.getDataset(namespaceKey);
    // DX-35283: wait for a while to avoid flaky failure caused by the same getLastModified() of oldConfig and newConfig.
    Thread.sleep(1);
    final DatasetConfig newConfig = namespaceService.renameDataset(namespaceKey, new NamespaceKey(PathUtils.parseFullPath("s.b.ds22")));
    items = NamespaceTestUtils.listFolder(namespaceService, "s.b");
    assertEquals(1, items.size());
    assertTrue(items.containsKey("s.b.ds22"));
    assertTrue(newConfig.getLastModified() > oldConfig.getLastModified());
    assertEquals(newConfig.getCreatedAt(), oldConfig.getCreatedAt());

    namespaceService.renameDataset(new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds3")), new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds33")));
    items = NamespaceTestUtils.listFolder(namespaceService, "s.a.c");
    assertEquals(1, items.size());
    assertTrue(items.containsKey("s.a.c.ds33"));

    try {
      namespaceService.renameDataset(new NamespaceKey(PathUtils.parseFullPath(("@blue.a.c.file1"))),
          new NamespaceKey(PathUtils.parseFullPath(("s.a.c.file1DOTjson"))));
      Assert.fail("renames on physical datasets should not be allowed");
    } catch (final UserException ex) {
      // pass
    }

    items = NamespaceTestUtils.listFolder(namespaceService, "L.F");
    assertEquals(2, items.size());
    assertTrue(items.containsKey("L.F.ds"));
    assertTrue(items.containsKey("L.F.folder"));

    // Move dataset
    namespaceService.renameDataset(new NamespaceKey(PathUtils.parseFullPath("s.a.c.ds33")), new NamespaceKey(PathUtils.parseFullPath("L.F.ds33r")));
    items = NamespaceTestUtils.listFolder(namespaceService, "@blue.a.c");
    assertEquals(1, items.size());
    assertTrue(items.containsKey("\"@blue\".a.c.file1"));

    items = NamespaceTestUtils.listFolder(namespaceService, "L.F");
    assertEquals(3, items.size());
    assertTrue(items.containsKey("L.F.ds"));
    assertTrue(items.containsKey("L.F.folder"));
    assertTrue(items.containsKey("L.F.ds33r"));
    //System.out.println("L.F->" + items.keySet());
  }

  @Test
  public void insertingDifferentEntityTypesAtSamePath() throws Exception {
    NamespaceTestUtils.addSpace(namespaceService, "a");

    try {
      NamespaceTestUtils.addSource(namespaceService, "a");
    } catch(UserException ex) {
      assertTrue(ex.getMessage().contains("The current location already contains a space named \"a\". Please use a unique name for the new source."));
    }

    NamespaceTestUtils.addFolder(namespaceService, "a.foo");

    // Try to add dataset with path "a.foo"
    try {
      NamespaceTestUtils.addDS(namespaceService, "a.foo");
      fail("Expected the above call to fail");
    } catch (UserException ex) {
      assertTrue(ex.getMessage().contains("The current location already contains a folder named \"foo\". Please use a unique name for the new dataset."));
    }

    // Try to add folder with path "a.foo". There already a folder at "a.foo"
    try {
      NamespaceTestUtils.addFolder(namespaceService, "a.foo");
      fail("Expected the above call to fail");
    } catch (UserException ex) {
      assertTrue(ex.getMessage().contains("The current location already contains a folder named \"foo\". Please use a unique name for the new folder."));
    }
  }

  @Test
  public void testInvalidName() throws Exception {
    try {
      NamespaceTestUtils.addSpace(namespaceService, "TMP");
      fail("Expected the above call to fail");
    } catch (InvalidNamespaceNameException ignored) {
    }

    try {
      NamespaceTestUtils.addSource(namespaceService, "TMP");
      fail("Expected the above call to fail");
    } catch (InvalidNamespaceNameException ignored) {
    }
  }

  private void expectSplits(List<PartitionChunk> expectedSplits, NamespaceService ns, DatasetConfig datasetConfig) {
    Iterable<PartitionChunkMetadata> nsSplits = ns.findSplits(new LegacyIndexedStore.LegacyFindByCondition().setCondition(PartitionChunkId.getSplitsQuery(datasetConfig)));

    final ImmutableMap.Builder<PartitionChunkId, PartitionChunkMetadata> builder = ImmutableMap.builder();
    for (PartitionChunkMetadata nsSplit: nsSplits) {
      final PartitionChunkId splitId = PartitionChunkId.of(datasetConfig, nsSplit, datasetConfig.getReadDefinition().getSplitVersion());
      builder.put(splitId, nsSplit);
    }
    final ImmutableMap<PartitionChunkId, PartitionChunkMetadata> newSplitsMap = builder.build();
    assert(newSplitsMap.size() == expectedSplits.size());

    for (PartitionChunk partitionChunk : expectedSplits) {
      final PartitionChunkId splitId = PartitionChunkId.of(datasetConfig, partitionChunk, datasetConfig.getReadDefinition().getSplitVersion());
      final PartitionChunkMetadata newSplit = newSplitsMap.get(splitId);
      assertNotNull(newSplit);
      assertTrue(comparePartitionChunk(partitionChunk, newSplit, datasetConfig.getReadDefinition().getSplitVersion()));
    }
  }

  boolean comparePartitionChunk(PartitionChunk partitionChunkProto, PartitionChunkMetadata partitionChunkMetadata, long splitVersion) {
    return partitionChunkProto.getSize() == partitionChunkMetadata.getSize()
      && partitionChunkProto.getRowCount() == partitionChunkMetadata.getRowCount()
      && Objects.equals(partitionChunkProto.getAffinitiesList(), ImmutableList.copyOf(partitionChunkMetadata.getAffinities()))
      && Objects.equals(partitionChunkProto.getPartitionValuesList(), ImmutableList.copyOf(partitionChunkMetadata.getPartitionValues()))
      && partitionChunkProto.getSplitKey().equals(partitionChunkMetadata.getSplitKey())
      && Objects.equals(partitionChunkProto.getPartitionExtendedProperty(), partitionChunkMetadata.getPartitionExtendedProperty());
  }

  @Test
  public void testDeleteEntityNotFound() throws Exception {
    try {
      namespaceService.deleteEntityWithCallback(new NamespaceKey(Arrays.asList("does", "not", "exist")), "123", true, null);
      fail("deleteEntity should have failed.");
    } catch(NamespaceNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void testNamespaceContainerVersionExtractor() throws Exception {
    NameSpaceContainerVersionExtractor versionExtractor = new NameSpaceContainerVersionExtractor();

    NameSpaceContainer container = new NameSpaceContainer();
    container.setType(NameSpaceContainer.Type.SOURCE);

    SourceConfig config = new SourceConfig();
    container.setSource(config);

    // test precommit for sources, which increments the version
    versionExtractor.preCommit(container);
    assertEquals(0, config.getConfigOrdinal().longValue());

    versionExtractor.preCommit(container);
    assertEquals(1, config.getConfigOrdinal().longValue());

    // test preCommit rollback
    AutoCloseable autoCloseable = versionExtractor.preCommit(container);
    assertEquals(2, config.getConfigOrdinal().longValue());
    autoCloseable.close();
    assertEquals(1, config.getConfigOrdinal().longValue());
  }
}
