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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusSubscriber;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Test class for NamespaceServiceImpl */
public class TestNamespaceServiceImpl extends DremioTest {
  private LocalKVStoreProvider kvStoreProvider;
  private LegacyKVStoreProvider legacyKVStoreProvider;
  private NamespaceServiceImpl ns;
  private CatalogStatusEvents catalogStatusEvents;
  private TestDatasetDeletionSubscriber testDatasetDeletionSubscriber;
  private NamespaceKey func1Key;

  @Before
  public void setup() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    legacyKVStoreProvider = kvStoreProvider.asLegacy();
    testDatasetDeletionSubscriber = new TestDatasetDeletionSubscriber();
    catalogStatusEvents = new CatalogStatusEventsImpl();
    catalogStatusEvents.subscribe(
        DatasetDeletionCatalogStatusEvent.getEventTopic(), testDatasetDeletionSubscriber);
    ns = new NamespaceServiceImpl(kvStoreProvider, catalogStatusEvents);
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

    assertEquals(
        "There should be two dataset deletion events fired.",
        2,
        testDatasetDeletionSubscriber.getTotalCount());
    assertEquals(
        String.format("There should be one dataset deletion fired for %s.", dataset1),
        1,
        testDatasetDeletionSubscriber.getCountPerPath(dataset1.toString()));
    assertEquals(
        String.format("There should be one dataset deletion fired for %s.", dataset2),
        1,
        testDatasetDeletionSubscriber.getCountPerPath(dataset2.toString()));
  }

  @Test
  public void testDeleteSpaceFiresDatasetDeletionEvent() throws Exception {
    final NamespaceKey spaceKey = new NamespaceKey("s1");
    ns.addOrUpdateSpace(spaceKey, newTestSpace("s1"));

    final NamespaceKey folderKey1 = new NamespaceKey(asList("s1", "fld1"));
    ns.addOrUpdateFolder(folderKey1, newTestFolder(spaceKey, "fld1"));

    final NamespaceKey dataset1 = new NamespaceKey(asList("s1", "fld1", "ds1"));
    ns.addOrUpdateDataset(dataset1, newTestVirtualDataset(folderKey1, "ds1", null, null, null));

    final NamespaceKey folderKey2 = new NamespaceKey(asList("s1", "fld1", "fld2"));
    ns.addOrUpdateFolder(folderKey2, newTestFolder(folderKey1, "fld2"));

    final NamespaceKey dataset2 = new NamespaceKey(asList("s1", "fld1", "fld2", "ds2"));
    ns.addOrUpdateDataset(dataset2, newTestVirtualDataset(folderKey2, "ds2", null, null, null));

    final SpaceConfig sourceConfig = ns.getSpace(spaceKey);
    ns.deleteSpace(spaceKey, sourceConfig.getTag());

    assertEquals(
        "There should be two dataset deletion events fired.",
        2,
        testDatasetDeletionSubscriber.getTotalCount());
    assertEquals(
        String.format("There should be one dataset deletion fired for %s.", dataset1),
        1,
        testDatasetDeletionSubscriber.getCountPerPath(dataset1.toString()));
    assertEquals(
        String.format("There should be one dataset deletion fired for %s.", dataset2),
        1,
        testDatasetDeletionSubscriber.getCountPerPath(dataset2.toString()));
  }

  @Test
  public void testChangeParentSource() throws NamespaceException {
    setUpViewsForAncestorChangeTest();

    DatasetConfig ds1 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view1")));
    List<ParentDataset> parents1 = Lists.newArrayList();
    parents1.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("Source1", "table1"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    parents1.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("Source1", "table3"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    ds1.getVirtualDataset().setParentsList(parents1);
    ns.addOrUpdateDataset(new NamespaceKey(ds1.getFullPathList()), ds1);

    DatasetConfig ds2 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view2")));
    assertEquals(true, ds2.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds3 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view3")));
    assertEquals(true, ds3.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds4 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view4")));
    assertNotEquals(true, ds4.getVirtualDataset().getSchemaOutdated());
  }

  @Test
  public void testChangeParentFields() throws NamespaceException {
    setUpViewsForAncestorChangeTest();

    DatasetConfig ds1 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view1")));
    List<ViewFieldType> fields1 = Lists.newArrayList();
    fields1.add(new ViewFieldType("field1a", "INT"));
    fields1.add(new ViewFieldType("field2", "INT"));
    ds1.getVirtualDataset().setSqlFieldsList(fields1);
    ns.addOrUpdateDataset(new NamespaceKey(ds1.getFullPathList()), ds1);

    DatasetConfig ds2 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view2")));
    assertEquals(true, ds2.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds3 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view3")));
    assertEquals(true, ds3.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds4 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view4")));
    assertNotEquals(true, ds4.getVirtualDataset().getSchemaOutdated());
  }

  @Test
  public void testParentNoMetadataChange() throws NamespaceException {
    setUpViewsForAncestorChangeTest();

    DatasetConfig ds1 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view1")));
    ns.addOrUpdateDataset(new NamespaceKey(ds1.getFullPathList()), ds1);

    DatasetConfig ds2 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view2")));
    assertNotEquals(true, ds2.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds3 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view3")));
    assertNotEquals(true, ds3.getVirtualDataset().getSchemaOutdated());

    DatasetConfig ds4 = ns.getDataset(new NamespaceKey(Lists.newArrayList("sp1", "view4")));
    assertNotEquals(true, ds4.getVirtualDataset().getSchemaOutdated());
  }

  @Test
  public void testAddOrUpdateDataset_fileCtimeIsSet() throws Exception {
    NamespaceKey sourceKey = new NamespaceKey("source");
    ns.addOrUpdateSource(sourceKey, newTestSource("source"));

    DatasetConfig datasetConfig = newTestPhysicalDataset(sourceKey, "dataset");
    assertNull(datasetConfig.getPhysicalDataset().getFormatSettings().getCtime());

    NamespaceKey datasetKey = new NamespaceKey(datasetConfig.getFullPathList());
    ns.addOrUpdateDataset(datasetKey, datasetConfig);

    Long ctime = ns.getDataset(datasetKey).getPhysicalDataset().getFormatSettings().getCtime();
    assertNotNull(ctime);
    assertTrue(System.currentTimeMillis() - 10000 < ctime && ctime <= System.currentTimeMillis());
  }

  @Test
  public void testList() throws Exception {
    // Create two sources, one will be with children, the other will serve as the boundary that
    // cannot be crossed.
    NamespaceKey sourceKey = new NamespaceKey("s1");
    ns.addOrUpdateSource(sourceKey, newTestSource("s1"));
    ns.addOrUpdateSource(new NamespaceKey("s2"), newTestSource("s2"));

    // Create folders in the first source.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    int numFolders = 100;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      NamespaceKey folderKey = new NamespaceKey(asList("s1", folderName));
      ns.addOrUpdateFolder(folderKey, newTestFolder(sourceKey, folderName));
      allExpectedChildrenPaths.add(String.format("%s.%s", sourceKey.getName(), folderName));
    }

    // Paginate over the first source.
    int maxResults = 13;
    String startChildName = null;
    List<String> allChildrenPaths = new ArrayList<>();
    do {
      List<NameSpaceContainer> children = ns.list(sourceKey, startChildName, maxResults + 1);

      assertTrue(children.size() <= maxResults + 1);

      startChildName = null;
      if (maxResults + 1 == children.size()) {
        NameSpaceContainer last = children.get(maxResults);
        startChildName = last.getFullPathList().get(last.getFullPathList().size() - 1);
        children = children.subList(0, maxResults);
      }

      children.forEach(c -> allChildrenPaths.add(String.join(".", c.getFullPathList())));
    } while (startChildName != null);

    assertEquals(allChildrenPaths, allExpectedChildrenPaths);
  }

  @Test
  public void testFind_keySort() {
    // Mock IndexedStore to be able to check sort options.
    IndexedStore<String, NameSpaceContainer> namespaceKvStore = mock(IndexedStore.class);
    NamespaceServiceImpl namespaceService =
        new NamespaceServiceImpl(
            new KVStoreProviderWithNamespace(namespaceKvStore, kvStoreProvider),
            catalogStatusEvents);

    // Test find with sort.
    when(namespaceKvStore.find(any(FindByCondition.class), any())).thenReturn(ImmutableList.of());
    namespaceService.find(new ImmutableFindByCondition.Builder().setPageSize(10).build());
    verify(namespaceKvStore, times(1))
        .find(
            eq(
                new ImmutableFindByCondition.Builder()
                    .setPageSize(10)
                    .addSort(
                        NamespaceIndexKeys.UNQUOTED_LC_PATH.toSortField(
                            SearchTypes.SortOrder.ASCENDING))
                    .build()));

    // Test find without sort.
    Mockito.reset(namespaceKvStore);
    when(namespaceKvStore.find(any(FindByCondition.class), any())).thenReturn(ImmutableList.of());
    namespaceService.find(
        new ImmutableFindByCondition.Builder().setPageSize(10).build(),
        new ImmutableEntityNamespaceFindOptions.Builder().setDisableKeySort(true).build());
    verify(namespaceKvStore, times(1))
        .find(eq(new ImmutableFindByCondition.Builder().setPageSize(10).build()));
  }

  @Test
  public void testListingFunctions() throws Exception {
    // Create a function at root level
    final NamespaceKey func1Key = new NamespaceKey("func1");
    ns.addOrUpdateFunction(func1Key, newTestFunction(func1Key));

    // Create a function in space sp1
    final NamespaceKey spaceKey = new NamespaceKey("sp1");
    ns.addOrUpdateSpace(spaceKey, newTestSpace("sp1"));
    final NamespaceKey func2Key = new NamespaceKey(Lists.newArrayList("sp1", "func2"));
    ns.addOrUpdateFunction(func2Key, newTestFunction(func2Key));

    // Create a function in folder f1 in space sp1
    final NamespaceKey folderKey = new NamespaceKey(Lists.newArrayList("sp1", "f1"));
    ns.addOrUpdateFolder(folderKey, newTestFolder(spaceKey, "f1"));
    final NamespaceKey func3Key = new NamespaceKey(Lists.newArrayList("sp1", "f1", "func3"));
    ns.addOrUpdateFunction(func3Key, newTestFunction(func3Key));

    assertEquals("There should be 1 UDF at the root level.", 1, ns.getTopLevelFunctions().size());
    assertEquals("There should be 3 UDFs in total at all levels.", 3, ns.getAllFunctions().size());
  }

  private SourceConfig newTestSource(String sourceName) {
    return new SourceConfig().setName(sourceName).setType("NAS").setCtime(1000L);
  }

  private SpaceConfig newTestSpace(String spaceName) {
    return new SpaceConfig().setName(spaceName);
  }

  private DatasetConfig newTestVirtualDataset(
      NamespaceKey parent,
      String name,
      List<ParentDataset> parents,
      List<ParentDataset> grandParents,
      List<ViewFieldType> fields) {
    DatasetConfig ds = new DatasetConfig();
    VirtualDataset vds = new VirtualDataset();
    vds.setVersion(DatasetVersion.newVersion());
    vds.setParentsList(parents);
    vds.setGrandParentsList(grandParents);
    vds.setSqlFieldsList(fields);
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

  private FunctionConfig newTestFunction(NamespaceKey namespaceKey) {
    return new FunctionConfig()
        .setName(namespaceKey.getName())
        .setFullPathList(namespaceKey.getPathComponents());
  }

  private List<String> path(NamespaceKey parent, String name) {
    List<String> path = Lists.newArrayList();
    path.addAll(parent.getPathComponents());
    path.add(name);
    return path;
  }

  private void setUpViewsForAncestorChangeTest() throws NamespaceException {
    NamespaceKey spaceKey1 = new NamespaceKey("sp1");
    ns.addOrUpdateSpace(spaceKey1, newTestSpace("sp1"));

    List<ParentDataset> parents1 = Lists.newArrayList();
    parents1.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("Source1", "table1"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    parents1.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("Source1", "table2"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    List<ViewFieldType> fields1 = Lists.newArrayList();
    fields1.add(new ViewFieldType("field1", "INT"));
    fields1.add(new ViewFieldType("field2", "INT"));
    DatasetConfig ds1 = newTestVirtualDataset(spaceKey1, "view1", parents1, null, fields1);
    ns.addOrUpdateDataset(new NamespaceKey(ds1.getFullPathList()), ds1);

    List<ParentDataset> parents2 = Lists.newArrayList();
    parents2.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("sp1", "view1"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    List<ParentDataset> grandParents2 = Lists.newArrayList();
    grandParents2.addAll(parents1);
    List<ViewFieldType> fields2 = Lists.newArrayList();
    DatasetConfig ds2 = newTestVirtualDataset(spaceKey1, "view2", parents2, grandParents2, fields2);
    ns.addOrUpdateDataset(new NamespaceKey(ds2.getFullPathList()), ds2);

    List<ParentDataset> parents3 = Lists.newArrayList();
    parents3.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("sp1", "view2"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    List<ParentDataset> grandParents3 = Lists.newArrayList();
    grandParents3.addAll(grandParents2);
    grandParents3.addAll(parents2);
    List<ViewFieldType> fields3 = Lists.newArrayList();
    DatasetConfig ds3 = newTestVirtualDataset(spaceKey1, "view3", parents3, grandParents3, fields3);
    ns.addOrUpdateDataset(new NamespaceKey(ds3.getFullPathList()), ds3);

    List<ParentDataset> parents4 = Lists.newArrayList();
    parents4.add(
        new ParentDataset()
            .setDatasetPathList(Lists.newArrayList("sp2", "view1"))
            .setType(DatasetType.VIRTUAL_DATASET)
            .setLevel(1));
    List<ViewFieldType> fields4 = Lists.newArrayList();
    DatasetConfig ds4 = newTestVirtualDataset(spaceKey1, "view4", parents4, null, fields4);
    ns.addOrUpdateDataset(new NamespaceKey(ds4.getFullPathList()), ds4);
  }

  private final class TestDatasetDeletionSubscriber implements CatalogStatusSubscriber {
    private int totalCount = 0;
    private final Map<String, Integer> pathToCount = new HashMap<>();

    @Override
    public void onCatalogStatusEvent(CatalogStatusEvent event) {
      if (!(event instanceof DatasetDeletionCatalogStatusEvent)) {
        return;
      }
      String datasetPath = ((DatasetDeletionCatalogStatusEvent) event).getDatasetPath();
      Integer previousCount = pathToCount.getOrDefault(datasetPath, 0);
      pathToCount.put(datasetPath, previousCount + 1);
      totalCount++;
    }

    public int getCountPerPath(String datasetPath) {
      return pathToCount.getOrDefault(datasetPath, 0);
    }

    public int getTotalCount() {
      return totalCount;
    }
  }

  /**
   * Helper class for mocking namespace store. Inherit from a provider not to override many other
   * methods.
   */
  private static class KVStoreProviderWithNamespace extends LocalKVStoreProvider {
    private final IndexedStore<String, NameSpaceContainer> namespaceStore;
    private final KVStoreProvider delegate;

    KVStoreProviderWithNamespace(
        IndexedStore<String, NameSpaceContainer> namespaceStore, KVStoreProvider delegate) {
      super(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
      this.namespaceStore = namespaceStore;
      this.delegate = delegate;
    }

    @Override
    public <K, V, T extends KVStore<K, V>> T getStore(
        Class<? extends StoreCreationFunction<K, V, T>> creator) {
      return creator.equals(NamespaceServiceImpl.NamespaceStoreCreator.class)
          ? (T) namespaceStore
          : delegate.getStore(creator);
    }
  }
}
