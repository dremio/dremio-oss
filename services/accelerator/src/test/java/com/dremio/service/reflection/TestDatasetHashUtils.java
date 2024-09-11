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
package com.dremio.service.reflection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDatasetHashUtils {

  private CatalogService catalogService;
  private Catalog catalog;

  private TableMetadata t1TableMetadata;
  private ScanCrel t1Node;
  private DatasetConfig t1Config;
  private DremioTable t1Table;

  private DatasetConfig v1Config;
  private ExpansionNode v1Node;
  private DremioTable v1Table;

  private DatasetConfig v2Config;
  private ExpansionNode v2Node;
  private DremioTable v2Table;

  @Before
  public void setup() {
    catalogService = Mockito.mock(CatalogService.class);
    catalog = Mockito.mock(Catalog.class);
    when(catalogService.getCatalog(Mockito.any())).thenReturn(catalog);

    // Create datasetconfig and relnode for table t1
    t1Config =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setRecordSchema(ByteString.bytesDefaultValue("recordSchema"))
            .setFullPathList(ImmutableList.of("t1"));
    t1Table = mock(DremioTable.class);
    when(t1Table.getPath()).thenReturn(new NamespaceKey(ImmutableList.of("t1")));
    t1TableMetadata = mock(TableMetadata.class);
    when(t1Table.getDataset()).thenReturn(t1TableMetadata);
    when(t1Table.getDatasetConfig()).thenReturn(t1Config);
    t1Node = mock(ScanCrel.class);
    when(t1Node.accept(any(RelShuttle.class))).thenCallRealMethod();
    RelOptTable t1RelOptTable = mock(RelOptTable.class);
    when(t1RelOptTable.unwrap(DremioTable.class)).thenReturn(t1Table);
    when(t1Node.getTable()).thenReturn(t1RelOptTable);
    when(catalog.getTable(new NamespaceKey(ImmutableList.of("t1")))).thenReturn(t1Table);
    when(catalog.getTableSnapshot(
            CatalogEntityKey.fromNamespaceKey(new NamespaceKey(ImmutableList.of("t1")))))
        .thenReturn(t1Table);

    // Create datasetconfig and relnode for view v1
    v1Config =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setFullPathList(ImmutableList.of("v1"));
    VirtualDataset virtualDataset =
        new VirtualDataset().setSqlFieldsList(ImmutableList.of()).setSql("select * from t1");
    v1Config.setVirtualDataset(virtualDataset);
    v1Table = mock(DremioTable.class);
    when(v1Table.getDatasetConfig()).thenReturn(v1Config);
    v1Node = mock(ExpansionNode.class);
    when(v1Node.getPath()).thenReturn(new NamespaceKey(ImmutableList.of("v1")));
    when(v1Node.accept(any(RelShuttle.class))).thenCallRealMethod();
    ParentDataset parentDataset = new ParentDataset().setDatasetPathList(ImmutableList.of("t1"));
    virtualDataset.setParentsList(ImmutableList.of(parentDataset));
    when(catalog.getTable(new NamespaceKey("v1"))).thenReturn(v1Table);
    when(catalog.getTableSnapshot(CatalogEntityKey.fromNamespaceKey(new NamespaceKey("v1"))))
        .thenReturn(v1Table);
    when(v1Node.getInput(0)).thenReturn(t1Node);

    // Create datasetconfig and relnode for view v2
    v2Config =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setFullPathList(ImmutableList.of("v2"));
    VirtualDataset virtualDataset2 =
        new VirtualDataset().setSqlFieldsList(ImmutableList.of()).setSql("select * from v1");
    v2Config.setVirtualDataset(virtualDataset2);
    v2Table = mock(DremioTable.class);
    when(v2Table.getDatasetConfig()).thenReturn(v2Config);
    v2Node = mock(ExpansionNode.class);
    when(v2Node.getPath()).thenReturn(new NamespaceKey(ImmutableList.of("v2")));
    when(v2Node.accept(any(RelShuttle.class))).thenCallRealMethod();
    ParentDataset parentDataset2 = new ParentDataset().setDatasetPathList(ImmutableList.of("v1"));
    virtualDataset2.setParentsList(ImmutableList.of(parentDataset2));
    when(catalog.getTable(new NamespaceKey("v2"))).thenReturn(v2Table);
    when(catalog.getTableSnapshot(CatalogEntityKey.fromNamespaceKey(new NamespaceKey("v2"))))
        .thenReturn(v2Table);
    when(v2Node.getInput(0)).thenReturn(v1Node);
  }

  /**
   * Validate that {@link DatasetHashUtils.ParentDatasetBuilder} builds a mapping of views to parent
   * datasets
   */
  @Test
  public void testParentDatasetBuilderForNonVersionedDatasets() {
    DatasetHashUtils.ParentDatasetBuilder builder =
        new DatasetHashUtils.ParentDatasetBuilder(v2Node, catalog);
    List<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> parents =
        builder.getParents(SubstitutionUtils.VersionedPath.of(v2Config.getFullPathList(), null));
    assertEquals(1, parents.size());
    assertEquals(
        SubstitutionUtils.VersionedPath.of(ImmutableList.of("v1"), null), parents.get(0).left);
    assertEquals(v1Config, parents.get(0).right);

    parents = builder.getParents(SubstitutionUtils.VersionedPath.of(ImmutableList.of("v1"), null));
    assertEquals(1, parents.size());
    assertEquals(
        SubstitutionUtils.VersionedPath.of(ImmutableList.of("t1"), null), parents.get(0).left);
    assertEquals(t1Config, parents.get(0).right);
  }

  /**
   * Validate that {@link DatasetHashUtils.ParentDatasetBuilder} builds a mapping of views to parent
   * datasets
   */
  @Test
  public void testParentDatasetBuilderForVersionedDatasets() {
    TableVersionContext etlTableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "etl");
    when(t1TableMetadata.getVersionContext()).thenReturn(etlTableVersionContext);
    when(v1Node.getVersionContext()).thenReturn(etlTableVersionContext);
    when(catalog.getTableSnapshot(
            CatalogEntityKey.newBuilder()
                .keyComponents(ImmutableList.of("v1"))
                .tableVersionContext(etlTableVersionContext)
                .build()))
        .thenReturn(v1Table);
    when(catalog.getTableSnapshot(
            CatalogEntityKey.newBuilder()
                .keyComponents(ImmutableList.of("t1"))
                .tableVersionContext(etlTableVersionContext)
                .build()))
        .thenReturn(t1Table);

    DatasetHashUtils.ParentDatasetBuilder builder =
        new DatasetHashUtils.ParentDatasetBuilder(v2Node, catalog);
    List<Pair<SubstitutionUtils.VersionedPath, DatasetConfig>> parents =
        builder.getParents(SubstitutionUtils.VersionedPath.of(v2Config.getFullPathList(), null));
    assertEquals(1, parents.size());
    assertEquals(
        SubstitutionUtils.VersionedPath.of(ImmutableList.of("v1"), etlTableVersionContext),
        parents.get(0).left);
    assertEquals(v1Config, parents.get(0).right);

    parents =
        builder.getParents(
            SubstitutionUtils.VersionedPath.of(ImmutableList.of("v1"), etlTableVersionContext));
    assertEquals(1, parents.size());
    assertEquals(
        SubstitutionUtils.VersionedPath.of(ImmutableList.of("t1"), etlTableVersionContext),
        parents.get(0).left);
    assertEquals(t1Config, parents.get(0).right);
  }

  /**
   * Validate that computeDatasetHash using relNode tree produces same hash as using the dataset
   * config's parent dataset.
   *
   * @throws Exception
   */
  @Test
  public void testViewHashForNonVersionedDatasets() throws Exception {
    assertEquals(
        DatasetHashUtils.computeDatasetHash(v1Config, catalog, false),
        DatasetHashUtils.computeDatasetHash(catalog, v1Node, false));

    assertEquals(
        DatasetHashUtils.computeDatasetHash(v1Config, catalog, true),
        DatasetHashUtils.computeDatasetHash(catalog, v1Node, true));
  }

  /**
   * Validate that computeDatasetHash using relNode tree produces same hash as using the dataset
   * config's parent dataset.
   *
   * @throws Exception
   */
  @Test
  public void testNestedViewHashForNonVersionedDatasets() throws Exception {
    assertEquals(
        DatasetHashUtils.computeDatasetHash(v2Config, catalog, false),
        DatasetHashUtils.computeDatasetHash(catalog, v2Node, false));

    assertEquals(
        DatasetHashUtils.computeDatasetHash(v2Config, catalog, true),
        DatasetHashUtils.computeDatasetHash(catalog, v2Node, true));
  }

  /**
   * Validate that computeDatasetHash using relNode tree produces same hash as using the dataset
   * config's parent dataset.
   *
   * @throws Exception
   */
  @Test
  public void testTableHashForNonVersionedDatasets() throws Exception {
    assertEquals(
        DatasetHashUtils.computeDatasetHash(t1Config, catalog, false),
        DatasetHashUtils.computeDatasetHash(catalog, t1Node, false));

    assertEquals(
        DatasetHashUtils.computeDatasetHash(t1Config, catalog, true),
        DatasetHashUtils.computeDatasetHash(catalog, t1Node, true));
  }

  /** Validate that hashes are different for same table from different branches */
  @Test
  public void testTableHashForVersionedDatasets() {
    // Update t1 to be branch main
    when(t1TableMetadata.getVersionContext())
        .thenReturn(new TableVersionContext(TableVersionType.BRANCH, "main"));
    when(catalog.getTableSnapshot(
            CatalogEntityKey.newBuilder().keyComponents(ImmutableList.of("t1")).build()))
        .thenReturn(t1Table);

    // Create datasetconfig and relnode for table t1 at branch dev
    DatasetConfig t1ConfigAtDev =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setRecordSchema(ByteString.bytesDefaultValue("recordSchema_dev"));
    DremioTable t1TableAtDev = mock(DremioTable.class);
    when(t1TableAtDev.getPath()).thenReturn(new NamespaceKey(ImmutableList.of("t1")));
    TableMetadata t1TableMetadataAtDev = mock(TableMetadata.class);
    when(t1TableAtDev.getDataset()).thenReturn(t1TableMetadataAtDev);
    when(t1TableAtDev.getDatasetConfig()).thenReturn(t1ConfigAtDev);
    ScanCrel t1NodeAtDev = mock(ScanCrel.class);
    when(t1NodeAtDev.accept(any(RelShuttle.class))).thenCallRealMethod();
    RelOptTable t1RelOptTableAtDev = mock(RelOptTable.class);
    when(t1RelOptTableAtDev.unwrap(DremioTable.class)).thenReturn(t1TableAtDev);
    when(t1NodeAtDev.getTable()).thenReturn(t1RelOptTableAtDev);
    when(catalog.getTableSnapshot(
            CatalogEntityKey.newBuilder().keyComponents(ImmutableList.of("t1")).build()))
        .thenReturn(t1TableAtDev);

    assertNotEquals(
        DatasetHashUtils.computeDatasetHash(catalog, t1Node, false),
        DatasetHashUtils.computeDatasetHash(catalog, t1NodeAtDev, false));

    assertNotEquals(
        DatasetHashUtils.computeDatasetHash(catalog, t1Node, true),
        DatasetHashUtils.computeDatasetHash(catalog, t1NodeAtDev, true));
  }
}
