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
package com.dremio.exec.planner.physical.visitor;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergManifestListPrel;
import com.dremio.exec.store.iceberg.ManifestContentType;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRootPointerSubstitutionVisitor {

  private final DatasetConfig metadata0 =
      new DatasetConfig()
          .setPhysicalDataset(
              new PhysicalDataset()
                  .setFormatSettings(new FileConfig().setType(FileType.ICEBERG))
                  .setIcebergMetadata(
                      new IcebergMetadata()
                          .setMetadataFileLocation("Location0")
                          .setSnapshotId(0L)));

  private final DatasetConfig metadata1 =
      new DatasetConfig()
          .setPhysicalDataset(
              new PhysicalDataset()
                  .setFormatSettings(new FileConfig().setType(FileType.ICEBERG))
                  .setIcebergMetadata(
                      new IcebergMetadata()
                          .setMetadataFileLocation("Location1")
                          .setSnapshotId(1L)));

  @Mock private RelOptCluster clusterMock;
  @Mock private RexBuilder rexBuilderMock;
  @Mock private RelTraitSet traitsMock;
  @Mock private StoragePluginId pluginIdMock;
  @Mock private SplitsPointer splitsMock;
  @Mock private RelDataType typeMock;
  @Mock private BatchSchema schemaMock;
  @Mock private RelOptTable tableMock;
  @Mock private TableFunctionConfig configMock;
  @Mock private RexLiteral conditionMock;

  @Before
  public void setup() {
    when(clusterMock.getRexBuilder()).thenReturn(rexBuilderMock);
    when(rexBuilderMock.makeLiteral(anyBoolean())).thenReturn(conditionMock);
    when(traitsMock.replaceIfs(any(), any())).thenReturn(traitsMock);
    when(conditionMock.isAlwaysTrue()).thenReturn(true);
  }

  @Test
  public void testMetadataSubstituted() {
    // Create a tree consisting of two IcebergManifestListPrel leaves, both referencing the same
    // metadata.
    final TableMetadata tableMetadata =
        new TableMetadataImpl(pluginIdMock, metadata0, "USER", splitsMock, ImmutableList.of());
    final Prel tree = createPrelTree(tableMetadata);

    // Simulate root pointer metadata swap.
    final List<DatasetConfig> replaced = new ArrayList<>();
    final Map<String, DatasetConfig> swap = ImmutableMap.of("Location0", metadata1);
    final Prel newTree = RootPointerSubstitutionVisitor.substitute(tree, swap, replaced);

    // Assert that a new tree was produced with the new metadata replaced in each
    // IcebergManifestListPrel.
    final IcebergManifestListPrel leftManifestList =
        (IcebergManifestListPrel) newTree.getInput(0).getInput(0);
    final IcebergManifestListPrel rightManifestList =
        (IcebergManifestListPrel) newTree.getInput(1).getInput(0);
    assertNotSame(tree, newTree);
    assertSame(metadata1, replaced.get(0));
    assertSame(metadata1, leftManifestList.getTableMetadata().getDatasetConfig());
    assertSame(metadata1, rightManifestList.getTableMetadata().getDatasetConfig());
  }

  @Test
  public void testMetadataNotSubstituted() {
    // Create a tree consisting of two IcebergManifestListPrel leaves, both referencing the same
    // metadata.
    final TableMetadata tableMetadata =
        new TableMetadataImpl(pluginIdMock, metadata0, "USER", splitsMock, ImmutableList.of());
    final Prel tree = createPrelTree(tableMetadata);

    // Try to swap metadata that is not present in tree.
    final List<DatasetConfig> replaced = new ArrayList<>();
    final Map<String, DatasetConfig> swap = ImmutableMap.of("Location1", metadata0);
    final Prel newTree = RootPointerSubstitutionVisitor.substitute(tree, swap, replaced);

    // Assert that the original tree was not mutated and the original metadata is present.
    final IcebergManifestListPrel leftManifestList =
        (IcebergManifestListPrel) newTree.getInput(0).getInput(0);
    final IcebergManifestListPrel rightManifestList =
        (IcebergManifestListPrel) newTree.getInput(1).getInput(0);
    assertSame(tree, newTree);
    assertTrue(replaced.isEmpty());
    assertSame(metadata0, leftManifestList.getTableMetadata().getDatasetConfig());
    assertSame(metadata0, rightManifestList.getTableMetadata().getDatasetConfig());
  }

  private Prel createPrelTree(TableMetadata tableMetadata) {
    final IcebergManifestListPrel manifestList0 =
        new IcebergManifestListPrel(
            clusterMock,
            traitsMock,
            tableMetadata,
            schemaMock,
            ImmutableList.of(),
            typeMock,
            null,
            ManifestContentType.DATA);
    final IcebergManifestListPrel manifestList1 =
        new IcebergManifestListPrel(
            clusterMock,
            traitsMock,
            tableMetadata,
            schemaMock,
            ImmutableList.of(),
            typeMock,
            null,
            ManifestContentType.DATA);
    final TableFunctionPrel leftScan =
        new TableFunctionPrel(
            clusterMock, traitsMock, tableMock, manifestList0, tableMetadata, configMock, typeMock);
    final TableFunctionPrel rightScan =
        new TableFunctionPrel(
            clusterMock, traitsMock, tableMock, manifestList1, tableMetadata, configMock, typeMock);
    return NestedLoopJoinPrel.create(
        clusterMock, traitsMock, leftScan, rightScan, JoinRelType.INNER, conditionMock);
  }
}
