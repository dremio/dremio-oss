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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createStockIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.loadTable;
import static com.dremio.exec.store.iceberg.OptimizeManifestsTableFunction.NO_CLUSTERING_RULE;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.context.UserContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.dremio.exec.planner.sql.OptimizeTests;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Collections;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.junit.Test;

/** Tests for {@link OptimizeManifestsTableFunction} */
public class TestOptimizeManifestsTableFunction extends BaseTestQuery {
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testCleanOrphans() throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(SOURCE, 0, 2, "clean_orphans")) {
      OptimizeTests.insertCommits(table, 5);

      Table icebergTable = loadTable(table);
      Snapshot originalSnapshot = icebergTable.currentSnapshot();
      Snapshot inProgressSnapshot =
          icebergTable.rewriteManifests().clusterBy(NO_CLUSTERING_RULE).apply();

      OptimizeManifestsTableFunction.cleanOrphans(icebergTable.io(), inProgressSnapshot);

      OptimizeTests.assertNoOrphanManifests(icebergTable, originalSnapshot);
    }
  }

  @Test
  public void testHasNoManifestRewritten() {
    Snapshot snapshot = mock(Snapshot.class);

    when(snapshot.summary()).thenReturn(Collections.EMPTY_MAP);
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    when(snapshot.summary()).thenReturn(ImmutableMap.of("manifests-created", "0"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    when(snapshot.summary()).thenReturn(ImmutableMap.of("manifests-replaced", "0"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    when(snapshot.summary())
        .thenReturn(ImmutableMap.of("manifests-created", "0", "manifests-replaced", "0"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    // Inconsistent cases, when manifests replaced XOR manifests created
    when(snapshot.summary()).thenReturn(ImmutableMap.of("manifests-created", "5"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    when(snapshot.summary()).thenReturn(ImmutableMap.of("manifests-replaced", "5"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();
  }

  @Test
  public void testHasNoManifestValid() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created", "10", "manifests-replaced", "1", "total-data-files", "10"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isFalse();

    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created",
                "10",
                "manifests-replaced",
                "2",
                "total-data-files",
                "10",
                "total-delete-files",
                "1"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isFalse();

    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created", "2", "manifests-replaced", "2", "total-data-files", "10"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isFalse();

    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created",
                "3",
                "manifests-replaced",
                "3",
                "total-data-files",
                "10",
                "total-delete-files",
                "1"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isFalse();
  }

  @Test
  public void testHasNoManifestRewrittenResidualFiles() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created", "1", "manifests-replaced", "1", "total-data-files", "10"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();

    when(snapshot.summary())
        .thenReturn(
            ImmutableMap.of(
                "manifests-created",
                "2",
                "manifests-replaced",
                "2",
                "total-data-files",
                "10",
                "total-delete-files",
                "10"));
    assertThat(OptimizeManifestsTableFunction.hasNoManifestChanges(snapshot)).isTrue();
  }

  @Test
  public void testOptimizeManifestFailureOnCommit() throws Exception {
    try (DmlQueryTestUtils.Table table = createStockIcebergTable(SOURCE, 0, 2, "clean_orphans")) {
      OptimizeTests.insertCommits(table, 5);

      Table icebergTable = spy(loadTable(table));
      Snapshot snapshot = icebergTable.currentSnapshot();
      RewriteManifests rewriteManifestSpy = spy(icebergTable.rewriteManifests());
      doThrow(new CommitFailedException("Forced failure of the commit"))
          .when(rewriteManifestSpy)
          .commit();
      when(icebergTable.rewriteManifests()).thenReturn(rewriteManifestSpy);

      OptimizeManifestsTableFunction optimizeManifestsTableFunction =
          initializeWithMocks(icebergTable);

      assertThatThrownBy(() -> optimizeManifestsTableFunction.noMoreToConsume())
          .isInstanceOf(UserException.class)
          .hasMessage("Error while rewriting table manifests.");

      icebergTable.refresh();
      assertThat(icebergTable.currentSnapshot().snapshotId()).isEqualTo(snapshot.snapshotId());

      // Ensure no orphans
      Snapshot[] allSnapshots = Iterables.toArray(icebergTable.snapshots(), Snapshot.class);
      OptimizeTests.assertNoOrphanManifests(icebergTable, allSnapshots);
    }
  }

  private OptimizeManifestsTableFunction initializeWithMocks(Table icebergTable)
      throws ExecutionSetupException {
    FragmentExecutionContext fec = mock(FragmentExecutionContext.class);
    OperatorContext context = mock(OperatorContext.class);
    OpProps opProps = mock(OpProps.class);
    TableFunctionConfig tableFunctionConfig = mock(TableFunctionConfig.class);

    OptimizeManifestsTableFunctionContext ctx = mock(OptimizeManifestsTableFunctionContext.class);
    when(tableFunctionConfig.getFunctionContext()).thenReturn(ctx);

    StoragePluginId pluginId = mock(StoragePluginId.class);
    when(ctx.getPluginId()).thenReturn(pluginId);

    SupportsIcebergMutablePlugin icebergMutablePlugin = mock(SupportsIcebergMutablePlugin.class);
    when(fec.getStoragePlugin(any(StoragePluginId.class))).thenReturn(icebergMutablePlugin);

    IcebergTableProps tableProps = mock(IcebergTableProps.class);
    when(ctx.getIcebergTableProps()).thenReturn(tableProps);

    when(opProps.getUserName()).thenReturn(UserContext.SYSTEM_USER_CONTEXT.getUserId());

    IcebergModel icebergModel = mock(IcebergModel.class);
    when(icebergMutablePlugin.getIcebergModel(
            any(IcebergTableProps.class),
            anyString(),
            any(OperatorContext.class),
            any(FileIO.class)))
        .thenReturn(icebergModel);
    doNothing().when(icebergModel).refreshVersionContext();
    IcebergTableIdentifier icebergTableIdentifier = mock(IcebergTableIdentifier.class);
    when(icebergModel.getTableIdentifier(anyString())).thenReturn(icebergTableIdentifier);
    when(icebergModel.getIcebergTable(any(IcebergTableIdentifier.class))).thenReturn(icebergTable);

    OperatorStats operatorStats = mock(OperatorStats.class);
    doNothing().when(operatorStats).addLongStat(any(MetricDef.class), anyLong());
    when(context.getStats()).thenReturn(operatorStats);
    when(icebergModel.propertyAsLong(any(IcebergTableIdentifier.class), anyString(), anyLong()))
        .thenReturn(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);

    return new OptimizeManifestsTableFunction(fec, context, opProps, tableFunctionConfig);
  }
}
