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
package com.dremio.exec.planner.cost;

import static com.dremio.test.dsl.RexDsl.and;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.notEq;
import static com.dremio.test.dsl.RexDsl.varcharInput;
import static org.mockito.Mockito.when;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.test.DremioTest;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestFilterCostEstimation {

  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Rel.LOGICAL);
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private static final RelDataType rowType =
      typeFactory.createStructType(
          List.of(
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR)),
          List.of("col1", "col2", "col3"));
  private static final DatasetConfig datasetConfig =
      new DatasetConfig()
          .setPhysicalDataset(
              new PhysicalDataset()
                  .setIcebergMetadata(new IcebergMetadata().setFileType(FileType.ICEBERG)));

  @Mock private ClusterResourceInformation clusterResourceInformation;
  @Mock private LegacyKVStore kvStore;
  @Mock private RelOptTable table;
  @Mock private StoragePluginId pluginId;
  @Mock private TableMetadata dataset;

  private OptionManager optionManager;

  @BeforeEach
  public void setup() throws Exception {
    final LegacyKVStoreProvider storeProvider =
        new LegacyKVStoreProvider() {
          @Override
          public <K, V, T extends LegacyKVStore<K, V>, U extends KVStore<K, V>> T getStore(
              Class<? extends LegacyStoreCreationFunction<K, V, T, U>> creator) {
            when(kvStore.find()).thenReturn(Collections.emptyList());
            return (T) kvStore;
          }

          @Override
          public void start() throws Exception {}

          @Override
          public void close() throws Exception {}
        };
    final OptionValidatorListing optionValidatorListing =
        new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT);
    SystemOptionManager som =
        new SystemOptionManager(
            optionValidatorListing,
            new LogicalPlanPersistence(DremioTest.CLASSPATH_SCAN_RESULT),
            () -> storeProvider,
            false);
    optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(new DefaultOptionManager(optionValidatorListing))
            .withOptionManager(som)
            .build();
    som.start();
  }

  /**
   *
   *
   * <pre>
   * This tests DX-86309. At the time, the partition filter factor was not reducing the plan cost
   * enough so that the planner would choose the plan with partition filter pushed down. The
   * scenario was as follows: For the below plan, the planner was not pushing the partition filter
   * part_col='A' down into the scan.
   *
   * Filter(condition=[AND(part_col='A', non_part_col1!='B', non_part_col2!='C')])
   *  Scan
   *
   * However, when the partition filter appeared last in the condition list like below, the planner
   * would push the partition filter down producing the optimal plan.
   *
   * Filter(condition=[AND(non_part_col1!='B', non_part_col2!='C', part_col='A')])
   *  Scan
   *
   * For this case, the planner was not using statistics, but was using general selectivity
   * estimates.
   * </pre>
   */
  @Test
  public void testPartitionFilterOrderWithoutStatistics() throws Exception {
    // Don't use statistics in cost estimation
    optionManager.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, PlannerSettings.USE_STATISTICS.getOptionName(), false));
    optionManager.setOption(
        OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM,
            PlannerSettings.USE_SEMIJOIN_COSTING.getOptionName(),
            false));

    // Filter with partition filter appearing first
    FilterRel filterWithPartitionFilterFirst =
        FilterRel.create(
            newScan(),
            and(
                eq(varcharInput(0), literal("A")),
                notEq(varcharInput(1), literal("B")),
                notEq(varcharInput(2), literal("C"))));

    // Filter with partition filter appearing last
    FilterRel filterWithPartitionFilterLast =
        FilterRel.create(
            newScan(),
            and(
                notEq(varcharInput(1), literal("B")),
                notEq(varcharInput(2), literal("C")),
                eq(varcharInput(0), literal("A"))));

    // Filter with partition filter pushed down into scan
    FilterRel filterPushedDown =
        FilterRel.create(
            newScan()
                .applyPartitionFilter(
                    new PruneFilterCondition(null, null, eq(varcharInput(0), literal("A"))),
                    null,
                    null),
            and(notEq(varcharInput(1), literal("B")), notEq(varcharInput(2), literal("C"))));

    // Filter with partition filter pushed down into scan should be cheaper than when partition
    // filter appears first in filter
    Assertions.assertThat(
            filterPushedDown
                .computeSelfCost(
                    filterPushedDown.getCluster().getPlanner(),
                    filterPushedDown.getCluster().getMetadataQuery())
                .isLt(
                    filterWithPartitionFilterFirst.computeSelfCost(
                        filterWithPartitionFilterFirst.getCluster().getPlanner(),
                        filterWithPartitionFilterFirst.getCluster().getMetadataQuery())))
        .isTrue();

    // Filter with partition filter pushed down into scan should be cheaper than when partition
    // filter appears last in filter
    Assertions.assertThat(
            filterPushedDown
                .computeSelfCost(
                    filterPushedDown.getCluster().getPlanner(),
                    filterPushedDown.getCluster().getMetadataQuery())
                .isLt(
                    filterWithPartitionFilterLast.computeSelfCost(
                        filterWithPartitionFilterLast.getCluster().getPlanner(),
                        filterWithPartitionFilterLast.getCluster().getMetadataQuery())))
        .isTrue();
  }

  private FilesystemScanDrel newScan() throws Exception {
    PlannerSettings plannerSettings =
        new PlannerSettings(
            DremioTest.DEFAULT_SABOT_CONFIG, optionManager, () -> clusterResourceInformation);
    RelOptPlanner planner = new VolcanoPlanner(new DremioCost.Factory(), plannerSettings);
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    cluster.setMetadataQuery(DremioRelMetadataQuery.QUERY_SUPPLIER);

    when(dataset.getDatasetConfig()).thenReturn(datasetConfig);
    when(dataset.getSplitRatio()).thenReturn(1.0d);
    when(dataset.getSchema()).thenReturn(CalciteArrowHelper.fromCalciteRowType(rowType));
    when(table.getRowCount()).thenReturn(1000d);

    return new FilesystemScanDrel(
        cluster,
        traits,
        table,
        pluginId,
        dataset,
        Stream.of("col1", "col2", "col3")
            .map(SchemaPath::getSimplePath)
            .collect(Collectors.toList()),
        1.0d,
        Collections.emptyList(),
        false,
        SnapshotDiffContext.NO_SNAPSHOT_DIFF);
  }
}
