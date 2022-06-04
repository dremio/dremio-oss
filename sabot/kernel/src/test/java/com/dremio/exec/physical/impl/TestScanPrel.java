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
package com.dremio.exec.physical.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.filter.RuntimeFilterId;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.exec.store.parquet.ParquetScanPrel;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;

/**
 * TestScanPrel
 */
public class TestScanPrel {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);

  @Test // Test case to ensure there is no missing information after copying a ScanPrel
  public void testDX44450() {
    final BatchSchema batchSchema = BatchSchema.newBuilder()
      .addField(CompleteType.VARCHAR.toField("a"))
      .addField(CompleteType.VARCHAR.toField("b"))
      .build();

    final RelDataType rowType = typeFactory.createStructType(
      ImmutableList.of(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
      ImmutableList.of("a","b"));

    final PhysicalDataset physicalDataset = PhysicalDataset.getDefaultInstance();
    final DatasetConfig datasetConfig = DatasetConfig.getDefaultInstance().setPhysicalDataset(physicalDataset);
    final TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getSchema()).thenReturn(batchSchema);
    when(tableMetadata.getDatasetConfig()).thenReturn(datasetConfig);
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build(), context, false,
      null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    RelTraitSet traitSet = RelTraitSet.createEmpty();
    RelOptTable table = mock(RelOptTable.class);
    when(table.getRowType()).thenReturn(rowType);
    StoragePluginId pluginId = mock(StoragePluginId.class);

    final List<SchemaPath> projectedColumns = ImmutableList.of(SchemaPath.getSimplePath("a"), SchemaPath.getSimplePath("b"));
    final ParquetScanFilter parquetScanFilter = new ParquetScanFilter(
      ImmutableList.of(
        new ParquetFilterCondition(SchemaPath.getSimplePath("a"), null, null, 0)));

    final boolean arrowCachingEnabled = true;
    final List<RuntimeFilteredRel.Info> runtimeFilters = ImmutableList.of(
      new RuntimeFilteredRel.Info(
        new RuntimeFilterId(1234L, false), null, "test", "test"));

    final double observedRowCountAdjustment = 0.5d;
    final double delta = 0.1;
    ParquetScanPrel scanPrel = new ParquetScanPrel(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowCountAdjustment, parquetScanFilter, arrowCachingEnabled, runtimeFilters);

    // test copy
    final ParquetScanPrel copiedScanPrel = (ParquetScanPrel) scanPrel.copy(traitSet, null);
    Assert.assertEquals(scanPrel.getCluster(), copiedScanPrel.getCluster());
    Assert.assertEquals(scanPrel.getTraitSet(), copiedScanPrel.getTraitSet());
    Assert.assertEquals(scanPrel.getTable(), copiedScanPrel.getTable());
    Assert.assertEquals(scanPrel.getPluginId(), copiedScanPrel.getPluginId());
    Assert.assertEquals(scanPrel.getTableMetadata(), copiedScanPrel.getTableMetadata());
    Assert.assertEquals(scanPrel.getProjectedColumns(), copiedScanPrel.getProjectedColumns());
    Assert.assertEquals(scanPrel.getProjectedColumns(), copiedScanPrel.getProjectedColumns());
    Assert.assertEquals(scanPrel.getProjectedColumns(), copiedScanPrel.getProjectedColumns());
    Assert.assertEquals(scanPrel.getCostAdjustmentFactor(), copiedScanPrel.getCostAdjustmentFactor(), delta);
    Assert.assertEquals(scanPrel.isArrowCachingEnabled(), copiedScanPrel.isArrowCachingEnabled());
    Assert.assertEquals(scanPrel.getRuntimeFilters(), copiedScanPrel.getRuntimeFilters());

    // test cloneWithProject
    final List<SchemaPath> newProjectedColumns = ImmutableList.of(SchemaPath.getSimplePath("a"));
    final ParquetScanPrel projectedScanPrel = scanPrel.cloneWithProject(newProjectedColumns);
    Assert.assertEquals(scanPrel.getCluster(), projectedScanPrel.getCluster());
    Assert.assertEquals(scanPrel.getTraitSet(), projectedScanPrel.getTraitSet());
    Assert.assertEquals(scanPrel.getTable(), projectedScanPrel.getTable());
    Assert.assertEquals(scanPrel.getPluginId(), projectedScanPrel.getPluginId());
    Assert.assertEquals(scanPrel.getTableMetadata(), projectedScanPrel.getTableMetadata());
    Assert.assertEquals(newProjectedColumns, projectedScanPrel.getProjectedColumns());
    Assert.assertEquals(scanPrel.getCostAdjustmentFactor(), projectedScanPrel.getCostAdjustmentFactor(), delta);
    Assert.assertEquals(scanPrel.isArrowCachingEnabled(), projectedScanPrel.isArrowCachingEnabled());
    Assert.assertEquals(scanPrel.getRuntimeFilters(), projectedScanPrel.getRuntimeFilters());
  }
}
