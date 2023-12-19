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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.sys.SystemPluginConf;
import com.dremio.exec.store.sys.SystemScanPrel;
import com.dremio.exec.store.sys.SystemTable;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link RelMdRowCount}
 */
public class TestRelMdRowCount {
  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private LegacyKVStoreProvider storeProvider;
  private RelOptCluster cluster;

  @Before
  public void setup() throws Exception {
    final LegacyKVStoreProvider storeProvider = new LegacyKVStoreProvider() {
      @Override
      public <K, V, T extends LegacyKVStore<K, V>, U extends KVStore<K, V>>
      T getStore(Class<? extends LegacyStoreCreationFunction<K, V, T, U>> creator) {
        LegacyKVStore<?,?> store = mock(LegacyKVStore.class);
        when(store.find()).thenReturn(Collections.emptyList());
        return (T) store;
      }

      @Override
      public void start() throws Exception {

      }

      @Override
      public void close() throws Exception {

      }
    };
    final OptionValidatorListing optionValidatorListing = new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT);
    SystemOptionManager som = new SystemOptionManager(optionValidatorListing, new LogicalPlanPersistence(DremioTest.DEFAULT_SABOT_CONFIG, DremioTest.CLASSPATH_SCAN_RESULT), () -> storeProvider, false);
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(optionValidatorListing))
      .withOptionManager(som)
      .build();
    som.start();

    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);
    PlannerSettings plannerSettings =
      new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionManager, () -> info);
    cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings), rexBuilder);
    cluster.setMetadataQuery(DremioRelMetadataQuery.QUERY_SUPPLIER);
  }

  @Test
  public void simpleScan() throws Exception {
    Prel input =
      newScreen(
        newProject(exprs(), rowType(),
          newUnionExchange(
            newProject(exprs(), rowType(),
              newScan(rowType(), 500, 1.0 /* split ratio */)
            )
          )
        )
      );

    verifyCount(500d, input);
  }

  @Test
  public void simpleScanPrunedPartitions() throws Exception {
    Prel input =
      newScreen(
        newProject(exprs(), rowType(),
          newUnionExchange(
            newProject(exprs(), rowType(),
              newScan(rowType(), 500, 0.75 /* split ratio */)
            )
          )
        )
      );

    verifyCount(500 * 0.75, input);
  }

  @Test
  public void joinCartesian() throws Exception {
    Prel input =
      newScreen(
        newProject(exprs(), rowType(),
          newUnionExchange(
            newJoin(
              newProject(exprs(), rowType(), newScan(rowType(), 2_000, 1.0d)),
              newProject(exprs(), rowType(), newScan(rowType(), 5_000, 1.0d)),
              rexBuilder.makeLiteral(true) // cartesian
            )
          )
        )
      );

    verifyCount(10_000_000d /* max rowCount from */, input);
  }

  @Test
  public void joinEquality() throws Exception {
    Prel left = newProject(exprs(), rowType(), newScan(rowType(), 2_000, 1.0d));
    Prel right = newProject(exprs(), rowType(), newScan(rowType(), 5_000, 1.0d));
    RexNode joinExpr = rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(left, 1),
        rexBuilder.makeInputRef(right.getRowType().getFieldList().get(1).getType(), 3)
    );

    Prel input =
      newScreen(
        newProject(exprs(), rowType(),
          newUnionExchange(
            newJoin(left, right, joinExpr)
          )
        )
      );

    verifyCount(5_000d /* max rowCount from */, input);
  }

  @Test
  public void joinNonEqui() throws Exception {
    Prel left = newProject(exprs(), rowType(), newScan(rowType(), 2_000, 1.0d));
    Prel right = newProject(exprs(), rowType(), newScan(rowType(), 5_000, 1.0d));
    RexNode joinExpr = rexBuilder.makeCall(
        SqlStdOperatorTable.LESS_THAN,
        rexBuilder.makeInputRef(left, 1),
        rexBuilder.makeInputRef(right.getRowType().getFieldList().get(1).getType(), 3)
    );

    Prel input =
      newScreen(
        newProject(exprs(), rowType(),
          newUnionExchange(
            newJoin(left, right, joinExpr)
          )
        )
      );

    verifyCount(2_500d /* max rowCount from */, input);
  }

  private void verifyCount(Double expected, Prel input) {
    final RelMetadataQuery metadataQuery = input.getCluster().getMetadataQuery();
    Double rowCountFromGet = metadataQuery.getRowCount(input);
    assertEquals(expected, rowCountFromGet.doubleValue(), 0.0d);

    Double rowCountFromEstimateRowCount = input.estimateRowCount(metadataQuery);
    assertEquals(expected, rowCountFromEstimateRowCount.doubleValue(), 0.0d);
  }

  private Prel newScreen(Prel child) {
    return new ScreenPrel(cluster, traits, child);
  }

  private Prel newProject(List<RexNode> exprs, RelDataType rowType, Prel child) {
    return ProjectPrel.create(cluster, traits, child, exprs, rowType);
  }

  private Prel newScan(RelDataType rowType, double rowCount, double splitRatio) throws Exception {
    TableMetadata metadata = Mockito.mock(TableMetadata.class);
    when(metadata.getName()).thenReturn(new NamespaceKey(ImmutableList.of("sys", "version")));
    when(metadata.getSchema()).thenReturn(SystemTable.VERSION.getRecordSchema());
    when(metadata.getSplitRatio()).thenReturn(splitRatio);
    StoragePluginId pluginId = new StoragePluginId(new SourceConfig().setConfig(new SystemPluginConf().toBytesString()), new SystemPluginConf(), SourceCapabilities.NONE);
    when(metadata.getStoragePluginId()).thenReturn(pluginId);
    List<SchemaPath> columns = FluentIterable.from(SystemTable.VERSION.getRecordSchema()).transform(input -> SchemaPath.getSimplePath(input.getName())).toList();
    final RelOptTable relOptTable = Mockito.mock(RelOptTable.class);
    when(relOptTable.getRowCount()).thenReturn(rowCount);
    return new SystemScanPrel(cluster, traits, relOptTable, metadata, columns, 1.0d, ImmutableList.of(), rowType,
                              ImmutableList.of());
  }

  private RelDataType rowType() {
    return typeFactory.createStructType(
        asList(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.DOUBLE)),
        asList("intCol", "doubleCol")
    );
  }

  private List<RexNode> exprs() {
    return ImmutableList.of(
        rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER)),
        rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.DOUBLE))
    );
  }

  private Prel newUnionExchange(Prel child) {
    return new UnionExchangePrel(cluster, traits, child);
  }

  private Prel newJoin(Prel left, Prel right, RexNode joinExpr) {
    return HashJoinPrel.create(cluster, traits, left, right, joinExpr, null, JoinRelType.INNER);
  }
}
