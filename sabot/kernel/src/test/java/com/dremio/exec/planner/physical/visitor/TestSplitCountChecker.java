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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.sys.SystemPluginConf;
import com.dremio.exec.store.sys.SystemScanPrel;
import com.dremio.exec.store.sys.SystemTable;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Unit test for SplitCountChecker
 */
public class TestSplitCountChecker {
  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  private RelOptCluster cluster;

  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    OptionList optionList = new OptionList();
    optionList.add(ExecConstants.SLICE_TARGET_OPTION.getDefault());
    optionList.add(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    optionList.add(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getDefault());
    final OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));
    when(optionManager.getOption(eq(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getOptionName())))
      .thenReturn(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault());
    when(optionManager.getNonDefaultOptions()).thenReturn(optionList);

    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);

    final PlannerSettings plannerSettings =
      new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG, optionManager, () -> info);
    cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings), rexBuilder);
  }

  private void verifySplits(Prel root, int querySplitLimit, int datasetSplitLimit, boolean expectedResult) {
    if (!expectedResult) {
      // Expect an exception
      thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, "Number of splits"));
    }
    SplitCountChecker.checkNumSplits(root, querySplitLimit, datasetSplitLimit);
    assertTrue("Expected verification failure. Instead, verification succeeded", expectedResult);
  }

  // Single scan, large limit. Expect success
  @Test
  public void simpleScan() throws Exception {
    Prel input =
      newScreen(
        newScan(rowType(), 500, 5)
      );
    verifySplits(input, 100, 100, true);
  }

  // Single scan, small dataset limit. Expect failure
  @Test
  public void singleScanDatasetLimit() throws Exception {
    Prel input =
      newScreen(
        newScan(rowType(), 500, 5)
      );
    verifySplits(input, 100, 3, false);
  }

  // Single scan, small query limit. Expect failure
  @Test
  public void singleScanQueryLimit() throws Exception {
    Prel input =
      newScreen(
        newScan(rowType(), 500, 5)
      );
    verifySplits(input, 3, 100, false);
  }

  // Two scans, large limit. Expect success
  @Test
  public void twoTablesLargeLimit() throws Exception {
    Prel input =
      newScreen(
        newJoin(
          newScan(rowType(), 500, 5),
          newScan(rowType(), 700, 10),
          rexBuilder.makeLiteral(true)
        )
      );
    verifySplits(input, 100, 100, true);
  }

  // Two scans, small query limit. Each dataset can squeeze through the dataset limit. Query can
  // squeeze through the query limit (but not the dataset limit). Expect success
  @Test
  public void twoTablesSmallLimit() throws Exception {
    Prel input =
      newScreen(
        newJoin(
          newScan(rowType(), 500, 10),
          newScan(rowType(), 700, 10),
          rexBuilder.makeLiteral(true)
        )
      );
    verifySplits(input, 25, 15, true);
  }

  // Two scans, small query limit. Expect failure
  @Test
  public void twoTablesQueryLimit() throws Exception {
    Prel input =
      newScreen(
        newJoin(
          newScan(rowType(), 500, 10),
          newScan(rowType(), 700, 10),
          rexBuilder.makeLiteral(true)
        )
      );
    verifySplits(input, 15, 15, false);
  }

  // Two scans, small dataset limit. Expect failure
  @Test
  public void twoTablesDatasetLimit() throws Exception {
    Prel input =
      newScreen(
        newJoin(
          newScan(rowType(), 500, 3),
          newScan(rowType(), 700, 10),
          rexBuilder.makeLiteral(true)
        )
      );
    verifySplits(input, 20, 5, false);
  }

  private Prel newScreen(Prel child) {
    return new ScreenPrel(cluster, traits, child);
  }

  private Prel newJoin(Prel left, Prel right, RexNode joinExpr) {
    return HashJoinPrel.create(cluster, traits, left, right, joinExpr, JoinRelType.INNER, JoinUtils.projectAll(left.getRowType().getFieldCount()+right.getRowType().getFieldCount()));
  }

  private Prel newScan(RelDataType rowType, double rowCount, int splitCount) throws Exception {
    TableMetadata metadata = Mockito.mock(TableMetadata.class);
    when(metadata.getName()).thenReturn(new NamespaceKey(ImmutableList.of("sys", "version")));
    when(metadata.getSchema()).thenReturn(SystemTable.VERSION.getRecordSchema());
    when(metadata.getSplitRatio()).thenReturn(0.75);
    when(metadata.getSplitCount()).thenReturn(splitCount);
    StoragePluginId pluginId = new StoragePluginId(new SourceConfig().setConfig(new SystemPluginConf().toBytesString()), new SystemPluginConf(), SourceCapabilities.NONE);
    when(metadata.getStoragePluginId()).thenReturn(pluginId);
    List<SchemaPath> columns = FluentIterable.from(SystemTable.VERSION.getRecordSchema()).transform(input -> SchemaPath.getSimplePath(input.getName())).toList();
    final RelOptTable relOptTable = Mockito.mock(RelOptTable.class);
    when(relOptTable.getRowCount()).thenReturn(rowCount);
    when(relOptTable.getQualifiedName()).thenReturn(ImmutableList.of("sys", "version"));
    return new SystemScanPrel(cluster, traits, relOptTable, metadata, columns, 1.0d, rowType);
  }

  private RelDataType rowType() {
    return typeFactory.createStructType(
      asList(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.DOUBLE)),
      asList("intCol", "doubleCol")
    );
  }
}
