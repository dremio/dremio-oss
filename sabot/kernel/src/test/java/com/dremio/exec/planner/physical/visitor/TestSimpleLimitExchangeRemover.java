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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
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
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.sys.SystemPluginConf;
import com.dremio.exec.store.sys.SystemScanPrel;
import com.dremio.exec.store.sys.SystemTable;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestSimpleLimitExchangeRemover {

  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  private OptionManager optionManager;
  private OptionList optionList;
  private PlannerSettings plannerSettings;
  private RelOptCluster cluster;

  @Before
  public void setup() {
    optionList = new OptionList();
    optionList.add(ExecConstants.SLICE_TARGET_OPTION.getDefault());
    optionList.add(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    optionList.add(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getDefault());
    optionManager = mock(OptionManager.class);

    when(optionManager.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));
    when(optionManager.getNonDefaultOptions()).thenReturn(optionList);
    when(optionManager.getOption(eq(ExecConstants.SLICE_TARGET)))
      .thenReturn(ExecConstants.SLICE_TARGET_OPTION.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getOptionName())))
      .thenReturn(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())))
      .thenReturn(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getOptionName())))
      .thenReturn(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getDefault());
    ClusterResourceInformation info = mock(ClusterResourceInformation.class);
    when(info.getExecutorNodeCount()).thenReturn(1);

    plannerSettings = new PlannerSettings(DremioTest.DEFAULT_SABOT_CONFIG,
      optionManager, () -> info);
    cluster = RelOptCluster.create(new VolcanoPlanner(plannerSettings), rexBuilder);
  }

  @Test
  public void simpleSelectNoLimit() {
    Prel input =
        newScreen(
            newProject(exprs(), rowType(),
                newUnionExchange(
                    newProject(exprs(), rowType(),
                        newSoftScan(rowType())
                    )
                )
            )
        );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output, "Screen", "Project", "UnionExchange", "Project", "SystemScan");
  }

  @Test
  public void simpleSelectWithLimitWithSoftScan() {
    Prel input =
        newScreen(
            newLimit(0, 10,
                newProject(exprs(), rowType(),
                    newUnionExchange(
                        newLimit(0, 10,
                            newProject(exprs(), rowType(),
                                newSoftScan(rowType())
                            )
                        )
                    )
                )
            )
        );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output, "Screen", "Limit", "Project", "Limit", "Project", "SystemScan");
  }


  @Test
  public void simpleSelectWithLimitWithSoftScanWithLeafLimitsEnabled() {
    OptionValue optionEnabled = OptionValue.createBoolean(OptionValue.OptionType.QUERY, PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName(), true);
    when(optionManager.getOption(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())).thenReturn(optionEnabled);
    optionList.remove(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    optionList.add(optionEnabled);

    Prel input =
      newScreen(
        newLimit(0, 10,
          newProject(exprs(), rowType(),
            newUnionExchange(
              newLimit(0, 10,
                newProject(exprs(), rowType(),
                  newSoftScan(rowType())
                )
              )
            )
          )
        )
      );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output, "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "SystemScan");
  }

  @Test
  public void simpleSelectWithLargeLimitWithSoftScan() {
    Prel input =
        newScreen(
            newLimit(0, 200000,
                newProject(exprs(), rowType(),
                    newUnionExchange(
                        newLimit(0, 200000,
                            newProject(exprs(), rowType(),
                                newSoftScan(rowType())
                            )
                        )
                    )
                )
            )
        );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output,
        "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "SystemScan");
  }

  @Test
  public void simpleSelectWithLimitWithHardScan() {
    Prel input =
        newScreen(
            newLimit(0, 10,
                newProject(exprs(), rowType(),
                    newUnionExchange(
                        newLimit(0, 10,
                            newProject(exprs(), rowType(),
                                newHardScan(rowType())
                            )
                        )
                    )
                )
            )
        );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output, "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "SystemScan");
  }

  @Test
  public void joinWithLimitWithSoftScan() {
    Prel input =
        newScreen(
            newLimit(0, 10,
                newProject(exprs(), rowType(),
                    newUnionExchange(
                        newJoin(
                            newProject(exprs(), rowType(),
                                newSoftScan(rowType())
                            ),
                            newProject(exprs(), rowType(),
                                newSoftScan(rowType())
                            )
                        )
                    )
                )
            )
        );

    Prel output = SimpleLimitExchangeRemover.apply(plannerSettings, input);
    verifyOutput(output,
        "Screen", "Limit", "Project", "UnionExchange", "HashJoin", "Project", "SystemScan", "Project", "SystemScan");
  }

  private void verifyOutput(Prel output, String... expRels) {
    final List<String> actRels = Lists.newArrayList();
    addRels(actRels, output);
    assertEquals(asList(expRels), actRels);
  }

  private void addRels(List<String> list, Prel input) {
    String descr = input.getDescription();
    list.add(descr.substring(0, descr.lastIndexOf("Prel")));
    for(Prel child : input) {
      addRels(list, child);
    }
  }

  private Prel newScreen(Prel child) {
    return new ScreenPrel(cluster, traits, child);
  }

  private Prel newProject(List<RexNode> exprs, RelDataType rowType, Prel child) {
    return ProjectPrel.create(cluster, traits, child, exprs, rowType);
  }

  private Prel newHardScan(RelDataType rowType) {
    TableMetadata metadata = Mockito.mock(TableMetadata.class);
    when(metadata.getName()).thenReturn(new NamespaceKey(ImmutableList.of("sys", "memory")));
    when(metadata.getSchema()).thenReturn(SystemTable.MEMORY.getRecordSchema());
    StoragePluginId pluginId = new StoragePluginId(new SourceConfig().setConfig(new SystemPluginConf().toBytesString()), new SystemPluginConf(), SourceCapabilities.NONE);
    when(metadata.getStoragePluginId()).thenReturn(pluginId);

    List<SchemaPath> columns = FluentIterable.from(SystemTable.MEMORY.getRecordSchema()).transform(new Function<Field, SchemaPath>(){
      @Override
      public SchemaPath apply(Field input) {
        return SchemaPath.getSimplePath(input.getName());
      }}).toList();
    return new SystemScanPrel(cluster, traits, Mockito.mock(RelOptTable.class), metadata, columns, 1.0d, rowType);
  }

  private Prel newSoftScan(RelDataType rowType) {
    TableMetadata metadata = Mockito.mock(TableMetadata.class);
    when(metadata.getName()).thenReturn(new NamespaceKey(ImmutableList.of("sys", "version")));
    when(metadata.getSchema()).thenReturn(SystemTable.VERSION.getRecordSchema());
    StoragePluginId pluginId = new StoragePluginId(new SourceConfig().setConfig(new SystemPluginConf().toBytesString()), new SystemPluginConf(), SourceCapabilities.NONE);
    when(metadata.getStoragePluginId()).thenReturn(pluginId);
    List<SchemaPath> columns = FluentIterable.from(SystemTable.VERSION.getRecordSchema()).transform(new Function<Field, SchemaPath>(){
      @Override
      public SchemaPath apply(Field input) {
        return SchemaPath.getSimplePath(input.getName());
      }}).toList();
    return new SystemScanPrel(cluster, traits, Mockito.mock(RelOptTable.class), metadata, columns, 1.0d, rowType);
  }

  private RelDataType rowType() {
    return typeFactory.createStructType(
        asList(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.DOUBLE)),
        asList("intCol", "doubleCol")
    );
  }

  private List<RexNode> exprs() {
    return ImmutableList.<RexNode>of(
        rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER)),
        rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.DOUBLE))
    );
  }

  private Prel newUnionExchange(Prel child) {
    return new UnionExchangePrel(cluster, traits, child);
  }

  private Prel newLimit(int offset, int fetch, Prel child) {
    return new LimitPrel(cluster, traits, child,
        rexBuilder.makeBigintLiteral(new BigDecimal(offset)),
        rexBuilder.makeBigintLiteral(new BigDecimal(fetch))
    );
  }

  private Prel newJoin(Prel left, Prel right) {
    return HashJoinPrel.create(cluster, traits, left, right, rexBuilder.makeLiteral(true), JoinRelType.INNER, JoinUtils.projectAll(left.getRowType().getFieldCount()+right.getRowType().getFieldCount()));
  }
}
