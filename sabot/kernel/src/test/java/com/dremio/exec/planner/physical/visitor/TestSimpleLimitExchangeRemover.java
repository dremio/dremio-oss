/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.exec.planner.sql.SqlConverter.TYPE_SYSTEM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.OldScanPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.dremio.exec.store.ischema.InfoSchemaGroupScan;
import com.dremio.exec.store.ischema.InfoSchemaTableType;
import com.dremio.exec.store.sys.SystemTable;
import com.dremio.exec.store.sys.SystemTablePlugin;
import com.dremio.exec.store.sys.SystemTableScan;
import com.google.common.collect.Lists;

public class TestSimpleLimitExchangeRemover {

  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Prel.PHYSICAL);
  private static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(TYPE_SYSTEM);
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  private OptionManager optionManager;
  private PlannerSettings plannerSettings;
  private RelOptCluster cluster;

  @Before
  public void setup() {
    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(ExecConstants.SLICE_TARGET)))
        .thenReturn(ExecConstants.SLICE_TARGET_OPTION.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())))
        .thenReturn(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getOptionName())))
        .thenReturn(PlannerSettings.ENABLE_TRIVIAL_SINGULAR.getDefault());

    plannerSettings = new PlannerSettings(optionManager, null);
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
    verifyOutput(output, "Screen", "Project", "UnionExchange", "Project", "OldScan");
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
    verifyOutput(output, "Screen", "Limit", "Project", "Limit", "Project", "OldScan");
  }


  @Test
  public void simpleSelectWithLimitWithSoftScanWithLeafLimitsEnabled() {
    when(optionManager.getOption(eq(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())))
        .thenReturn(OptionValue.createBoolean(OptionType.QUERY, PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName(), true));

    try {
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
      verifyOutput(output, "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "OldScan");
    } finally {
      when(optionManager.getOption(eq(PlannerSettings.ENABLE_LEAF_LIMITS.getOptionName())))
          .thenReturn(PlannerSettings.ENABLE_LEAF_LIMITS.getDefault());
    }
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
        "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "OldScan");
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
    verifyOutput(output, "Screen", "Limit", "Project", "UnionExchange", "Limit", "Project", "OldScan");
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
        "Screen", "Limit", "Project", "UnionExchange", "HashJoin", "Project", "OldScan", "Project", "OldScan");
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
    return new ProjectPrel(cluster, traits, child, exprs, rowType);
  }

  private Prel newHardScan(RelDataType rowType) {
    return new OldScanPrel(cluster, traits, new SystemTableScan(SystemTable.MEMORY, (SystemTablePlugin)null), rowType, asList("sys", "memory"));
  }

  private Prel newSoftScan(RelDataType rowType) {
    return new OldScanPrel(cluster, traits, new InfoSchemaGroupScan(InfoSchemaTableType.TABLES, null, (SabotContext)null), rowType, asList("col1", "col2"));
  }

  private RelDataType rowType() {
    return typeFactory.createStructType(
        asList(typeFactory.createSqlType(SqlTypeName.INTEGER), typeFactory.createSqlType(SqlTypeName.DOUBLE)),
        asList("intCol", "doubleCol")
    );
  }

  private List<RexNode> exprs() {
    return asList(
        (RexNode) new RexInputRef(0, typeFactory.createSqlType(SqlTypeName.INTEGER)),
        (RexNode) new RexInputRef(1, typeFactory.createSqlType(SqlTypeName.DOUBLE))
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
    try {
      return new HashJoinPrel(cluster, traits, left, right, rexBuilder.makeLiteral(true), JoinRelType.INNER);
    } catch (Exception e) {
      fail();
    }
    return null;
  }
}
