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

import java.util.Arrays;

import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.physical.config.Project;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.filter.FilterOperator;
import com.dremio.sabot.op.filter.FilterStats;
import com.dremio.sabot.op.llvm.expr.GandivaPushdownSieveHelper;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.ProjectorStats;
import com.google.common.collect.Lists;

import io.airlift.tpch.GenerationDefinition;

public class TestExcessiveSplits extends BaseTestOperator {

  private final ArrowType boolType = new ArrowType.Bool();
  private final ArrowType strType = new ArrowType.Utf8();
  private final ArrowType longType = new ArrowType.Int(64, false);
  private final ArrowType decimalType = new ArrowType.Decimal(2,0, 128);

  private final FunctionSignature equalFn = new FunctionSignature("equal", boolType, Lists.newArrayList(strType, strType));
  private final FunctionSignature isnullFn = new FunctionSignature("isnull", boolType, Lists.newArrayList(strType));
  private final FunctionSignature castDecimalFn = new FunctionSignature("castDECIMAL", decimalType,
    Lists.newArrayList(longType));

  private final GandivaPushdownSieveHelper gandivaPushdownSieveHelper = new GandivaPushdownSieveHelper();

  private static final String EXPR = "case when isnull(c_name) then false else isnull(case when " +
    "(isnull" +
    "(c_name) OR " +
    "equal(c_name,'nan') OR equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  " +
    "'WD') OR equal( c_name, 'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF') OR equal( c_name,  " +
    "'A-1+')) then c_address else  c_name end)  OR equal (case when (isnull(c_name) OR equal( c_name,  'nan') " +
    "OR equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR " +
    "equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) OR equal( c_name,  'A-1+')) " +
    "then c_address else  c_name  end,  'nan') OR equal(case when (  isnull(c_name) OR equal( c_name,  'nan') OR " +
    "equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) " +
    "OR equal( c_name,  'A-1+')) then c_address else  c_name  end,  'None') OR equal(case when (  isnull(c_name) OR equal( c_name,  'nan') OR equal( c_name,  'None')" +
    " OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) " +
    "OR equal( c_name,  'A-1+')) then c_address else  c_name  end,  '1') OR equal(case when (  isnull(c_name) OR equal( c_name,  'nan') OR equal( c_name,  'None') " +
    "OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' )" +
    " OR equal( c_name,  'A-1+')) then c_address else c_name end,  '1+') OR equal(case when  (  isnull(c_name) OR equal( c_name,  'nan') " +
    "OR equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') " +
    "OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) OR equal( c_name,  'A-1+')) then c_address else  c_name end,  'WD') " +
    "OR equal(case when  (  isnull( c_name) OR equal( c_name,  'nan') OR equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') " +
    "OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) OR equal( c_name,  'A-1+')) " +
    "then c_address else  c_name  end,  'NR') OR equal(case when  (  isnull( c_name) OR equal( c_name,  'nan') OR equal( c_name,  'None') " +
    "OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') " +
    "OR equal( c_name,  'PIF' ) OR equal( c_name,  'A-1+')) then c_address else  c_name  end,  'WR') " +
    "OR equal(case when  (  isnull( c_name) OR equal( c_name,  'nan') OR equal( c_name,  'None') OR equal( c_name,  '1')" +
    " OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) " +
    "OR equal( c_name,  'A-1+')) then c_address else  c_name  end,  'PIF' ) OR equal(case when  (  isnull( c_name) OR equal( c_name,  'nan')" +
    " OR equal( c_name,  'None') OR equal( c_name,  '1') OR equal( c_name,  '1+') OR equal( c_name,  'WD') OR equal( c_name,  'NR') OR equal( c_name,  'WR') OR equal( c_name,  'PIF' ) OR equal( c_name,  'A-1+')) then c_address else  c_name end , 'A+1') end";

  private static final String EXPR_DECIMAL = "case when equal( c_name,  '1+') then castDECIMAL(1l,2l, 0l) else " +
    "castDECIMAL('1', 2l, 0l) end";

  @Before
  public void cleanExpToExpSplitsCache() {
    testContext.invalidateExpToExpSplitsCache();
  }
  /*
   * Gandiva function bool equal(str, str) is hidden. This leads to too many splits
   * and the expression is evaluated in Java completely
   */
  @Test
  public void testExcessiveSplits() throws Exception {
    try {
      gandivaPushdownSieveHelper.addFunctionToHide(equalFn);

      Project project = new Project(PROPS, null, Arrays.asList(n(EXPR, "res")));
      OperatorStats stats = runSingle(project, ProjectOperator.class, GenerationDefinition.TpchTable
        .CUSTOMER_LIMITED, 6, 10);
      long gandivaExpr = stats.getLongStat(ProjectorStats.Metric.GANDIVA_EXPRESSIONS);
      long javaExpr = stats.getLongStat(ProjectorStats.Metric.JAVA_EXPRESSIONS);
      Assert.assertEquals(0, gandivaExpr);
      Assert.assertEquals(1, javaExpr);
    } finally {
      gandivaPushdownSieveHelper.removeFunctionToHide(equalFn);
    }
  }

  /*
   * Gandiva function bool equal(str, str) is hidden. This leads to too many splits
   * and the expression is evaluated in Java completely. A second expression is evaluated
   * completely in Gandiva
   */
  @Test
  public void testMultipleExprsWithOneExcessiveSplits() throws Exception {
    try {
      gandivaPushdownSieveHelper.addFunctionToHide(equalFn);

      Project project = new Project(PROPS, null, Arrays.asList(n(EXPR, "res"), n("isnull" +
        "(c_nationkey)", "res1")));
      OperatorStats stats = runSingle(project, ProjectOperator.class, GenerationDefinition.TpchTable
        .CUSTOMER_LIMITED, 6, 10);
      long gandivaExpr = stats.getLongStat(ProjectorStats.Metric.GANDIVA_EXPRESSIONS);
      long javaExpr = stats.getLongStat(ProjectorStats.Metric.JAVA_EXPRESSIONS);
      Assert.assertEquals(1, gandivaExpr);
      Assert.assertEquals(1, javaExpr);
    } finally {
      gandivaPushdownSieveHelper.removeFunctionToHide(equalFn);
    }
  }

  @Test
  public void testExcessiveSplitsFilter() throws Exception {
    try {
      gandivaPushdownSieveHelper.addFunctionToHide(equalFn);

      Filter filter = new Filter(PROPS, null, parseExpr(EXPR), 1.0f);
      OperatorStats stats = runSingle(filter, FilterOperator.class, GenerationDefinition.TpchTable
        .CUSTOMER_LIMITED, 6, 10);
      long gandivaExpr = stats.getLongStat(FilterStats.Metric.GANDIVA_EXPRESSIONS);
      long javaExpr = stats.getLongStat(FilterStats.Metric.JAVA_EXPRESSIONS);
      Assert.assertEquals(0, gandivaExpr);
      Assert.assertEquals(1, javaExpr);
    } finally {
      gandivaPushdownSieveHelper.removeFunctionToHide(equalFn);
    }
  }

  /*
   * Gandiva function bool isnull(str) is hidden. This leads to too many splits
   * but Gandiva still does enough work evaluating the equal() functions leading to a
   * mixed mode execution
   */
  @Test
  public void testExcessiveSplitsEnoughWork() throws Exception {
    try {
      gandivaPushdownSieveHelper.addFunctionToHide(isnullFn);

      Project project = new Project(PROPS, null, Arrays.asList(n(EXPR, "res")));
      OperatorStats stats = runSingle(project, ProjectOperator.class, GenerationDefinition.TpchTable
        .CUSTOMER_LIMITED, 6, 10);
      long gandivaExpr = stats.getLongStat(ProjectorStats.Metric.GANDIVA_EXPRESSIONS);
      long javaExpr = stats.getLongStat(ProjectorStats.Metric.JAVA_EXPRESSIONS);
      Assert.assertEquals(0, gandivaExpr);
      Assert.assertEquals(0, javaExpr);
      Assert.assertEquals(1, stats.getLongStat(ProjectorStats.Metric.MIXED_EXPRESSIONS));
    } finally {
      gandivaPushdownSieveHelper.removeFunctionToHide(isnullFn);
    }
  }

  /**
   * Force excessive splits and there by materialize the expression again.
   * The materialization should use original expression and not the one with code gen context.
   * @throws Exception
   */
  @Test
  public void testExcessiveSplitsMaterlization() throws Exception {
    try {
      gandivaPushdownSieveHelper.addFunctionToHide(castDecimalFn);
      gandivaPushdownSieveHelper.addFunctionToHide(equalFn);

      testContext.getOptions().setOption(OptionValue.createLong(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.MAX_SPLITS_PER_EXPR_KEY,
        1));
      testContext.getOptions().setOption(OptionValue.createDouble(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.WORK_THRESHOLD_FOR_SPLIT_KEY,
        100.0));


      Project project = new Project(PROPS, null, Arrays.asList(n(EXPR_DECIMAL, "res")));
      OperatorStats stats = runSingle(project, ProjectOperator.class, GenerationDefinition.TpchTable
        .CUSTOMER_LIMITED, 6, 10);
      long gandivaExpr = stats.getLongStat(ProjectorStats.Metric.GANDIVA_EXPRESSIONS);
      long javaExpr = stats.getLongStat(ProjectorStats.Metric.JAVA_EXPRESSIONS);
      Assert.assertEquals(0, gandivaExpr);
      Assert.assertEquals(1, javaExpr);
    } finally {
      gandivaPushdownSieveHelper.removeFunctionToHide(castDecimalFn);
      gandivaPushdownSieveHelper.removeFunctionToHide(equalFn);
      testContext.getOptions().setOption(OptionValue.createLong(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.MAX_SPLITS_PER_EXPR_KEY,
        ExecConstants.MAX_SPLITS_PER_EXPRESSION.getDefault().getNumVal()));
      testContext.getOptions().setOption(OptionValue.createDouble(
        OptionValue.OptionType.SYSTEM,
        ExecConstants.WORK_THRESHOLD_FOR_SPLIT_KEY,
        ExecConstants.WORK_THRESHOLD_FOR_SPLIT.getDefault().getFloatVal()));
    }
  }
}
