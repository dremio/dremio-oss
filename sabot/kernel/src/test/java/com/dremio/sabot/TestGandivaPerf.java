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
package com.dremio.sabot;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.physical.config.Project;
import com.dremio.options.OptionValue;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.filter.FilterOperator;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.ProjectorStats.Metric;
import com.dremio.sabot.op.spi.SingleInputOperator;
import io.airlift.tpch.GenerationDefinition.TpchTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/*
 * Run the same test (simple expression) on both gandiva and java, and compare the perf.
 * Ignoring test by default, since it can take very long to run. And, requires gandiva support.
 */
@Ignore
public class TestGandivaPerf extends BaseTestOperator {
  private final String PREFER_JAVA = "java";
  private final String PREFER_GANDIVA = "gandiva";
  private final int numThreads = 1;
  private final int batchSize = 16383;

  class RunWithPreference<T extends SingleInputOperator> implements Callable<OperatorStats> {
    TpchTable table;
    double scale;
    PhysicalOperator operator;
    Class<T> clazz;

    RunWithPreference(TpchTable table, double scale, PhysicalOperator operator, Class<T> clazz) {
      this.table = table;
      this.scale = scale;
      this.operator = operator;
      this.clazz = clazz;
    }

    @Override
    public OperatorStats call() throws Exception {
      return runSingle(operator, clazz, table, scale, batchSize);
    }
  }

  /*
   * Returns total evaluation time.
   */
  <T extends SingleInputOperator> long runOne(
      String preference,
      String expr,
      TpchTable table,
      double scale,
      PhysicalOperator operator,
      Class<T> clazz)
      throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(numThreads);

    testContext
        .getOptions()
        .setOption(
            OptionValue.createString(
                OptionValue.OptionType.SYSTEM, ExecConstants.QUERY_EXEC_OPTION_KEY, preference));

    List<Future<OperatorStats>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      Future<OperatorStats> ret =
          service.submit(new RunWithPreference(table, scale, operator, clazz));
      futures.add(ret);
    }

    long totalEvalTime = 0;
    long javaCodegenEvalTime = 0;
    long gandivaCodegenEvalTime = 0;
    for (Future<OperatorStats> future : futures) {
      OperatorStats stats = future.get();
      javaCodegenEvalTime += stats.getLongStat(Metric.JAVA_EVALUATE_TIME);
      gandivaCodegenEvalTime += stats.getLongStat(Metric.GANDIVA_EVALUATE_TIME);
    }
    totalEvalTime = javaCodegenEvalTime + gandivaCodegenEvalTime;
    System.out.println(
        "evaluate time with pref "
            + preference
            + " for ["
            + expr
            + "] is "
            + " ["
            + " eval  : "
            + (javaCodegenEvalTime + gandivaCodegenEvalTime)
            + "ms "
            + " javaCodeGen : "
            + javaCodegenEvalTime
            + "ms "
            + " gandivaCodeGen : "
            + gandivaCodegenEvalTime
            + "ms "
            + "]");
    return totalEvalTime;
  }

  /*
   * Returns delta of evaluation time as a %.
   */
  <T extends SingleInputOperator> int runBoth(
      String expr, TpchTable table, double scale, PhysicalOperator operator, Class clazz)
      throws Exception {

    long javaTime = runOne(PREFER_JAVA, expr, table, scale, operator, clazz);
    long gandivaTime = runOne(PREFER_GANDIVA, expr, table, scale, operator, clazz);

    int deltaPcnt = (int) (((javaTime - gandivaTime) * 100) / javaTime);

    System.out.println("delta for [" + expr + "] is " + deltaPcnt + "%");
    return deltaPcnt;
  }

  private int compareProject(TpchTable table, double scale, String expr) throws Exception {
    Project project = new Project(PROPS, null, Arrays.asList(n(expr, "res")));
    return runBoth(expr, table, scale, project, ProjectOperator.class);
  }

  private int compareFilter(TpchTable table, double scale, String expr) throws Exception {
    Filter filter = new Filter(PROPS, null, parseExpr(expr), 1f);
    return runBoth(expr, table, scale, filter, FilterOperator.class);
  }

  @Test
  public void testProjectAdd() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "c_custkey + c_nationkey");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectLike() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "like(c_name, '%PROMO%')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectBeginsWith() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "like(c_name, 'PROMO%')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractYear() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "extractYear(castDATE(c_acctbal))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastDate() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "castDATE(c_date)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastTimestamp() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "castTIMESTAMP(c_time)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectToDate() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "to_date(c_date, 'YYYY-MM-DD', 1)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectConcat() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "concat(c_name, c_mktsegment, c_comment)");
    Assert.assertTrue(delta > 0);
    delta =
        compareProject(
            TpchTable.CUSTOMER,
            6,
            "concat(c_name, c_mktsegment, c_name, c_address, c_comment, c_phone)");
    Assert.assertTrue(delta > 0);
    delta =
        compareProject(
            TpchTable.CUSTOMER,
            6,
            "concat(c_phone, c_name, c_comment, c_mktsegment, c_name, "
                + "c_address, c_comment, c_phone, c_mktsegment, c_address)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testFilterSimple() throws Exception {
    int delta = compareFilter(TpchTable.CUSTOMER, 6, "c_custkey < c_nationkey");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testFilterLike() throws Exception {
    int delta = compareFilter(TpchTable.CUSTOMER, 6, "like(c_name, '%PROMO%')");
    Assert.assertTrue(delta > 0);
  }
}
