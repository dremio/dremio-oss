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
package com.dremio.sabot.filter;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.proto.UserBitShared.ExpressionSplitInfo;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.util.BatchPrinter;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.filter.FilterOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public class TestSimpleFilter extends BaseTestOperator {

  @Test
  public void simpleFilter() throws Exception {

    Filter f = new Filter(PROPS, null, toExpr("c0 < 10"), 1f);
    Table input = t(
        th("c0"),
        tr(35),
        tr(8),
        tr(22)
        );

    Table output = t(
        th("c0"),
        tr(8)
        );

    validateSingle(f, FilterOperator.class, input, output);
  }

  @Test
  public void simpleFilter2() throws Exception {

    Filter f = new Filter(PROPS, null, toExpr("c0 < c1"), 1f);
    Table input = t(
      th("c0", "c1"),
      tr(35, 45),
      tr(8, 6),
      tr(22, 23)
    );

    Table output = t(
      th("c0","c1"),
      tr(35, 45),
      tr(22, 23)
    );

    validateSingle(f, FilterOperator.class, input, output);
  }

  /**
   * This gives wrong results. Filter operator should handle SV2, but apparently it doesn't
   *
   * @throws Exception
   */
  @Test
  @Ignore
  public void filterOnFilter() throws Exception {
    Filter f = new Filter(PROPS, null, toExpr("mod(n_nationkey, 2) = 0"), 1f);
    FilterOperator op = newOperator(FilterOperator.class, f, 100);
    Generator gen = TpchGenerator.singleGenerator(TpchTable.NATION, 1, getTestAllocator());
    VectorAccessible f1Output = op.setup(gen.getOutput());
    Filter f2 = new Filter(PROPS, null, toExpr("mod(n_nationkey, 3) = 0"), 1f);
    FilterOperator op2 = newOperator(FilterOperator.class, f2, 100);
    VectorAccessible finalOutput = op2.setup(f1Output);
    gen.next(100);
    op.consumeData(100);
    op.outputData();
    BatchPrinter.printBatch(f1Output);
    op2.consumeData(100);
    op2.outputData();
    BatchPrinter.printBatch(finalOutput);
  }

  @Test
  public void varcharFilter() throws Exception {

    Filter f = new Filter(PROPS, null, toExpr("like(c0, 'hell%')"), 1f);
    Table input = t(
        th("c0"),
        tr("hello"),
        tr("bye")
        );

    Table output = t(
        th("c0"),
        tr("hello")
        );

    validateSingle(f, FilterOperator.class, input, output);
  }

  @Test
  public void strlenFilter() throws Exception {
    LogicalExpression expr = toExpr("(length(c0) + length(c1)) > 10");
    Filter f = new Filter(PROPS, null, expr, 1f);
    Table input = t(
      th("c0", "c1"),
      tr("hello", "world"),
      tr("good", "morning"),
      tr("bye", "bye"),
      tr("happy", "birthday")
    );

    Table output = t(
      th("c0", "c1"),
      tr("good", "morning"),
      tr("happy", "birthday")
    );

    validateSingle(f, FilterOperator.class, input, output);
  }

  @Test
  public void optimisationInFilter() throws Exception {
    // Small Filter
    LogicalExpression expr = toExpr("c0 = 1 or c1 = 2");
    Filter f = new Filter(PROPS, null, expr, 1f);
    Table input = t(
      th("c0", "c1"),
      tr(1, 3),
      tr(1, 2),
      tr(5, 6),
      tr(3, 4)
    );

    Table output = t(
      th("c0", "c1"),
      tr(1, 3),
      tr(1, 2)
    );
    OperatorStats stats = validateSingle(f, FilterOperator.class, input.toGenerator(getTestAllocator()), output, 4000);
    List<ExpressionSplitInfo> splitInfoList = stats.getProfile(true).getDetails().getSplitInfosList();

    Assert.assertFalse(splitInfoList.isEmpty());
    Assert.assertTrue(splitInfoList.get(0).getOptimize());

    // Large Filter
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    int i = 8000;
    sb.append("c0 < 10");
    while (i-- > 0) {
      sb.append(" OR c0 < ").append(random.nextInt());
    }
    expr = toExpr(sb.toString());
    f = new Filter(PROPS, null, expr, 1f);
    input = t(
      th("c0"),
      tr(1)
    );

    output = t(
      th("c0"),
      tr(1)
    );

    stats = validateSingle(f, FilterOperator.class, input.toGenerator(getTestAllocator()), output, 4095);
    splitInfoList = stats.getProfile(true).getDetails().getSplitInfosList();

    Assert.assertFalse(splitInfoList.isEmpty());
    Assert.assertFalse(splitInfoList.get(0).getOptimize());
  }
}
