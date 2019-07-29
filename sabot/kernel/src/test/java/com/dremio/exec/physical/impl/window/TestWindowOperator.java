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
package com.dremio.exec.physical.impl.window;

import static com.dremio.exec.physical.impl.window.DataPar.dataB1P1;
import static com.dremio.exec.physical.impl.window.DataPar.dataB1P2;
import static com.dremio.exec.physical.impl.window.DataPar.dataB2P2;
import static com.dremio.exec.physical.impl.window.DataPar.dataB2P4;
import static com.dremio.exec.physical.impl.window.DataPar.dataB3P2;
import static com.dremio.exec.physical.impl.window.DataPar.dataB4P4;
import static com.dremio.exec.physical.impl.window.WindowGenerator.generateInput;
import static com.dremio.exec.physical.impl.window.WindowGenerator.generateOutput;
import static com.dremio.sabot.Fixtures.t;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Order;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.physical.config.WindowPOP.Bound;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.windowframe.WindowFrameOperator;

public class TestWindowOperator extends BaseTestOperator {

  private WindowPOP createWindowPOP(boolean withPartitionBy, boolean withOrderBy) {
    List<NamedExpression> withins = withPartitionBy ? singletonList(n("position_id")) : Collections.<NamedExpression>emptyList();
    List<NamedExpression> aggregations = withOrderBy ?
      Arrays.asList(
        n("sum(salary)", "sum"),
        n("count(position_id)", "count"),
        n("row_number()", "row_number"),
        n("rank()", "rank"),
        n("dense_rank()", "dense_rank"),
        n("cume_dist()", "cume_dist"),
        n("percent_rank()", "percent_rank")
      )
      :
      Arrays.asList(n("sum(salary)", "sum"), n("count(position_id)", "count"));
    List<Order.Ordering> orderings = withOrderBy ? singletonList(ordering("sub", DESCENDING, FIRST)) : Collections.<Order.Ordering>emptyList();
    return new WindowPOP(PROPS, null, withins, aggregations, orderings, false, new Bound(true, Long.MIN_VALUE), new Bound(false, 0));
  }

  private void validateWindow(DataPar[] dataDef, boolean withPartitionBy, boolean withOrderBy) throws Exception {
    final WindowPOP window = createWindowPOP(withPartitionBy, withOrderBy);
    final Table input = t(WindowGenerator.header, generateInput(dataDef));
    validateSingle(window, WindowFrameOperator.class, input, generateOutput(dataDef, withOrderBy), 20);
  }

  private void runTests(DataPar[] withPartitionDef, DataPar[] withoutPartitionDef) throws Exception {
    validateWindow(withPartitionDef, true, true);
    validateWindow(withPartitionDef, true, false);
    validateWindow(withoutPartitionDef, false, true);
    validateWindow(withoutPartitionDef, false, false);
  }

  @Test
  public void testB1P1() throws Exception {
    runTests(dataB1P1(), dataB1P1());
  }

  @Test
  public void testB1P2() throws Exception {
    runTests(dataB1P2(true), dataB1P2(false));
  }

  @Test
  public void testB2P2() throws Exception {
    runTests(dataB2P2(true), dataB2P2(false));
  }

  @Test
  public void testB2P4() throws Exception {
    runTests(dataB2P4(true), dataB2P4(false));
  }

  @Test
  public void testB3P2() throws Exception {
    runTests(dataB3P2(true), dataB3P2(false));
  }

  @Test
  public void testB4P4() throws Exception {
    runTests(dataB4P4(true), dataB4P4(false));
  }

  @Test // DRILL-4657
  public void test4657() throws Exception {
    // SELECT row_number() OVER(ORDER BY position_id) rn, rank() OVER(ORDER BY position_id) rnk FROM dfs.\"%s/window/b3.p2\"
    final WindowPOP window = new WindowPOP(PROPS, null,
      Collections.<NamedExpression>emptyList(), // withins
      Arrays.asList(n("row_number()", "rn"), n("rank()", "rnk")), // aggregations
      singletonList(ordering("position_id", DESCENDING, FIRST)), // ordering
      false, new Bound(true, Long.MIN_VALUE), new Bound(false, 0));

    final DataPar[] partitions = dataB3P2(true);
    final Table input = Fixtures.split(WindowGenerator.header, 20, generateInput(partitions));
    final Table output = Fixtures.t(WindowGenerator.header4657, WindowGenerator.generateOutput4657(partitions));
    validateSingle(window, WindowFrameOperator.class, input, output, 20);
  }

}
