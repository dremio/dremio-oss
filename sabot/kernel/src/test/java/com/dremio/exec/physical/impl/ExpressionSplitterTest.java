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

import static com.dremio.sabot.Fixtures.NULL_BIGINT;
import static com.dremio.sabot.Fixtures.date;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.ts;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.dremio.sabot.Fixtures;

/**
 * Unit test cases for the expression splitter
 */
public class ExpressionSplitterTest extends BaseExpressionSplitterTest {
  String ifQuery = "case when c0 > c1 then c0 - c1 else c1 + c0 end";

  // Evaluates the expression with a batch size of 2 for the inputs below
  // This evaluates the expression twice
  Fixtures.Table ifInput = Fixtures.split(
    th("c0", "c1"),
    2,
    tr(10, 11),
    tr(4, 3),
    tr(5, 5)
  );

  Fixtures.Table ifOutput = t(
    th("out"),
    tr(21),
    tr(1),
    tr(10)
  );

  @Test
  public void noSplitIf() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract", "add");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)", 1, 0)
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitCondJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("subtract", "add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitCondGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitThenJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "add");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (add(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (subtract(c0, c1)) else (_xxx1) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitThenGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("subtract");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (_xxx1) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitElseJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (_xxx1) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitElseGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (add(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (subtract(c0, c1)) else (_xxx1) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void testNestedIfInFunction() throws Exception {
    // if (c0 > c1) then (((c0 > 10) ? 10 : c0) + c0) else (c1 + c0)
    String query = "case when c0 > c1 then (case when c0 > 10 then 10i else c0 end) + c0 else c1 + c0 end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(10, 11),
      tr(4, 3),
      tr(11, 5)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(21),
      tr(8),
      tr(21)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (greater_than(c0, 10i)) else(cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (add((if (_xxx1) then (10i) else (c0) end), c0)) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void testNestedIfInFunctionSplitThen() throws Exception {
    // if (c0 > c1) then (((c0 > 10) ? 10 : c0) + c0) else (c1 - c0)
    String query = "case when c0 > c1 then (case when c0 > 10 then 10i else c0 end) + c0 else c1 - c0 end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(10, 11),
      tr(4, 3),
      tr(11, 5)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(1),
      tr(8),
      tr(21)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 3),
      new Split(true, "_xxx1", "(if (_xxx0) then ((if (greater_than(c0, 10i)) then (10i) else (c0) end)) else(cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (subtract(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx3", "(if (_xxx0) then (add(_xxx1, c0)) else (_xxx2) end)", 3, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void ifTimestamp() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("less_than", "greater_than", "booleanAnd");
    String query = "case when (c0 < castTIMESTAMP(698457600000l) AND (c1 > castTIMESTAMP(909619200000l))) then (castTIMESTAMP(1395822273000l))  else (castTIMESTAMP(1395822273999l)) end";
    Fixtures.Table input = Fixtures.t(
      th("c0", "c1"),
      tr(date("2001-01-01"), date("2001-12-31"))
    );

    Fixtures.Table output = Fixtures.t(
      th("out"),
      tr(ts("2014-03-26T08:24:33.999"))
    );

    Split[] splits = {
      new Split(false, "_xxx0", "casttimestamp(c0)", 1, 1),
      new Split(false, "_xxx1", "casttimestamp(698457600000l)", 1, 1),
      new Split(true, "_xxx2", "less_than(_xxx0, _xxx1)", 2, 3, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx2) then (casttimestamp(c1)) else (cast((__$internal_null$__) as timestamp)) end)", 3, 1, "_xxx2"),
      new Split(false, "_xxx4", "(if (_xxx2) then (casttimestamp(909619200000l)) else (cast((__$internal_null$__) as timestamp)) end)", 3, 1, "_xxx2"),
      new Split(true, "_xxx5", "(if (_xxx2) then(greater_than(_xxx3, _xxx4)) else (false) end)", 4, 1, "_xxx2", "_xxx3", "_xxx4"),
      new Split(false, "_xxx6", "(if (_xxx5) then(casttimestamp(1395822273000l)) else(casttimestamp(1395822273999l)) end)", 5, 0, "_xxx5")
    };
    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testConstantExpression() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("castTIMESTAMP");
    String query = "case " +
      "when castTIMESTAMP(698457600000l) > 3l then castTIMESTAMP(1395822273999l) " +
      "else castTIMESTAMP(1396762453000l) end";

    Fixtures.Table input = Fixtures.t(
      th("c0"),
      tr(date("2001-01-01"))
    );

    Fixtures.Table output = Fixtures.t(
      th("out"),
      tr(ts("2014-03-26T08:24:33.999"))
    );

    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(castTIMESTAMP(698457600000l), castTIMESTAMP(3l))", 1, 1),
      new Split(true, "_xxx1",
        "(if (_xxx0 ) then (castTIMESTAMP(1395822273999l))  else (castTIMESTAMP(1396762453000l)) end)", 2, 0, "_xxx0")
    };
    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testFunctionArgs() throws Exception {
    String query = "((c0 + c1) * c2) + (c0 - c1)";
    int batchSize = 4 * 1000; // 4k rows
    int numRows = 1000 * 1000; // 1m rows

    List<Fixtures.Table> inout = genRandomDataForFunction(numRows, batchSize);
    Fixtures.Table input = inout.get(0);
    Fixtures.Table output = inout.get(1);

    GandivaAnnotator annotator = new GandivaAnnotator("add", "subtract");
    Split[] splits = {
      new Split(true, "_xxx0", "add(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "multiply(_xxx0, c2)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "add(_xxx1, subtract(c0, c1))", 3, 0, "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
    super.cleanExpToExpSplitCache();

    // support all functions and test
    annotator = new GandivaAnnotator("add", "subtract", "multiply");
    splits = new Split[]{
      new Split(true, "_xxx0", "add(multiply(add(c0, c1), c2), subtract(c0, c1))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testBooleanAnd() throws Exception {
    String query = "c0 > 10 AND c0 < 20";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18),
      tr(9),
      tr(24)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 20i)) else (false) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
    super.cleanExpToExpSplitCache();

    annotator = new GandivaAnnotator("less_than");
    splits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 20i)) else (false) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
    super.cleanExpToExpSplitCache();

    annotator = new GandivaAnnotator("greater_than", "less_than", "booleanAnd");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanAnd(greater_than(c0, 10i), less_than(c0, 20i))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndBCGandiva() throws Exception {
    // A && B && C with B and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0), // A is false
      tr(28, 0), // B is false
      tr(20, 10) // C is false
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("less_than", "booleanAnd", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then ((if (less_than(c0, 24i)) then(equal(c1, 0i)) else (false) end)) else (false) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndACGandiva() throws Exception {
    // A && B && C with A and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0),
      tr(28, 0),
      tr(20, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "booleanAnd", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (false) end)) else (false) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndABGandiva() throws Exception {
    // A && B && C with A and B supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0),
      tr(28, 0),
      tr(20, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "booleanAnd", "less_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (false) end)) else (false) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4AndBDGandiva() throws Exception {
    // A && B && C && D with B and D supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0 AND c2 < 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 0, 1),
      tr(9, 0, 3), // false because of A
      tr(30, 0, 2), // false because of B
      tr(18, 1, 2), // false because of C
      tr(20, 10, 3) // false because of D
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "less_than", "mod");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 3),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 2, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (cast((__$internal_null$__) as bit)) end)) else (cast((__$internal_null$__) as bit)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "_xxx3", "(if (_xxx0) then ((if (_xxx1) then ((if (_xxx2) then (less_than(c2, 4i)) else (false) end)) else (false) end)) else (false) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4AndACGandiva() throws Exception {
    // A && B && C && D with A and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0 AND c2 < 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 0, 1),
      tr(9, 0, 3), // false because of A
      tr(30, 0, 2), // false because of B
      tr(18, 1, 2), // false because of C
      tr(20, 10, 3) // false because of D
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "greater_than", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 3),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (cast((__$internal_null$__) as bit)) end)) else (cast((__$internal_null$__) as bit)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then ((if (_xxx1) then ((if (_xxx2) then (less_than(c2, 4i)) else (false) end)) else (false) end)) else (false) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testNestedIfConditions() throws Exception {
    String query = "case when c0 = 0 then c0 + c1 else (case when c1 = 0 then c1 + c0 else 1/c0 end) end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(0, 11)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(11)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("divide");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "equal(c0, 0i)", 1, 3),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else(equal(c1, 0i)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else ((if (_xxx1) then (cast((__$internal_null$__) as int)) else (divide(1i, c0)) end)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then (add(c0, c1)) else ((if (_xxx1) then (add(c1, c0)) else (_xxx2) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void testSplitElseInNestedIfCondition() throws Exception {
    String query = "case when c0 = 0 then c0 + c1 else (case when c1 = 0 then c1 + c0 else c1 + 1/c0 end) end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(0, 11)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(11)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("divide");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "equal(c0, 0i)", 1, 3),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else(equal(c1, 0i)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else ((if (_xxx1) then (cast((__$internal_null$__) as int)) else (divide(1i, c0)) end)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then (add(c0, c1)) else ((if (_xxx1) then (add(c1, c0)) else (add(c1, _xxx2)) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void testBooleanOr() throws Exception {
    String query = "c0 > 10 OR c0 < 0";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18),
      tr(9),
      tr(24)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(true)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (true) else (less_than(c0, 0i)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("less_than");
    super.cleanExpToExpSplitCache();
    splits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (true) else (less_than(c0, 0i)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("less_than", "greater_than", "booleanOr");
    super.cleanExpToExpSplitCache();
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanOr(greater_than(c0, 10i), less_than(c0, 0i))", 1, 0),
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrBCGandiva() throws Exception {
    // A || B || C with B and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "less_than", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 24i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (true) else ((if (less_than(c0, 10i)) then (true) else(equal(c1, 0i)) end)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrACGandiva() throws Exception {
    // A || B || C with A and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else (equal(c1, 0i)) end)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrABGandiva() throws Exception {
    // A || B || C with A and B supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than", "less_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else (equal(c1, 0i)) end)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4OrBCGandiva() throws Exception {
    // A || B || C || D with B and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0 OR c2 > 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(30, 1, 3), // true due to A
      tr(9, 2, -2), // true due to B
      tr(20, 0, 0), // true due to C
      tr(20, 3, 6), // true due to D
      tr(24, 10, 4)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "less_than", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 24i)", 1, 3),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else ((if (_xxx1) then (cast((__$internal_null$__) as bit)) else (equal(c1, 0i)) end)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else ((if (_xxx2) then (true) else (greater_than(c2, 4i)) end)) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4OrADGandiva() throws Exception {
    // A || B || C || D with A and D supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0 OR c2 > 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(30, 1, 3), // true due to A
      tr(9, 2, -2), // true due to B
      tr(20, 0, 0), // true due to C
      tr(20, 3, 6), // true due to D
      tr(24, 10, 4)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 3),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 2, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else ((if (_xxx1) then (cast((__$internal_null$__) as bit)) else (equal(c1, 0i)) end)) end)", 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "_xxx3", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else ((if (_xxx2) then (true) else (greater_than(c2, 4i)) end)) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testBooleanOperator() throws Exception {
    String query = "(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18L),
      tr(9L),
      tr(24L)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(true)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    // or is not supported, but both the args are completely supported
    // or gets translated to if-expr
    Split[] splits = {
      new Split(true, "_xxx0", "(if (booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i)))) then (true) else(equal(mod(c0, 2i),0i)) end)", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
    super.cleanExpToExpSplitCache();
    annotator = new GandivaAnnotator("booleanOr", "booleanAnd", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i))), equal(mod(c0, 2i),0i))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testExceptionHandling() {
    String query = "(c0 / c1) + c2";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 2, 5),
      tr(20, 5, 1),
      tr(20, 0, 4) // this causes a divide by zero exception during evaluation
    );

    Fixtures.Table output = t(
      th("out"),
      tr(14),
      tr(5),
      tr(4)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("divide");
    Split[] splits = {
      new Split(true, "_xxx0", "divide(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "add(_xxx0, c2)", 2, 0, "_xxx0")
    };

    boolean gotException = false;
    try {
      splitAndVerify(query, input, output, splits, annotator);
    } catch (Exception e) {
      // suppress exception
      gotException = true;
    }

    // The post-test checks ensure that there is no memory leak in the ArrowBufs allocated
    assertTrue(gotException);
  }

  @Test
  public void testCastDateIsNotUsingCastVarchar() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator();

    String query = "extractYear(cast(c0 as date))";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      1,
      tr(ts("2014-02-01T17:20:34")),
      tr(ts("2019-02-01T17:20:36")),
      tr(ts("2015-02-01T17:20:50"))
    );
    Fixtures.Table output = t(
      th("out"),
      tr(2014L),
      tr(2019L),
      tr(2015L)
    );
    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "extractyear(castdate(c0))", 1, 0)
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void testTruncAndNullCast() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator();

    String query = "trunc(extractYear(cast((__$INTERNAL_NULL$__) as TIMESTAMP)))";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      1,
      tr(ts("2014-02-01T17:20:34")),
      tr(ts("2019-02-01T17:20:36")),
      tr(ts("2015-02-01T17:20:50"))
    );
    Fixtures.Table output = t(
      th("out"),
      tr(NULL_BIGINT),
      tr(NULL_BIGINT),
      tr(NULL_BIGINT)
    );
    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "trunc(extractyear(cast((__$INTERNAL_NULL$__) as TIMESTAMP)))", 1, 0)
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }
}
