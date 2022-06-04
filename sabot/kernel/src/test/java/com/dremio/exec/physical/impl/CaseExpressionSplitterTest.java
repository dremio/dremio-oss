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

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import org.junit.Test;

import com.dremio.sabot.Fixtures;

/**
 * Unit test cases for the case expression splitter
 */
public class CaseExpressionSplitterTest extends BaseExpressionSplitterTest {
  private static final String simpleCaseQuery = "case when (c0 / 2) >= (c1 * 2) then c0 - c1 else c1 + c0 end";
  private static final String simpleCaseExpr =
    "(case when (greater_than_or_equal_to(divide(c0,2i), multiply(c1,2i))) then (subtract(c0, c1)) " +
      "else (add(c1, c0)) end)";
  private static final String simpleCaseWhenSplit =
    "(case when (greater_than_or_equal_to(divide(c0,2i), multiply(c1, 2i))) then (0i) " +
      "else (1i) end)";
  private static final String simpleCaseThenSplit =
    "(case when (equal(_xxx0, 0i)) then (subtract(c0, c1)) " +
      "else (add(c1, c0)) end)";
  private static final String multiCaseQuery =
    "case when c0 >= c1 then c0 - c1 " +
      "when c0 >= c1 - 10 then c0 - c1 + 10 " +
      "when c0 <= c1 - 100 then c1 - c0 " +
      "when c0 <= c1 - 90 then c1 - c0 - 10 else c1 + c0 end";
  private static final String multiCaseExpr =
    "(case when (greater_than_or_equal_to(c0, c1)) then (subtract(c0, c1)) " +
      "when (greater_than_or_equal_to(c0, subtract(c1, 10i))) then (add(subtract(c0, c1), 10i))  " +
      "when (less_than_or_equal_to(c0, subtract(c1, 100i))) then (subtract(c1, c0))  " +
      "when (less_than_or_equal_to(c0, subtract(c1, 90i))) then (subtract(subtract(c1, c0), 10i))  " +
      "else (add(c1, c0)) end)";
  private static final String altCaseQuery =
    "case when c0 >= c1 then c0 - c1 " +
      "when c2 <= c1 + 1 then c1 + c2 * 10 " +
      "when c3 >= c1 then c3 - c1 " +
      "when c4 <= c1 then (c1 + c4) / 2 else c4 - c0 end";
  private final String mixedCaseQuery =
    "case when c0 >= c1 then 'null' " +
      "when c0 <= (c1 - c2) then 'Done' " +
      "else 'Not Done' end";

  final Fixtures.Table mixedCaseInput = Fixtures.split(
    th("c0", "c1", "c2"),
    3,
    tr(12, 11, 23),
    tr(11, 12, 6),
    tr(4, 22, 14)
  );

  final Fixtures.Table mixedCaseOutput = t(
    th("out"),
    tr("null"),
    tr("Not Done"),
    tr("Done")
  );


  private static final Fixtures.Table caseInputSimple = Fixtures.split(
    th("c0", "c1"),
    2,
    tr(20, 4),
    tr(10, 11),
    tr(40, 10)
  );

  Fixtures.Table caseOutputSimple = t(
    th("out"),
    tr(16),
    tr(21),
    tr(30)
  );

  private static final Fixtures.Table caseInputMulti = Fixtures.split(
    th("c0", "c1"),
    2,
    tr(11, 10),
    tr(11, 15),
    tr(10, 115),
    tr(10, 101),
    tr(10, 25)
  );

  private static final Fixtures.Table caseOutputMulti = t(
    th("out"),
    tr(1),
    tr(6),
    tr(105),
    tr(81),
    tr(35)
  );

  private static final Fixtures.Table altInputMulti = Fixtures.split(
    th("c0", "c1", "c2", "c3", "c4"),
    5,
    tr(11, 11, 23, 22, 1),     // c0 >= c1, c0 - c1 = 0
    tr(11, 12, 6, 21, 22),     // c0 < c1 and c2 <= c1 + 1; c1 + c2 * 10 = 72
    tr(8, 12, 14, 15, 22),     // c0 < c1, c2 > c1 + 1, c3 >= c1; c3 - c1 = 3
    tr(10, 100, 111, 8, 60),   // c0 < c1, c2 > c1 + 1, c3 < c1, c4 <= c1; (c1 + c4)/2 = 80
    tr(10, 101, 111, 8, 291)   // else, c4 - c0 = 281
  );

  private static final Fixtures.Table altOutputMulti = t(
    th("out"),
    tr(0),
    tr(72),
    tr(3),
    tr(80),
    tr(281)
  );

  @Test
  public void testZeroSplitsSimpleJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase();
    Split[] expSplits = new Split[]{
      new Split(false, "out", simpleCaseExpr, 1, 0)
    };
    splitAndVerifyCase(simpleCaseQuery, caseInputSimple, caseOutputSimple, expSplits, annotator);
  }

  @Test
  public void testZeroSplitsMultiJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase();
    Split[] expSplits = new Split[]{
      new Split(false, "out", multiCaseExpr, 1, 0)
    };
    splitAndVerifyCase(multiCaseQuery, caseInputMulti, caseOutputMulti, expSplits, annotator);
  }

  @Test
  public void testZeroSplitsSimpleGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to",
      "subtract", "add", "divide", "multiply");
    Split[] expSplits = new Split[]{
      new Split(true, "out", simpleCaseExpr, 1, 0)
    };
    splitAndVerifyCase(simpleCaseQuery, caseInputSimple, caseOutputSimple, expSplits, annotator);
  }

  @Test
  public void testZeroSplitsMultiGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "less_than_or_equal_to",
      "subtract", "add");
    Split[] expSplits = new Split[]{
      new Split(true, "out", multiCaseExpr, 1, 0)
    };
    splitAndVerifyCase(multiCaseQuery, caseInputMulti, caseOutputMulti, expSplits, annotator);
  }

  @Test
  public void testVerticalSplitThenInGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("subtract", "add");
    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", simpleCaseWhenSplit, 1, 1),
      new Split(true, "out", simpleCaseThenSplit, 2, 0, "_xxx0")
    };
    splitAndVerifyCase(simpleCaseQuery, caseInputSimple, caseOutputSimple, expSplits, annotator);
  }

  @Test
  public void testVerticalSplitThenInJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "divide", "multiply");
    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", simpleCaseWhenSplit, 1, 1),
      new Split(false, "out", simpleCaseThenSplit, 2, 0, "_xxx0")
    };
    splitAndVerifyCase(simpleCaseQuery, caseInputSimple, caseOutputSimple, expSplits, annotator);
  }

  @Test
  public void testMixedSplitWhenInitial() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("divide", "multiply", "add", "subtract");
    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "divide(c0, 2i)", 1, 1),
      new Split(true, "_xxx1", "multiply(c1, 2i)", 1, 1),
      new Split(false, "_xxx2",
        "(case when (greater_than_or_equal_to(_xxx0, _xxx1)) then (0i) else (1i) end)", 2, 1, "_xxx0", "_xxx1"),
      new Split(true, "out",
        "(case when (equal(_xxx2, 0i)) then (subtract(c0, c1)) else (add(c1, c0)) end)", 3, 0, "_xxx2")
    };
    splitAndVerifyCase(simpleCaseQuery, caseInputSimple, caseOutputSimple, expSplits, annotator);
  }

  @Test
  public void testMixedSplitWhenMiddle() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) " +
      "when (greater_than_or_equal_to(c0, subtract(c1, 10i))) then (1i) " +
      "else (-1i) end)";
    final String xxx1 = "(if (less_than(_xxx0, 0i)) then (subtract(c1, 100i)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx2 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (less_than_or_equal_to(c0, _xxx1)) then (2i) else (-1i) end)";
    final String xxx3 = "(if (less_than(_xxx2, 0i)) then (subtract(c1, 90i)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx4 = "(case when (greater_than(_xxx2, -1i)) then (_xxx2) " +
      "when (less_than_or_equal_to(c0, _xxx3)) then (3i) else (4i) end)";
    final String xxx5 = "(case when (equal(_xxx4, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx4, 1i)) then (add(subtract(c0, c1), 10i)) " +
      "when (equal(_xxx4, 2i)) then (subtract(c1, c0)) " +
      "when (equal(_xxx4, 3i)) then (subtract(subtract(c1, c0), 10i)) " +
      "else (add(c1, c0)) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "add", "subtract");
    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", xxx0, 1, 2),
      new Split(true, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(false, "_xxx2", xxx2, 3, 2, "_xxx0", "_xxx1"),
      new Split(true, "_xxx3", xxx3, 4, 1, "_xxx2"),
      new Split(false, "_xxx4", xxx4, 5, 1, "_xxx2", "_xxx3"),
      new Split(true, "out", xxx5, 6, 0, "_xxx4")
    };
    splitAndVerifyCase(multiCaseQuery, caseInputMulti, caseOutputMulti, expSplits, annotator);
  }

  @Test
  public void testAlternateWhenSplits() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) " +
      "else (-1i) end)";
    final String xxx1 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (less_than_or_equal_to(c2, add(c1,1i))) then (1i) " +
      "else (-1i) end)";
    final String xxx2 = "(case when (greater_than(_xxx1, -1i)) then (_xxx1) " +
      "when (greater_than_or_equal_to(c3, c1)) then (2i) " +
      "else (-1i) end)";
    final String xxx3 = "(case when (greater_than(_xxx2, -1i)) then (_xxx2) " +
      "when (less_than_or_equal_to(c4, c1)) then (3i) " +
      "else (4i) end)";
    final String xxx4 = "(case when (equal(_xxx3, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx3, 1i)) then (add(c1, multiply(c2, 10i))) " +
      "when (equal(_xxx3, 2i)) then (subtract(c3, c1)) " +
      "when (equal(_xxx3, 3i)) then (divide(add(c1, c4), 2i)) " +
      "else (subtract(c4, c0)) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to", "add", "subtract",
      "multiply", "divide");
    Split[] expSplits = new Split[] {
      new Split(false, "_xxx0", xxx0, 1, 1),
      new Split(true, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(false, "_xxx2", xxx2, 3, 1, "_xxx1"),
      new Split(true, "_xxx3", xxx3, 4, 1, "_xxx2"),
      new Split(true, "out", xxx4, 5, 0, "_xxx3"),
    };
    splitAndVerifyCase(altCaseQuery, altInputMulti, altOutputMulti, expSplits, annotator);
  }

  @Test
  public void testAlternateMixedSplitsGandivaLast() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (-1i) end)";
    final String xxx1 = "(if (less_than(_xxx0, 0i)) then (subtract(c1, c2)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx2 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (less_than_or_equal_to(c0, _xxx1)) then (1i) else (2i) end)";
    final String xxx3 = "(case when (equal(_xxx2, 0i)) then ('null') when (equal(_xxx2, 1i)) then ('Done') " +
      "else ('Not Done') end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to");
    Split[] expSplits = new Split[] {
      new Split(false, "_xxx0", xxx0, 1, 2),
      new Split(false, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(true, "_xxx2", xxx2, 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "out", xxx3, 4, 0, "_xxx2"),
    };
    splitAndVerifyCase(mixedCaseQuery, mixedCaseInput, mixedCaseOutput, expSplits, annotator);
  }

  @Test
  public void testAlternateMixedSplitsJavaLast() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (-1i) end)";
    final String xxx1 = "(if (less_than(_xxx0, 0i)) then (subtract(c1, c2)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx2 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (less_than_or_equal_to(c0, _xxx1)) then (1i) else (2i) end)";
    final String xxx3 = "(case when (equal(_xxx2, 0i)) then ('null') when (equal(_xxx2, 1i)) then ('Done') " +
      "else ('Not Done') end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("subtract");
    Split[] expSplits = new Split[] {
      new Split(false, "_xxx0", xxx0, 1, 2),
      new Split(true, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(false, "_xxx2", xxx2, 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "out", xxx3, 4, 0, "_xxx2"),
    };
    splitAndVerifyCase(mixedCaseQuery, mixedCaseInput, mixedCaseOutput, expSplits, annotator);
  }

  @Test
  public void testAlternateThenSplits() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) " +
      "when (less_than_or_equal_to(c2, add(c1,1i))) then (1i)  " +
      "when (greater_than_or_equal_to(c3, c1)) then (2i)  " +
      "when (less_than_or_equal_to(c4, c1)) then (3i)  " +
      "else (4i) end)";
    final String xxx1 = "(case when (equal(_xxx0, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx0, 2i)) then (subtract(c3, c1)) " +
      "when (equal(_xxx0, 4i)) then (subtract(c4, c0)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end) ";
    final String xxx2 = "(case when (equal(_xxx0, 1i)) then (add(c1, multiply(c2, 10i))) " +
      "when (equal(_xxx0, 3i)) then (divide(add(c1, c4), 2i)) " +
      "else (_xxx1) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to", "greater_than_or_equal_to",
      "add", "multiply", "divide");
    Split[] expSplits = new Split[] {
      new Split(true, "_xxx0", xxx0, 1, 2),
      new Split(false, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(true, "out", xxx2, 3, 0, "_xxx1", "_xxx0"),
    };
    splitAndVerifyCase(altCaseQuery, altInputMulti, altOutputMulti, expSplits, annotator);
  }

  @Test
  public void testMixedThenSplits() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) " +
      "when (less_than_or_equal_to(c2, add(c1,1i))) then (1i)  " +
      "when (greater_than_or_equal_to(c3, c1)) then (2i)  " +
      "when (less_than_or_equal_to(c4, c1)) then (3i)  " +
      "else (4i) end)";
    final String xxx1 = "(if (equal(_xxx0, 1i)) then (multiply(c2, 10i)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx2 = "(if (equal(_xxx0, 3i)) then (add(c1, c4)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx3 = "(case when (equal(_xxx0, 3i)) then (divide(_xxx2, 2i)) " +
      "when (equal(_xxx0, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx0, 2i)) then (subtract(c3, c1)) " +
      "when (equal(_xxx0, 4i)) then (subtract(c4, c0)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end) ";
    final String xxx4 = "(case when (equal(_xxx0, 1i)) then (add(c1, _xxx1)) " +
      "else (_xxx3) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to", "greater_than_or_equal_to", "add");
    Split[] expSplits = new Split[] {
      new Split(true, "_xxx0", xxx0, 1, 4),
      new Split(false, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(true, "_xxx2", xxx2, 2, 1, "_xxx0"),
      new Split(false, "_xxx3", xxx3, 3, 1, "_xxx0", "_xxx2"),
      new Split(true, "out", xxx4, 4, 0, "_xxx1", "_xxx0", "_xxx3")
    };
    splitAndVerifyCase(altCaseQuery, altInputMulti, altOutputMulti, expSplits, annotator);
  }

  @Test
  public void testNormalWhenAndThenSplits() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (-1i ) end)";
    final String xxx1 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (less_than_or_equal_to(c2, add(c1, 1i))) then (1i) else (-1i ) end)";
    final String xxx2 = "(case when (greater_than(_xxx1, -1i)) then (_xxx1) " +
      "when (greater_than_or_equal_to(c3, c1)) then (2i) else (-1i) end)";
    final String xxx3 = "(case when (greater_than(_xxx2, -1i)) then (_xxx2) " +
      "when (less_than_or_equal_to(c4, c1)) then (3i)  else (4i) end)";
    final String xxx4 = "(case when (equal(_xxx3, 1i)) then (add(c1, multiply(c2, 10i))) " +
      "when (equal(_xxx3, 3i)) then (divide(add(c1, c4), 2i)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx5 = "(case when (equal(_xxx3, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx3, 2i)) then (subtract(c3, c1)) " +
      "when (equal(_xxx3, 4i)) then (subtract(c4, c0)) else (_xxx4) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "subtract");
    Split[] expSplits = new Split[] {
      new Split(true, "_xxx0", xxx0, 1, 1),
      new Split(false, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(true, "_xxx2", xxx2, 3, 1, "_xxx1"),
      new Split(false, "_xxx3", xxx3, 4, 2, "_xxx2"),
      new Split(false, "_xxx4", xxx4, 5, 1, "_xxx3"),
      new Split(true, "out", xxx5, 6, 0, "_xxx4", "_xxx3")
    };
    splitAndVerifyCase(altCaseQuery, altInputMulti, altOutputMulti, expSplits, annotator);
  }

  @Test
  public void testMixedWhenAndThenSplits() throws Exception {
    final String xxx0 = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (-1i) end)";
    final String xxx1 = "(if (less_than(_xxx0, 0i)) then (add(c1, 1i)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx2 = "(case when (greater_than(_xxx0, -1i)) then (_xxx0)  " +
      "when (less_than_or_equal_to(c2, _xxx1)) then (1i) else (-1i) end)";
    final String xxx3 = "(case when (greater_than(_xxx2, -1i)) then (_xxx2) " +
      "when (greater_than_or_equal_to(c3, c1)) then (2i) else (-1i) end)";
    final String xxx4 = "(case when (greater_than(_xxx3, -1i)) then (_xxx3) " +
      "when (less_than_or_equal_to(c4, c1)) then (3i) else (4i) end)";
    final String xxx5 = "(if (equal(_xxx4, 3i)) then (add(c1, c4)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx6 = "(case when (equal(_xxx4, 1i)) then (add(c1, multiply(c2, 10i))) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String xxx7 = "(case when (equal(_xxx4, 3i)) then (divide(_xxx5, 2i)) " +
      "when (equal(_xxx4, 0i)) then (subtract(c0, c1)) " +
      "when (equal(_xxx4, 2i)) then (subtract(c3, c1)) " +
      "when (equal(_xxx4, 4i)) then (subtract(c4, c0)) " +
      "else (_xxx6) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to", "subtract", "divide");
    Split[] expSplits = new Split[] {
      new Split(false, "_xxx0", xxx0, 1, 2),
      new Split(false, "_xxx1", xxx1, 2, 1, "_xxx0"),
      new Split(true, "_xxx2", xxx2, 3, 1, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", xxx3, 4, 1, "_xxx2"),
      new Split(true, "_xxx4", xxx4, 5, 3, "_xxx3"),
      new Split(false, "_xxx5", xxx5, 6, 1, "_xxx4"),
      new Split(false, "_xxx6", xxx6, 6, 1, "_xxx4"),
      new Split(true, "out", xxx7, 7, 0, "_xxx4", "_xxx5", "_xxx6")
    };
    splitAndVerifyCase(altCaseQuery, altInputMulti, altOutputMulti, expSplits, annotator);
  }
}
