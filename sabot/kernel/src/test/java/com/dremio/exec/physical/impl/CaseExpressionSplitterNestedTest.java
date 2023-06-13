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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.dremio.sabot.Fixtures;

/**
 * Unit test cases for the case expression splitter
 */
public class CaseExpressionSplitterNestedTest extends BaseExpressionSplitterTest {
  private static final String nestedCaseQuerySimple =
    "case when c0 >= c1 then case when c0 <= c2 then c0 + c1 else c4 - c0 end " +
      "else case when c0 >= c3 then c0 - c3 else c0 + c3 end end";
  private static final String nestedCaseQueryWithConstants =
    "case when c0 >= c1 then case when c0 >= c2 then c0 * c1 " +
      "                           when c0 <= c3 then 12 " +
      "                           when c0 >= c4 then 11 " +
      "                           else case when c0 <= c4 - 10 then c0 else c4 / c0 end end " +
      "  when c0 >= c1 - 10 then case when c0 >= c2 then 12 " +
      "                           when c0 <= c3 then 13 " +
      "                           when c0 >= c4 then 14 " +
      "                           else 15 end " +
      "  when c0 >= c1 - 20 then case when c0 >= c2 then 120 " +
      "                           when c0 <= c3 then 130 " +
      "                           when c0 >= c4 then c0 * c4 " +
      "                           else 1000 / c1 end " +
      "  when c0 >= c1 - 30 then case when c0 >= c2 then c0 + c1 " +
      "                           when c0 <= c3 then 1300 " +
      "                           when c0 >= c4 then 1400 " +
      "                           else c0 - c1 end " +
      " else c1 + c4 end";

  private static final String nestedCaseQuery =
    "case when c0 >= c1 then case when c0 >= c2 then c0 * c1 " +
                                 "when c0 <= c3 then c0 + c3 " +
                                 "when c0 >= c4 then c0 * c4 " +
                                 "else case when c0 <= c4 - 10 then c0 else c4 / c0 end end " +
          "when c0 >= (case when c2 >= c1 then c3 - 100 else c4 + 10 end) then c1 + c0 " +
          "when c0 <= c3 then c0 * c3 else c1 + c4 end";
  private static final String innerThenCaseExpr =
    "(case when (greater_than_or_equal_to(c0, c2)) then (multiply(c0,c1)) " +
          "when (less_than_or_equal_to(c0, c3)) then (add(c0, c3)) " +
          "when (greater_than_or_equal_to(c0, c4)) then (multiply(c0,c4)) " +
          "else ((case when (less_than_or_equal_to(c0, subtract(c4,10i))) then (c0) else (divide(c4, c0)) end)) end)";
  private static final String innerWhenCaseExpr =
    "(case when (greater_than_or_equal_to(c2, c1)) then (subtract(c3,100i)) else (add(c4,10i)) end)";
  private static final String nestedCaseExpr =
    "(case when (greater_than_or_equal_to(c0, c1)) then (" + innerThenCaseExpr + ")" +
          "when (greater_than_or_equal_to(c0, " + innerWhenCaseExpr + ")) then (add(c1, c0)) " +
          "when (less_than_or_equal_to(c0, c3)) then (multiply(c0, c3))  " +
          "else (add(c1, c4)) end)";

  private static final Fixtures.Table nestedCaseInput = Fixtures.split(
    th("c0", "c1", "c2", "c3", "c4"),
    5,
    tr(20, 4, 8, 9, 10),        // 80, -10, 80
    tr(10, 3, 18, 16, 10),      // 26, 13, 12
    tr(20, 8, 22, 16, 20),      // 400, 28, 11
    tr(20, 8, 22, 16, 50),      // 20, 28, 20
    tr(20, 8, 22, 16, 25),      // 1, 28, 1
    tr(15, 25, 30, 111, 50),    // 40, 126, 13
    tr(15, 30, 25, 111, 5),     // 45, 126, 130
    tr(1, 25, 30, 111, 50),     // 111, 112, 1000
    tr(100, 300, 250, 99, 200)  // 500, 1, 500
  );

  private static final Fixtures.Table nestedCaseOutput = t(
    th("out"),
    tr(80),
    tr(26),
    tr(400),
    tr(20),
    tr(1),
    tr(40),
    tr(45),
    tr(111),
    tr(500)
  );

  private static final Fixtures.Table nestedCaseSimpleOutput = t(
    th("out"),
    tr(-10),
    tr(13),
    tr(28),
    tr(28),
    tr(28),
    tr(126),
    tr(126),
    tr(112),
    tr(1)
  );

  private static final Fixtures.Table nestedCaseWithConstantsOutput = t(
    th("out"),
    tr(80),
    tr(12),
    tr(11),
    tr(20),
    tr(1),
    tr(13),
    tr(130),
    tr(1300),
    tr(500)
  );

  @Test
  public void testNestedNoSplitsJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase();
    Split[] expSplits = new Split[] {
      new Split(false, "out", nestedCaseExpr, 1, 0)
    };
    splitAndVerifyCase(nestedCaseQuery, nestedCaseInput, nestedCaseOutput, expSplits, annotator);
  }

  @Test
  public void testNestedNoSplitsGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotatorCase("less_than_or_equal_to", "greater_than_or_equal_to",
      "subtract", "add", "multiply", "divide");
    Split[] expSplits = new Split[] {
      new Split(true, "out", nestedCaseExpr, 1, 0)
    };
    splitAndVerifyCase(nestedCaseQuery, nestedCaseInput, nestedCaseOutput, expSplits, annotator);
  }

  @SuppressWarnings("checkstyle:LocalFinalVariableName")
  @Test
  public void testNestedCaseQuerySimple() throws Exception {
    final String _xxx0g = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (1i) end)";
    final String _xxx1g = "(if (equal(_xxx0, 0i)) then " +
      "((case when (less_than_or_equal_to(c0, c2)) then (0i) else (1i) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx2j = "(if (equal(_xxx0, 0i)) then " +
      "((case when (equal(_xxx1, 1i)) then (subtract(c4, c0)) else (cast((__$INTERNAL_NULL$__) as INT)) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx3g = "(if (equal(_xxx0, 1i)) then " +
      "((case when (greater_than_or_equal_to(c0, c3)) then (0i) else (1i) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT))  end)";
    final String _xxx4j = "(if (equal(_xxx0, 1i)) then " +
      "((case when (equal(_xxx3, 0i)) then (subtract(c0, c3)) else (cast((__$INTERNAL_NULL$__) as INT)) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx5g =
      "(case when (equal(_xxx0, 0i)) then ((case when (equal(_xxx1, 0i)) then (add(c0, c1)) else (_xxx2) end)) " +
      "when (equal(_xxx0, 1i)) then ((case when (equal(_xxx3, 1i)) then (add(c0, c3)) else (_xxx4) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "less_than_or_equal_to", "add");
    Split[] expSplits = new Split[] {
      new Split(true, "_xxx0", _xxx0g, 1, 5),
      new Split(true, "_xxx1", _xxx1g, 2, 2, "_xxx0"),
      new Split(false, "_xxx2", _xxx2j, 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "_xxx3", _xxx3g, 2, 2, "_xxx0"),
      new Split(false, "_xxx4", _xxx4j, 3, 1, "_xxx0", "_xxx3"),
      new Split(true, "out", _xxx5g, 4, 0, "_xxx0", "_xxx1", "_xxx2", "_xxx3", "_xxx4")
    };
    splitAndVerifyCase(nestedCaseQuerySimple, nestedCaseInput, nestedCaseSimpleOutput, expSplits, annotator);
  }

  @SuppressWarnings("checkstyle:LocalFinalVariableName")
  @Test
  public void testNestedMixedSplits() throws Exception {
    final String _xxx0g = "(case when (greater_than_or_equal_to(c0, c1)) then (0i) else (-1i) end)";
    final String _xxx1g = "(if (less_than(_xxx0,0i)) then (greater_than_or_equal_to(c2,c1)) " +
      "else (cast((__$internal_null$__) as bit)) end)";
    final String _xxx2j = "(if (less_than(_xxx0, 0i)) then " +
      "((if (_xxx1) then (subtract(c3, 100i)) else (add(c4, 10i)) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx3j = "(case when (greater_than(_xxx0, -1i)) then (_xxx0) " +
      "when (greater_than_or_equal_to(c0, _xxx2)) then (1i) else (-1i) end)";
    final String _xxx4j = "(case when (greater_than(_xxx3, -1i)) then (_xxx3) " +
      "when (less_than_or_equal_to(c0, c3)) then (2i) else (3i) end)";
    final String _xxx5g = "(if (equal(_xxx4, 0i)) then ((case when (greater_than_or_equal_to(c0, c2)) then (0i) " +
      "else (-1i) end)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx6j = "(if (equal(_xxx4, 0i)) then ((case when (greater_than(_xxx5, -1i)) then (_xxx5) " +
      "when (less_than_or_equal_to(c0, c3)) then (1i) else (-1i) end)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx7g = "(if (equal(_xxx4, 0i)) then ((case when (greater_than(_xxx6, -1i)) then (_xxx6) " +
      "when (greater_than_or_equal_to(c0, c4)) then (2i) else (3i) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx8j = "(if (equal(_xxx4, 0i)) then ((if (equal(_xxx7, 3i)) then " +
      "((case when (less_than_or_equal_to(c0, subtract(c4, 10i))) then (0i) else (1i) end)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)) else (cast((__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx9j = "(if (equal(_xxx4, 0i)) then ((case when (equal(_xxx7, 1i)) then (add(c0, c3)) " +
      "else (cast((__$INTERNAL_NULL$__) as INT)) end)) else (cast((__$INTERNAL_NULL$__) as INT)) end) ";
    final String _xxx10j = "(case when (equal(_xxx4, 1i)) then (add(c1, c0)) " +
      "when (equal(_xxx4, 3i)) then (add(c1, c4)) else (cast( (__$INTERNAL_NULL$__) as INT)) end)";
    final String _xxx11g = "(case when (equal(_xxx4, 0i)) then ((case when (equal(_xxx7, 3i)) then " +
      "((case when (equal(_xxx8, 0i)) then (c0) else (divide(c4, c0)) end)) " +
      "when (equal(_xxx7, 0i)) then (multiply(c0, c1)) when (equal(_xxx7, 2i)) then (multiply(c0, c4)) " +
      "else (_xxx9) end)) when (equal(_xxx4, 2i)) then (multiply(c0, c3)) else (_xxx10) end)";

    GandivaAnnotator annotator = new GandivaAnnotatorCase("greater_than_or_equal_to", "multiply", "divide");
    Split[] expSplits = new Split[] {
      new Split(true, "_xxx0", _xxx0g, 1, 3),
      new Split(true, "_xxx1", _xxx1g, 2, 1, "_xxx0"),
      new Split(false, "_xxx2", _xxx2j, 3, 1, "_xxx0", "_xxx1"),
      new Split(true, "_xxx3", _xxx3j, 4, 1, "_xxx0", "_xxx2"),
      new Split(false, "_xxx4", _xxx4j, 5, 7, "_xxx3"),
      new Split(true, "_xxx5", _xxx5g, 6, 1, "_xxx4"),
      new Split(false, "_xxx6", _xxx6j, 7, 1, "_xxx4", "_xxx5"),
      new Split(true, "_xxx7", _xxx7g, 8, 3, "_xxx4", "_xxx6"),
      new Split(false, "_xxx8", _xxx8j, 9, 1, "_xxx4", "_xxx7"),
      new Split(false, "_xxx9", _xxx9j, 9, 1, "_xxx4", "_xxx7"),
      new Split(false, "_xxx10", _xxx10j, 6, 1, "_xxx4"),
      new Split(true, "out", _xxx11g, 10, 0, "_xxx4", "_xxx7", "_xxx8", "_xxx9", "_xxx10")
    };
    splitAndVerifyCase(nestedCaseQuery, nestedCaseInput, nestedCaseOutput, expSplits, annotator);
  }

  /**
   * The following tests will not assert splits, but simply assert that final output is as expected
   */
  @Test
  public void testCorrectnessWithRandomFunctionsInGandiva() throws Exception {
    final String[] fns = {"add", "subtract", "divide", "multiply", "less_than_or_equal_to", "greater_than_or_equal_to"};
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      final Set<String> chosenFns = new HashSet<>();
      int numFns = 1 + rand.nextInt(fns.length - 2);
      for (int j = 0; j < numFns; j++) {
        chosenFns.add(fns[rand.nextInt(fns.length)]);
      }
      GandivaAnnotator annotator = new GandivaAnnotatorCase(chosenFns.toArray(new String[0]));
      splitAndVerifyCase(nestedCaseQuery, nestedCaseInput, nestedCaseOutput, null, annotator);
    }
  }

  @Test
  public void testCorrectnessForConstantWithRandomFunctionsInGandiva() throws Exception {
    final String[] fns = {"add", "subtract", "divide", "multiply", "less_than_or_equal_to", "greater_than_or_equal_to"};
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      final Set<String> chosenFns = new HashSet<>();
      int numFns = 1 + rand.nextInt(fns.length - 2);
      for (int j = 0; j < numFns; j++) {
        chosenFns.add(fns[rand.nextInt(fns.length)]);
      }
      GandivaAnnotator annotator = new GandivaAnnotatorCase(chosenFns.toArray(new String[0]));
      splitAndVerifyCase(nestedCaseQueryWithConstants, nestedCaseInput, nestedCaseWithConstantsOutput, null, annotator);
    }
  }
}
