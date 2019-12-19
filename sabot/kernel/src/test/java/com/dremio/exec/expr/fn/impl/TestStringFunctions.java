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
package com.dremio.exec.expr.fn.impl;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;

public class TestStringFunctions extends BaseTestQuery {

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testStrPosMultiByte() throws Exception {
    testBuilder()
        .sqlQuery("select \"position\"('a', 'abc') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1)
        .go();

      testBuilder()
          .sqlQuery("select \"position\"('a', 'abcabc', 2) res1 from (values(1))")
          .ordered()
          .baselineColumns("res1")
          .baselineValues(4)
          .go();

    testBuilder()
        .sqlQuery("select \"position\"('\\u11E9', '\\u11E9\\u0031') res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues(1)
        .go();

    // edge case
    testBuilder()
        .sqlQuery("SELECT POSITION('foo' IN 'foobar' FROM 1) p1, POSITION('foo' IN a FROM 1) p2" +
            " FROM (VALUES('foobar')) tbl(a)")
        .ordered()
        // p1 is calcite const evaluation, p2 is dremio expression evaluation
        .baselineColumns("p1", "p2")
        .baselineValues(1, 1)
        .go();
  }

  @Test
  public void locate() throws Exception {
    testBuilder()
        .sqlQuery("SELECT LOCATE('foo', 'foobar', 1) l1, LOCATE('foo', a, 1) l2" +
            " FROM (VALUES('foobar')) tbl(a)")
        .ordered()
        // l1 is calcite const evaluation, l2 is dremio expression evaluation
        .baselineColumns("l1", "l2")
        .baselineValues(1, 1)
        .go();

    testBuilder()
        .sqlQuery("SELECT LOCATE('nope', 'foobar', 1) l1, LOCATE('nope', a, 1) l2" +
            " FROM (VALUES('foobar')) tbl(a)")
        .ordered()
        // l1 is calcite const evaluation, l2 is dremio expression evaluation
        .baselineColumns("l1", "l2")
        .baselineValues(0, 0)
        .go();
  }

  @Test
  public void invalidLocate() throws Exception {
    thrownException.expect(UserException.class);
    thrownException.expectMessage("must be greater than 0");

    test("SELECT LOCATE('nope', a, 0) FROM (VALUES('foobar')) tbl(a)");
  }

  @Test
  public void testSplitPart() throws Exception {
    testBuilder()
        .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 1) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("abc")
        .go();

    testBuilder()
        .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 2) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("def")
        .go();

    // invalid index
    boolean expectedErrorEncountered;
    try {
      testBuilder()
          .sqlQuery("select split_part('abc~@~def~@~ghi', '~@~', 0) res1 from (values(1))")
          .ordered()
          .baselineColumns("res1")
          .baselineValues("abc")
          .go();
      expectedErrorEncountered = false;
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Index in split_part must be positive, value provided was 0"));
      expectedErrorEncountered = true;
    }
    if (!expectedErrorEncountered) {
      throw new RuntimeException("Missing expected error on invalid index for split_part function");
    }

    // with a multi-byte splitter
    testBuilder()
        .sqlQuery("select split_part('abc\\u1111dremio\\u1111ghi', '\\u1111', 2) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("dremio")
        .go();

    // going beyond the last available index, returns empty string
    testBuilder()
        .sqlQuery("select split_part('a,b,c', ',', 4) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("")
        .go();

    // if the delimiter does not appear in the string, 1 returns the whole string
    testBuilder()
        .sqlQuery("select split_part('a,b,c', ' ', 1) res1 from (values(1))")
        .ordered()
        .baselineColumns("res1")
        .baselineValues("a,b,c")
        .go();
  }

  @Test
  public void testSplitPartMultiRow() throws Exception {
    File datasetSplit = tempDir.newFile("splitPart.csv");
    PrintWriter pw = new PrintWriter(datasetSplit);
    pw.println("abc~@~def~@~ghi");
    pw.println("abc~@~def");
    pw.println("abc");
    pw.close();

    testBuilder()
      .sqlQuery(String.format("select split_part(columns[0], '~@~', 1) as res1 FROM dfs.\"%s\"",
        datasetSplit.getAbsolutePath()))
      .ordered()
      .baselineColumns("res1")
      .baselineValues("abc")
      .baselineValues("abc")
      .baselineValues("abc")
      .go();

    testBuilder()
      .sqlQuery(String.format("select split_part(columns[0], '~@~', 2) as res1 FROM dfs.\"%s\"",
        datasetSplit.getAbsolutePath()))
      .ordered()
      .baselineColumns("res1")
      .baselineValues("def")
      .baselineValues("def")
      .baselineValues("")
      .go();

    // invalid index
    boolean expectedErrorEncountered;
    try {
      testBuilder()
        .sqlQuery(String.format("select split_part(columns[0], '~@~', 0) as res1 FROM dfs.\"%s\"",
          datasetSplit.getAbsolutePath()))
        .ordered()
        .baselineColumns("res1")
        .baselineValues("abc")
        .go();
      expectedErrorEncountered = false;
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("Index in split_part must be positive, value provided was 0"));
      expectedErrorEncountered = true;
    }
    if (!expectedErrorEncountered) {
      throw new RuntimeException("Missing expected error on invalid index for split_part function");
    }

    File datasetMultiByteSplit = tempDir.newFile("splitPartMultiByte.csv");
    PrintWriter pwMB = new PrintWriter(datasetMultiByteSplit);
    pwMB.println("abc\\u1111dremio\\u1111ghi', '\\u1111");
    pwMB.println("abc\\u1111dremio1\\u1111ghi', '\\u1111");
    pwMB.println("abc\\u1111dremio2\\u1111ghi', '\\u1111");
    pwMB.close();

    // with a multi-byte splitter
    testBuilder()
      .sqlQuery(String.format("select split_part(columns[0], '\\u1111', 2) as res1 FROM dfs.\"%s\"",
        datasetMultiByteSplit.getAbsolutePath()))
      .ordered()
      .baselineColumns("res1")
      .baselineValues("dremio")
      .baselineValues("dremio1")
      .baselineValues("dremio2")
      .go();

    // if the delimiter does not appear in the string, 1 returns the whole string
    testBuilder()
      .sqlQuery(String.format("select split_part(columns[0], ' ', 1) as res1 FROM dfs.\"%s\"",
        datasetSplit.getAbsolutePath()))
      .ordered()
      .baselineColumns("res1")
      .baselineValues("abc~@~def~@~ghi")
      .baselineValues("abc~@~def")
      .baselineValues("abc")
      .go();
  }

  @Test
  public void testRegexpMatches() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_like(a, '^a.*') res1, regexp_matches(b, '^a.*') res2, regexp_like(c, '.*b') res3 " +
                  "from (values('abc', 'bcd', 'cd \\n b'), ('bcd', 'abc', 'cd \\n c')) as t(a,b,c)")
        .unOrdered()
        .baselineColumns("res1", "res2", "res3")
        .baselineValues(true, false, true)
        .baselineValues(false, true, false)
        .build()
        .run();
  }

  @Test
  public void testRegexpReplaceStartsWith() throws Exception {
    validateRegexpReplace("^a", "bar",
        "barbc",
        "Ab",
        "bar",
        "defabc",
        "deab",
        "efa",
        null
    );
  }

  @Test
  public void testRegexpMatchesStartsWith() throws Exception {
    validateRegexpMatches("^a.*?",
        true,
        false,
        true,
        false,
        false,
        false,
        null
    );
  }

  @Test
  public void testRegexpReplaceStartsWithIgnoreCase() throws Exception {
    validateRegexpReplace("(?i)(?u)^a", "bar",
        "barbc",
        "barb",
        "bar",
        "defabc",
        "deab",
        "efa",
        null
    );
  }

  @Test
  public void testRegexpMatchesStartsWithIgnoreCase() throws Exception {
    validateRegexpMatches("(?i)(?u)^a.*?",
        true,
        true,
        true,
        false,
        false,
        false,
        null
    );
  }

  @Test
  public void testRegexpReplaceEndsWith() throws Exception {
    validateRegexpReplace("bc$", "bar",
        "abar",
        "Ab",
        "a",
        "defabar",
        "deab",
        "efa",
        null
    );
  }

  @Test
  public void testRegexpMatchesEndsWith() throws Exception {
    validateRegexpMatches(".*?bc$",
        true,
        false,
        false,
        true,
        false,
        false,
        null
    );
  }

  @Test
  public void testRegexpReplaceContains() throws Exception {
    validateRegexpReplace("ab", "bar",
        "barc",
        "Ab",
        "a",
        "defbarc",
        "debar",
        "efa",
        null
    );
  }

  @Test
  public void testRegexpMatchesContains() throws Exception {
    validateRegexpMatches(".*?ab.*?",
        true,
        false,
        false,
        true,
        true,
        false,
        null
    );
  }

  @Test
  public void testRegexpReplaceContainsIgnoreCase() throws Exception {
    validateRegexpReplace("(?i)(?u)ab", "bar",
        "barc",
        "bar",
        "a",
        "defbarc",
        "debar",
        "efa",
        null
    );
  }

  @Test
  public void testRegexpMatchesContainsIgnoreCase() throws Exception {
    validateRegexpMatches("(?i)(?u).*?ab.*?",
        true,
        true,
        false,
        true,
        true,
        false,
        null
    );
  }

  @Test
  public void testRegexpReplacePattern() throws Exception {
    validateRegexpReplace("[ec]", "bar",
        "abbar",
        "Ab",
        "a",
        "dbarfabbar",
        "dbarab",
        "barfa",
        null
    );
  }

  @Test
  public void testRegexpMatchesPattern() throws Exception {
    validateRegexpMatches(".*?[ec].*?",
        true,
        false,
        false,
        true,
        true,
        true,
        null
    );
  }

  @Test
  public void testRegexpReplacePatternIgnoreCase() throws Exception {
    validateRegexpReplace("(?i)(?u)[Ac]", "bar",
        "barbbar",
        "barb",
        "bar",
        "defbarbbar",
        "debarb",
        "efbar",
        null
    );
  }

  @Test
  public void testRegexpMatchesPatternIgnoreCase() throws Exception {
    validateRegexpMatches("(?i)(?u).*?[Ac].*?",
        true,
        true,
        true,
        true,
        true,
        true,
        null
    );
  }

  private void validateRegexpReplace(String pattern, String newValue, String... expected) throws Exception {
    /**
     * Contents of test file:
     *   { "a": "abc", "b": 0}
     *   { "a": "Ab", "b": 1}
     *   { "a": "a", "b": 2}
     *   { "a": "defabc", "b": 3}
     *   { "a": "deab", "b": 4}
     *   { "a": "efa", "b": 5}
     *   { "b": 6}
     */
    final String sql = format("select regexp_replace(a, '%s', '%s') as a from cp.\"functions/string/regexp_replace.json\"", pattern, newValue);

    TestBuilder builder = testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("a");

    for(String exp : expected) {
      if (exp == null) {
        builder = builder.baselineValues(new Object[] { null });
      } else {
        builder = builder.baselineValues(exp);
      }
    }

    builder.go();
  }

  private void validateRegexpMatches(String pattern, Boolean... expected) throws Exception {
    /**
     * Contents of test file:
     *   { "a": "abc", "b": 0}
     *   { "a": "Ab", "b": 1}
     *   { "a": "a", "b": 2}
     *   { "a": "defabc", "b": 3}
     *   { "a": "deab", "b": 4}
     *   { "a": "efa", "b": 5}
     *   { "b": 6}
     */
    final String sql = format("select regexp_matches(a, '%s') as a from cp.\"functions/string/regexp_replace.json\"", pattern);

    TestBuilder builder = testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("a");

    for(Boolean exp : expected) {
      if (exp == null) {
        builder = builder.baselineValues(new Object[] { null });
      } else {
        builder = builder.baselineValues(exp);
      }
    }

    builder.go();
  }

  @Test
  public void testRegexpMatchesNonAscii() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_matches(a, 'München') res1, regexp_matches(b, 'AMünchenA') res2 " +
            "from cp.\"functions/string/regexp_replace_ascii.json\"")
        .unOrdered()
        .baselineColumns("res1", "res2")
        .baselineValues(true, false)
        .baselineValues(false, true)
        .build()
        .run();
  }

  @Test
  public void testRegexpReplace() throws Exception {
    testBuilder()
        .sqlQuery("select regexp_replace(a, 'a|c', 'x') res1, regexp_replace(b, 'd', 'zzz') res2 " +
                  "from (values('abc', 'bcd'), ('bcd', 'abc')) as t(a,b)")
        .unOrdered()
        .baselineColumns("res1", "res2")
        .baselineValues("xbx", "bczzz")
        .baselineValues("bxd", "abc")
        .build()
        .run();
  }

  @Test
  public void testILike() throws Exception {
    testBuilder()
        .sqlQuery("select " +
            "ilike('UNITED_STATE', '%UNITED%') c1, " +
            "ilike('UNITED_KINGDOM', 'united%') c2, " +
            "ilike('KINGDOM \n NOT UNITED', '%united') c3 " +
            "from sys.version")
        .unOrdered()
        .baselineColumns("c1", "c2", "c3")
        .baselineValues(true, true, true)
        .build()
        .run();
  }

  @Test
  public void testILikeEscape() throws Exception {
    testBuilder()
        .sqlQuery("select a from (select concat(r_name , '_region') a from cp.\"tpch/region.parquet\") where ilike(a, 'asia#_region', '#') = true")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("ASIA_region")
        .build()
        .run();
  }

  @Test
  public void testSubstr() throws Exception {
    testBuilder()
        .sqlQuery("select substr(n_name, 'UN.TE.') a from cp.\"tpch/nation.parquet\" where ilike(n_name, 'united%') = true")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues("UNITED")
        .baselineValues("UNITED")
        .build()
        .run();
  }

  @Test
  public void testLpadTwoArgConvergeToLpad() throws Exception {
    final String query_1 = "SELECT lpad(r_name, 25) \n" +
        "FROM cp.\"tpch/region.parquet\"";


    final String query_2 = "SELECT lpad(r_name, 25, ' ') \n" +
        "FROM cp.\"tpch/region.parquet\"";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testRpadTwoArgConvergeToRpad() throws Exception {
    final String query_1 = "SELECT rpad(r_name, 25) \n" +
        "FROM cp.\"tpch/region.parquet\"";


    final String query_2 = "SELECT rpad(r_name, 25, ' ') \n" +
        "FROM cp.\"tpch/region.parquet\"";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testLtrimOneArgConvergeToLtrim() throws Exception {
    final String query_1 = "SELECT ltrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.\"tpch/region.parquet\"";


    final String query_2 = "SELECT ltrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.\"tpch/region.parquet\"";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testRtrimOneArgConvergeToRtrim() throws Exception {
    final String query_1 = "SELECT rtrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.\"tpch/region.parquet\"";


    final String query_2 = "SELECT rtrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.\"tpch/region.parquet\"";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testBtrimOneArgConvergeToBtrim() throws Exception {
    final String query_1 = "SELECT btrim(concat(' ', r_name, ' ')) \n" +
        "FROM cp.\"tpch/region.parquet\"";


    final String query_2 = "SELECT btrim(concat(' ', r_name, ' '), ' ') \n" +
        "FROM cp.\"tpch/region.parquet\"";

    testBuilder()
        .sqlQuery(query_1)
        .unOrdered()
        .sqlBaselineQuery(query_2)
        .build()
        .run();
  }

  @Test
  public void testInitCap() throws Exception {
    final String query_1 = "SELECT x, initcap(x) as y FROM (VALUES ('abc'), ('ABC'), ('12ABC')) as t1(x)";
    final String expected = "SELECT x, y FROM (VALUES ('abc', 'Abc'), ('ABC', 'Abc'), ('12ABC', '12abc')) as t1(x, y)";

    testBuilder()
      .sqlQuery(query_1)
      .unOrdered()
      .sqlBaselineQuery(expected)
      .build()
      .run();
  }

  @Test
  public void testReverse() throws Exception {
    testBuilder()
      .sqlQuery("SELECT full_name, reverse(substring(full_name, 2, 5)) AS reverse_sub_name "
        + "FROM cp.\"employee.json\" LIMIT 1")
      .unOrdered()
      .baselineColumns("full_name", "reverse_sub_name")
      .baselineValues("Sheri Nowmer", " ireh")
      .go();
  }
}
