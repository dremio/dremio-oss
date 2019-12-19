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
package com.dremio.exec.hive;

import static java.util.regex.Pattern.quote;

import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;

/**
 * Test pushing filter into ORC vectorized reader.
 */
public class ITORCFilterPushDown extends HiveTestBase {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);

  @Test
  public void equal() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey = 3";
    testPlanMatchingPatterns(query, new String[] {quote("[leaf-0 = (EQUALS r_regionkey 3), expr = leaf-0]") });

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name", "r_comment")
        .baselineValues(3L, "EUROPE", "ly final courts cajo")
        .go();
  }

  @Test
  public void greaterThan() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey > 3";
    testPlanMatchingPatterns(query,
        new String[] {quote("[leaf-0 = (LESS_THAN_EQUALS r_regionkey 3), expr = (not leaf-0)]")});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name", "r_comment")
        .baselineValues(4L, "MIDDLE EAST", "uickly special accou")
        .go();
  }

  @Test
  public void lessThanEqual() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey <=2";
    testPlanMatchingPatterns(query, new String[] {quote("[leaf-0 = (LESS_THAN_EQUALS r_regionkey 2), expr = leaf-0]")});

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name", "r_comment")
        .baselineValues(0L, "AFRICA", "lar deposits. blithe")
        .baselineValues(1L, "AMERICA", "hs use ironic, even ")
        .baselineValues(2L, "ASIA", "ges. thinly even pin")
        .go();
  }

  @Test
  public void and() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey <=2 AND r_name = 'EUROPE'";
    testPlanMatchingPatterns(query, new String[] {
        quote("[leaf-0 = (LESS_THAN_EQUALS r_regionkey 2), leaf-1 = (EQUALS r_name EUROPE), expr = (and leaf-0 leaf-1)]")
    });

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .expectsEmptyResultSet()
        .go();
  }

  @Test
  public void or() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey <=2 OR r_name = 'EUROPE'";
    testPlanMatchingPatterns(query, new String[] {
        quote("[leaf-0 = (LESS_THAN_EQUALS r_regionkey 2), leaf-1 = (EQUALS r_name EUROPE), expr = (or leaf-0 leaf-1)]")
    });

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name", "r_comment")
        .baselineValues(0L, "AFRICA", "lar deposits. blithe")
        .baselineValues(1L, "AMERICA", "hs use ironic, even ")
        .baselineValues(2L, "ASIA", "ges. thinly even pin")
        .baselineValues(3L, "EUROPE", "ly final courts cajo")
        .go();
  }

  @Test
  public void in() throws Exception {
    final String query = "SELECT * from hive.orc_region where r_regionkey in (2, 4)";
    testPlanMatchingPatterns(query, new String[] {
        quote("[leaf-0 = (EQUALS r_regionkey 2), leaf-1 = (EQUALS r_regionkey 4), expr = (or leaf-0 leaf-1)]")
    });

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("r_regionkey", "r_name", "r_comment")
        .baselineValues(2L, "ASIA", "ges. thinly even pin")
        .baselineValues(4L, "MIDDLE EAST", "uickly special accou")
        .go();
  }

  @Test
  public void testFixedWidthCharSingleSelect() throws Exception {
    String query = "SELECT * from hive.orc_strings where country_char25='INDIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .go();
    query = "SELECT * from hive.orc_strings where country_char25='INDONESIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
  }

  @Test
  public void testFixedWidthCharMultiSelect() throws Exception {
    final String query = "SELECT * from hive.orc_strings where continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
  }

  @Test
  public void testFixedWidthCharMultiPredicate() throws Exception {
    String query = "SELECT * from hive.orc_strings where country_char25='INDIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .go();
    query = "SELECT * from hive.orc_strings where country_char25='INDONESIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
  }

  @Test
  public void testFixedWidthCharMixedPredicateTypes() throws Exception {
    final String query = "SELECT * from hive.orc_strings where country_char25='INDIA' " +
      "and country_string='CHINA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .go();
  }

  @Test
  public void testFixedWidthCharDifferentPredicateOperators() throws Exception {
    String query = "SELECT * from hive.orc_strings where continent_char25 != 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
    query = "SELECT * from hive.orc_strings where continent_char25 < 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
    query = "SELECT * from hive.orc_strings where continent_char25 > 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(3, "FRANCE", "ITALY", "ROMANIA", "EUROPE")
      .go();
    query = "SELECT * from hive.orc_strings where continent_char25 >= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .baselineValues(3, "FRANCE", "ITALY", "ROMANIA", "EUROPE")
      .go();
    query = "SELECT * from hive.orc_strings where continent_char25 <= 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .baselineValues(3, "FRANCE", "ITALY", "ROMANIA", "EUROPE")
      .go();
    query = "SELECT * from hive.orc_strings where continent_char25 <= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .baselineValues(2, "INDONESIA", "THAILAND", "SINGAPORE", "ASIA")
      .go();
  }

  @Test
  public void testString() throws Exception {
    final String query = "SELECT * from hive.orc_strings where country_string='CHINA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .go();
  }

  @Test
  public void testVarchar() throws Exception {
    final String query = "SELECT * from hive.orc_strings where country_varchar='NEPAL'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key", "country_char25", "country_string", "country_varchar", "continent_char25")
      .baselineValues(1, "INDIA", "CHINA", "NEPAL", "ASIA")
      .go();
  }

  @Test
  public void testDoubleLiterals() throws Exception {
    final String query = "SELECT col1 from hive.orcdecimalcompare where col1 < 0.1";
    testPlanMatchingPatterns(query, new String[] {quote("[leaf-0 = (LESS_THAN col1 0.1), expr = leaf-0]") });

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(new Float("-0.1"))
      .go();
  }

  @Test
  public void testProjectionFixedWidthCharSingleSelect() throws Exception {
    String query = "SELECT country_char25 from hive.orc_strings where country_char25='INDIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("country_char25")
      .baselineValues("INDIA")
      .go();
    query = "SELECT country_char25 from hive.orc_strings_complex where country_char25='INDIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("country_char25")
      .baselineValues("INDIA")
      .go();
    query = "SELECT \"orc_strings_complex\".\"struct_col\".\"f1\" as f1 from hive.orc_strings_complex where country_char25='INDIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("f1")
      .baselineValues(1)
      .go();
    query = "SELECT country_char25 from hive.orc_strings where country_char25='INDONESIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("country_char25")
      .baselineValues("INDONESIA")
      .go();
    query = "SELECT country_char25 from hive.orc_strings_complex where country_char25='INDONESIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("country_char25")
      .baselineValues("INDONESIA")
      .go();
  }

  @Test
  public void testProjectionFixedWidthCharMultiSelect() throws Exception {
    String query = "SELECT continent_char25 from hive.orc_strings where continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
  }

  @Test
  public void testProjectionFixedWidthCharMultiPredicate() throws Exception {
    String query = "SELECT key from hive.orc_strings where country_char25='INDIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(1)
      .go();
    query = "SELECT key from hive.orc_strings_complex where country_char25='INDIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(1)
      .go();
    query = "SELECT key from hive.orc_strings where country_char25='INDONESIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(2)
      .go();
    query = "SELECT key from hive.orc_strings_complex where country_char25='INDONESIA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(2)
      .go();

  }

  @Test
  public void testProjectionFixedWidthCharMixedPredicateTypes() throws Exception {
    String query = "SELECT key from hive.orc_strings where country_char25='INDIA' " +
      "and country_string='CHINA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(1)
      .go();

    query = "SELECT key from hive.orc_strings_complex where country_char25='INDIA' " +
      "and country_string='CHINA' and continent_char25='ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(1)
      .go();
  }

  @Test
  public void testProjectionFixedWidthCharDifferentPredicateOperators() throws Exception {
    String query = "SELECT continent_char25 from hive.orc_strings where continent_char25 != 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 != 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings where continent_char25 < 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 < 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings where continent_char25 > 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 > 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings where continent_char25 >= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 >= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings where continent_char25 <= 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 <= 'EUROPE'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings where continent_char25 <= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
    query = "SELECT continent_char25 from hive.orc_strings_complex where continent_char25 <= 'ASIA'";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("continent_char25")
      .baselineValues("ASIA")
      .baselineValues("ASIA")
      .go();
  }
}
