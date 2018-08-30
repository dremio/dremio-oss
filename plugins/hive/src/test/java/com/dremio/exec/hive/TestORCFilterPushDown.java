/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.junit.Test;

/**
 * Test pushing filter into ORC vectorized reader.
 */
public class TestORCFilterPushDown extends HiveTestBase {

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
}
