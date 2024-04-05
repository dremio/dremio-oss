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
package com.dremio;

import com.dremio.test.TemporarySystemProperties;
import org.junit.Rule;
import org.junit.Test;

public class TestExcept extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestExcept.class);

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test // Except over values
  public void testExcept1() throws Exception {
    String query =
        ""
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS int), 1), (1, 1)) AS t1(c1, c2)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1)) AS t2(c1, c2)";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2")
        .baselineValues(null, 1)
        .go();
  }

  @Test // Except over values with matching null on both sides
  public void testExceptMatchingNulls() throws Exception {
    String query =
        ""
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS int), 1), (4, 1)) AS t1(c1, c2)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS int), 1)) AS t1(c1, c2)";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("c1", "c2").baselineValues(4, 1).go();
  }

  @Test // Except over values with non-matcing null on both sides
  public void testExceptNonMatchingNulls() throws Exception {
    String query =
        ""
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS int), 1), (4, 1)) AS t1(c1, c2)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM (VALUES (1, 2), (1, 1), (CAST(null AS int), 2)) AS t1(c1, c2)";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2")
        .baselineValues(null, 1)
        .baselineValues(4, 1)
        .go();
  }

  @Test // Simple Except over two scans
  public void testExcept1ColFromScan() throws Exception {
    String query =
        "(select n_name as name from cp.\"tpch/nation.parquet\") EXCEPT (select r_name as name from cp.\"tpch/region.parquet\")";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("name")
        .baselineValues("ALGERIA")
        .baselineValues("GERMANY")
        .baselineValues("RUSSIA")
        .baselineValues("ROMANIA")
        .baselineValues("PERU")
        .baselineValues("SAUDI ARABIA")
        .baselineValues("INDIA")
        .baselineValues("VIETNAM")
        .baselineValues("ARGENTINA")
        .baselineValues("CANADA")
        .baselineValues("ETHIOPIA")
        .baselineValues("KENYA")
        .baselineValues("MOROCCO")
        .baselineValues("UNITED STATES")
        .baselineValues("BRAZIL")
        .baselineValues("EGYPT")
        .baselineValues("IRAN")
        .baselineValues("UNITED KINGDOM")
        .baselineValues("INDONESIA")
        .baselineValues("JAPAN")
        .baselineValues("JORDAN")
        .baselineValues("CHINA")
        .baselineValues("FRANCE")
        .baselineValues("IRAQ")
        .baselineValues("MOZAMBIQUE")
        .go();
  }

  @Test // Simple Except over two scans
  public void testExceptFromScan() throws Exception {
    String query =
        "(select n_name as name, n_nationkey as key from cp.\"tpch/nation.parquet\") EXCEPT (select r_name as name, r_regionkey as key from cp.\"tpch/region.parquet\")";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("name", "key")
        .baselineValues("ALGERIA", 0)
        .baselineValues("ROMANIA", 19)
        .baselineValues("PERU", 17)
        .baselineValues("SAUDI ARABIA", 20)
        .baselineValues("INDIA", 8)
        .baselineValues("VIETNAM", 21)
        .baselineValues("ARGENTINA", 1)
        .baselineValues("CANADA", 3)
        .baselineValues("ETHIOPIA", 5)
        .baselineValues("KENYA", 14)
        .baselineValues("MOROCCO", 15)
        .baselineValues("UNITED STATES", 24)
        .baselineValues("BRAZIL", 2)
        .baselineValues("EGYPT", 4)
        .baselineValues("IRAN", 10)
        .baselineValues("UNITED KINGDOM", 23)
        .baselineValues("INDONESIA", 9)
        .baselineValues("RUSSIA", 22)
        .baselineValues("JAPAN", 12)
        .baselineValues("JORDAN", 13)
        .baselineValues("CHINA", 18)
        .baselineValues("FRANCE", 6)
        .baselineValues("GERMANY", 7)
        .baselineValues("IRAQ", 11)
        .baselineValues("MOZAMBIQUE", 16)
        .go();
  }
}
