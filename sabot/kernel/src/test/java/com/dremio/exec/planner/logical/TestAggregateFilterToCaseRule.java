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
package com.dremio.exec.planner.logical;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestAggregateFilterToCaseRule extends PlanTestBase {
  @Test
  public void testSum() throws Exception {
    final String withFilter = String.format(
      "SELECT SUM(sales) filter (where ppu < .7) as sum_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT SUM(CASE WHEN ppu < .7 THEN sales END) as sum_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("sum_sales_filter_ppu")
        .baselineValues(494L)
        .go();
    }
  }

  @Test
  public void testCount() throws Exception {
    final String withFilter = String.format(
      "SELECT COUNT(sales) filter (where ppu < .7) as count_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT COUNT(CASE WHEN ppu < .7 THEN sales END) as count_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("count_sales_filter_ppu")
        .baselineValues(4L)
        .go();
    }
  }

  @Test
  public void testCountStar() throws Exception {
    final String withFilter = String.format(
      "SELECT COUNT(*) filter (where ppu < .7) as count_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT COUNT(CASE WHEN ppu < .7 THEN sales END) as count_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), false, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("count_filter_ppu")
        .baselineValues(4L)
        .go();
    }
  }

  @Test
  public void testMax() throws Exception {
    final String withFilter = String.format(
      "SELECT MAX(sales) filter (where ppu < .7) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT MAX(CASE WHEN ppu < .7 THEN sales END) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("max_sales_filter_ppu")
        .baselineValues(300L)
        .go();
    }
  }

  @Test
  public void testMin() throws Exception {
    final String withFilter = String.format(
      "SELECT MIN(sales) filter (where ppu < .7) as min_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT MIN(CASE WHEN ppu < .7 THEN sales END) as min_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("min_sales_filter_ppu")
        .baselineValues(14L)
        .go();
    }
  }

  @Test
  public void testAverage() throws Exception {
    final String withFilter = String.format(
      "SELECT AVG(sales) filter (where ppu < .7) as avg_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT AVG(CASE WHEN ppu < .7 THEN sales END) as avg_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("avg_sales_filter_ppu")
        .baselineValues(123.5)
        .go();
    }
  }

  @Test
  public void testMedian() throws Exception {
    final String withFilter = String.format(
      "SELECT MEDIAN(sales) filter (where ppu < .7) as median_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT MEDIAN(CASE WHEN ppu < .7 THEN sales END) as median_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("median_sales_filter_ppu")
        .baselineValues(90.0)
        .go();
    }
  }

  @Test
  public void testMultipleAggregates() throws Exception {
    final String withFilter = String.format(
      "SELECT MIN(sales) filter (where ppu < .7) as min_sales_filter_ppu, MAX(sales) filter (where ppu < .71) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT MIN(CASE WHEN ppu < .7 THEN sales END) as min_sales_filter_ppu, MAX(CASE WHEN ppu < .71 THEN sales END) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]
        {
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)",
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.71\\)\\), \\$1, null\\)"
        });

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("min_sales_filter_ppu", "max_sales_filter_ppu")
        .baselineValues(14L, 300L)
        .go();
    }
  }

  @Test
  public void testMultipleAggregatesWithMedian() throws Exception {
    final String withFilter = String.format(
      "SELECT MIN(sales) filter (where ppu < .7) as min_sales_filter_ppu, MEDIAN(sales) filter (where ppu < .71) as median_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT MIN(CASE WHEN ppu < .7 THEN sales END) as min_sales_filter_ppu, MEDIAN(CASE WHEN ppu < .71 THEN sales END) as median_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]
        {
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)",
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.71\\)\\), \\$1, null\\)"
        });

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("min_sales_filter_ppu", "median_sales_filter_ppu")
        .baselineValues(14L, 90.0)
        .go();
    }
  }
  @Test
  public void testMixedFilterNoFilterAggregates() throws Exception {
    final String withFilter = String.format(
      "SELECT " +
        "MIN(sales) filter (where ppu < .7) as min_sales_filter_ppu, " +
        "SUM(sales) as sum_sales, " +
        "MAX(sales) filter (where ppu < .71) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT " +
        "MIN(CASE WHEN ppu < .7 THEN sales END) as min_sales_filter_ppu, " +
        "SUM(sales) as sum_sales, " +
        "MAX(CASE WHEN ppu < .71 THEN sales END) as max_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]
        {
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.7\\)\\), \\$1, null\\)",
          "CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\.71\\)\\), \\$1, null\\)"
        });

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("min_sales_filter_ppu", "sum_sales", "max_sales_filter_ppu")
        .baselineValues(14L, 1194L, 300L)
        .go();
    }
  }

  @Test
  public void testMaxWithGroupBy() throws Exception {
    final String withFilter = String.format(
      "SELECT id, MAX(sales) filter (where ppu < .7) as max_sales_filter_ppu" +
        "\nFROM %s " +
        "GROUP BY id " +
        "ORDER BY id",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT id, MAX(sales) filter (where ppu < .7) as max_sales_filter_ppu" +
        "\nFROM %s " +
        "GROUP BY id " +
        "ORDER BY id",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$1, 0\\.7\\)\\), \\$2, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("id", "max_sales_filter_ppu")
        .baselineValues("0001", 35L)
        .baselineValues("0002", 145L)
        .baselineValues("0003", 300L)
        .baselineValues("0004", 14L)
        .baselineValues("0005", null)
        .go();
    }
  }

  @Test
  public void testSumOnEmptySet() throws Exception {
    // When we sum on an empty set we expect to get a null instead of 0.
    final String withFilter = String.format(
      "SELECT SUM(sales) filter (where ppu < 0) as sum_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    final String withCase = String.format(
      "SELECT SUM(CASE WHEN ppu < 0 THEN sales END)  as sum_sales_filter_ppu" +
        "\nFROM %s",
      "cp.\"donuts.json\"");

    testPlanMatchingPatterns(
      withFilter,
      new String[]{"CASE\\(IS TRUE\\(\\<\\(\\$0, 0\\)\\), \\$1, null\\)"});

    final List<String> queries = new ArrayList<>();
    queries.add(withFilter);
    queries.add(withCase);

    for(String query : queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("sum_sales_filter_ppu")
        .baselineValues(null)
        .go();
    }
  }
}
