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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.tableName;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.TestBuilder;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.google.common.collect.Lists;

public class ITTestProjectPushdown extends ElasticBaseTestQuery {
  private String PARQUET_TABLE = null;
  private String ELASTIC_TABLE = null;

  private static final String NEW_COLUMN_1 = "/json/new_column/file1.json";
  private static final String NEW_COLUMN_2 = "/json/new_column/file2.json";
  private static final String NEW_COLUMN_MAPPING = "/json/new_column/mapping.json";

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    ColumnData[] data = getBusinessData();
    load(schema, table, data);
    PARQUET_TABLE = "dfs.\"[WORKING_PATH]/src/test/resources/small_business.parquet\"";
    ELASTIC_TABLE = String.format("elasticsearch.%s.%s", schema, table);

  }

  /**
   * Tests function evaluation of Dremio's native expressions in comparison to
   * expressions that are pushed into elastic.
   * @throws Exception
   */
  @Test
  public void testUnaryNumericFunctions() throws Exception {
    final List<String> unaryFunctions = Lists.newArrayList(
            "ABS", "ACOS", "ASIN", "ATAN", "COS", "CEILING", /*"COT",*/ "DEGREES", "FLOOR", "RADIANS", "SIN", "TAN", "EXP", "SIGN", "SQRT"
    );

    final String exprs = " cast(%s(stars) as double), cast(%s(review_count) as double)";
    for (String s : unaryFunctions) {
      String elasticQuery = String.format("select " + exprs + " from %s", s, s, ELASTIC_TABLE);
      testBuilder()
              .sqlQuery(elasticQuery)
              .unOrdered()
              .sqlBaselineQuery(String.format("select " + exprs + " from %s", s, s, PARQUET_TABLE))
              .go();
    }
  }

  @Test
  public void testBinaryNumericFunctions() throws Exception {
    final List<String> binaryFunctions = Lists.newArrayList("+", "-", "*" /*, "/"*/);

    // sanity check the parquet file
    // NOTE: this uses the test builder constructor directly instead of the testBuilder() helper method
    // because of an elastic specific override to run tests twice (disabling project pushdown on one run)
    // this is not needed here and using it actually causes a failure because of the simple way it rewrites
    // queries (looking for elasticsearch and replacing it, a word that appears in this path...)
    new TestBuilder(allocator)
        .sqlQuery("select stars, review_count from " + PARQUET_TABLE)
        .unOrdered()
        .baselineColumns("stars", "review_count")
        .baselineValues(4.5f, 11)
        .baselineValues(3.5f, 22)
        .baselineValues(5.0f, 33)
        .baselineValues(4.5f, 11)
        .baselineValues(1f, 1)
        .go();

    final String exprs = " cast((stars %s review_count) as double), cast((stars %s stars) as double), cast((review_count %s review_count) as double) ";
    for (String s : binaryFunctions) {
      String elasticQuery = String.format("select " + exprs + " from %s", s, s, s, ELASTIC_TABLE);
      testBuilder()
              .sqlQuery(elasticQuery)
              .unOrdered()
              .sqlBaselineQuery(String.format("select " + exprs + " from %s", s, s, s, PARQUET_TABLE))
              .go();
    }
  }

  @Test
  public void testBinaryPrefixNumericFunctions() throws Exception {
    final String exprs = " cast(%s(stars, review_count) as double), cast(%s(stars, stars) as double), cast(%s(review_count, review_count) as double) ";
    String s = "power";
    testBuilder()
            .sqlQuery(String.format("select " + exprs + " from %s", s, s, s, ELASTIC_TABLE))
            .unOrdered()
            .sqlBaselineQuery(String.format("select " + exprs + " from %s", s, s, s, PARQUET_TABLE))
            .go();
  }

  @Test
  public void testIntOnlyNumericFunctions() throws Exception {
    final String exprs = " cast(%s(review_count, cast(stars as integer)) as double), cast(%s(review_count, review_count) as double) ";
    final List<String> intOnlyFuncs = Lists.newArrayList("mod");
    for (String s : intOnlyFuncs) {
      String elasticQuery = String.format("select " + exprs + " from %s", s, s, ELASTIC_TABLE);
      testBuilder()
              .sqlQuery(elasticQuery)
              .unOrdered()
              .sqlBaselineQuery(String.format("select " + exprs + " from %s", s, s, PARQUET_TABLE))
              .go();
    }
  }

  @Test
  public void testCastIntToVarchar() throws Exception {
    final String sqlQueryBasic = "select city from %s where cast(review_count as varchar) = '33'";
    testBuilder()
            .sqlQuery(String.format(sqlQueryBasic, ELASTIC_TABLE))
            .unOrdered()
            .baselineColumns("city")
            .baselineValues("San Diego")
            .go();
    // There is some logic to allow some casts to be handled by elastic implicit conversion
    // in the PredicateAnalyzer, which constructs an equality or range filter.
    // Concat is added here to force scripts to be used, specifically to test the
    // cast within a script.
    String sqlQuery = "select city from %s where concat(cast(review_count as varchar), '') = '33'";
    testBuilder()
            .sqlQuery(String.format(sqlQuery, ELASTIC_TABLE))
            .unOrdered()
            .baselineColumns("city")
            .baselineValues("San Diego")
            .go();

    testBuilder()
            .sqlQuery(String.format(sqlQuery, ELASTIC_TABLE))
            .unOrdered()
            .sqlBaselineQuery(String.format(sqlQuery, PARQUET_TABLE))
            .go();

    testPlanSubstrPatterns(String.format(sqlQuery, ELASTIC_TABLE), new String[]{
        "(doc[\\\"review_count\\\"].empty) ? false : ( ( Long.toString(doc[\\\"review_count\\\"].value) + '' ) == '33' )"
    }, null);
  }

  @Test
  public void testNewColumns() throws Exception {
    String schema = schemaName();
    String table = tableName();
    elastic.load(schema, table, NEW_COLUMN_MAPPING);
    elastic.dataFromFile(schema, table, NEW_COLUMN_1);
    test(String.format("select * from elasticsearch.%s.%s", schema, table));
    elastic.dataFromFile(schema, table, NEW_COLUMN_2);

    try {
      test(String.format("select * from elasticsearch.%s.%s", schema, table));
    } catch (Exception e) {
      // ignore, learning schema
    }
    testBuilder()
      .sqlQuery(String.format("select h from elasticsearch.%s.%s", schema, table))
      .unOrdered()
      .baselineColumns("h")
      .baselineValues((Integer) null)
      .baselineValues(1)
      .go();
  }
}
