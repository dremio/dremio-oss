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

import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME;
import static com.dremio.exec.store.hive.HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE;
import static java.util.regex.Pattern.quote;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.dremio.exec.GuavaPatcherRunner;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.hive.HiveTestDataGenerator;

/**
 * Base class for Hive test. Takes care of adding Hive test plugin before tests and deleting the
 * plugin after tests. This class should replace {@link HiveTestBase} at some point.
 */
@RunWith(GuavaPatcherRunner.class)
public class LazyDataGeneratingHiveTestBase extends PlanTestBase {
  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);

  protected static HiveTestDataGenerator dataGenerator;
  protected static SabotContext sabotContext;

  @BeforeClass
  public static void generateHiveWithoutData() throws Exception{
    dataGenerator = HiveTestDataGenerator.newInstanceWithoutTestData(null);
    Objects.requireNonNull(dataGenerator);

    sabotContext = getSabotContext();
    Objects.requireNonNull(sabotContext);

    dataGenerator.addHiveTestPlugin(HIVE_TEST_PLUGIN_NAME, sabotContext.getCatalogService());
    dataGenerator.addHiveTestPlugin(HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, sabotContext.getCatalogService());
  }

  @AfterClass
  public static void cleanupHiveTestData() {
    if (dataGenerator != null) {
      dataGenerator.deleteHiveTestPlugin(HIVE_TEST_PLUGIN_NAME, sabotContext.getCatalogService());
      dataGenerator.deleteHiveTestPlugin(HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, sabotContext.getCatalogService());
    }
  }

  protected void runPushdownQueryAndVerify(String baseQuery, List<String[]> queryWithResults, String colName) throws Exception {
    for (String[] queryWithResult : queryWithResults) {
      String sqlQuery = baseQuery + queryWithResult[0];
      String planCheck = queryWithResult[1];
      if (planCheck != null) {
        // verify plan
        testPlanMatchingPatterns(
            sqlQuery,
            new String[]{quote("filters=[[Filter on `" + colName + "`: " + planCheck + "(`" + colName + "`")},
            "Filter\\(*\\)");
      }
      TestBuilder testBuilder = getTestBuilder(sqlQuery, queryWithResult);
      testBuilder.go();
    }
  }

  protected TestBuilder getTestBuilder(String sqlQuery, String[] queryWithResult) {
    TestBuilder testBuilder = testBuilder()
        .unOrdered()
        .sqlQuery(sqlQuery)
        .baselineColumns("name");

    if (queryWithResult.length <= 2) {
      testBuilder.expectsEmptyResultSet();
    } else {
      for (int i = 2; i < queryWithResult.length; i++) {
        testBuilder.baselineValues(queryWithResult[i]);
      }
    }

    return testBuilder;
  }

  protected void verifySqlQueryForSingleBaselineColumn(String query, String baselineColumn, Object[] baselineValues) throws Exception {
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(baselineColumn);
    for (Object baselineValue : baselineValues) {
      testBuilder = testBuilder.baselineValues(baselineValue);
    }
    testBuilder.go();
  }
}
