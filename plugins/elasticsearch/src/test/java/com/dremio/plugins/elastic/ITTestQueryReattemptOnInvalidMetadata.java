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

import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.google.common.collect.Lists;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class ITTestQueryReattemptOnInvalidMetadata extends ElasticBaseTestQuery {

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public static ColumnData[] getGodFather(String title, int year) {
    return new ColumnData[] {
      new ColumnData("title", ElasticsearchType.TEXT, new Object[][] {{title}}),
      new ColumnData("director", ElasticsearchType.TEXT, new Object[][] {{DIR}}),
      new ColumnData("year", ElasticsearchType.INTEGER, new Object[][] {{year}})
    };
  }

  private static final String GF1 = "The Godfather";
  private static final String GF2 = "The Godfather 2";
  private static final String DIR = "Francis Ford Coppola";

  private static ColumnData[] getGodFather1() {
    return getGodFather(GF1, 1972);
  }

  private static ColumnData[] getGodFather2() {
    return getGodFather(GF2, 1974);
  }

  @Test
  public void changeTablesInAliasAndNoticeReattempt() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = true");
    try {
      final String movieType = TestNameGenerator.tableName();

      final String gf1 = TestNameGenerator.schemaName();
      elastic.load(gf1, movieType, getGodFather1());

      final String aliasName = TestNameGenerator.aliasName();
      elastic.alias(aliasName, gf1);

      testBuilder()
          .sqlQuery(
              "SELECT \"year\", \"director\", \"title\" FROM elasticsearch.%s.%s",
              aliasName, movieType)
          .unOrdered()
          .baselineColumns("title", "director", "year")
          .baselineValues(GF1, DIR, 1972)
          .go();

      final String gf2 = TestNameGenerator.schemaName();
      elastic.load(gf2, movieType, getGodFather2());

      elastic.alias(
          Lists.newArrayList(
              new ElasticTestActions.AliasActionDef(
                  ElasticTestActions.AliasActionType.ADD, gf2, aliasName),
              new ElasticTestActions.AliasActionDef(
                  ElasticTestActions.AliasActionType.REMOVE, gf1, aliasName)));

      elastic.wipe(gf1);

      testBuilder()
          .sqlQuery(
              "SELECT \"year\", \"director\", \"title\" FROM elasticsearch.%s.%s",
              aliasName, movieType)
          .unOrdered()
          .baselineColumns("title", "director", "year")
          .baselineValues(GF2, DIR, 1974)
          .go();
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\"");
    }
  }
}
