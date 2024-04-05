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
package com.dremio.exec.physical.impl.flatten;

import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.PlanTestBase;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.test.TemporarySystemProperties;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class TestFlattenPlanning extends PlanTestBase {

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testFlattenPlanningAvoidUnnecessaryProject() throws Exception {
    // Because of Java7 vs Java8 map ordering differences, we check for both cases
    // See DRILL-4331 for details
    testPlanMatchingPatterns(
        "select flatten(complex), rownum from cp.\"/store/json/test_flatten_mappify2.json\"",
        new String[] {
          "\\QProject(EXPR$0=[$1], rownum=[$0])\\E|\\QProject(EXPR$0=[$0], rownum=[$1])\\E"
        },
        new String[] {
          "\\QProject(EXPR$0=[$0], EXPR$1=[$1], EXPR$3=[$1])\\E|\\QProject(EXPR$0=[$1], EXPR$1=[$0], EXPR$3=[$0])\\E"
        });
  }

  @Test
  public void testPushFilterPastProjectWithFlatten() throws Exception {
    final String query =
        " select comp, rownum "
            + " from (select flatten(complex) comp, rownum "
            + "      from cp.\"/store/json/test_flatten_mappify2.json\") "
            + " where comp IS NOT NULL "
            + // should not be pushed down
            "   and rownum = 100"; // should be pushed down.

    final String[] expectedPlans = {"(?s)Filter.*IS NOT NULL.*Flatten.*Filter.*=.*"};
    final String[] excludedPlans = {"Filter.*AND.*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlans, excludedPlans);
  }

  @Test // DRILL-4121 : push partial filter past projects : neg test case
  public void testPushFilterPastProjectWithFlattenNeg() throws Exception {
    final String query =
        " select comp, rownum "
            + " from (select flatten(complex) comp, rownum "
            + "      from cp.\"/store/json/test_flatten_mappify2.json\") t"
            + " where comp IS NOT NULL "
            + // should NOT be pushed down
            "   OR rownum = 100"; // should NOT be pushed down.

    final String[] expectedPlans = {"(?s)Filter.*OR.*Flatten"};
    final String[] excludedPlans = {"(?s)Filter.*Flatten.*Filter.*"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlans, excludedPlans);
  }

  @Test
  public void dx26675() throws Exception {
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      final String vds =
          "create vds dfs_test.flatten26675 as SELECT * FROM cp.\"flatten/dx26675.json\"";
      testNoResult(vds);

      final String query =
          "SELECT t2.flattened.a, t2.flattened.b "
              + "FROM (select flatten(t1.list_col) as flattened from dfs_test.flatten26675 as t1) t2";

      test(query);
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void dx8383_flatten_lost() throws Exception {
    try {
      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      final String vds =
          "create vds dfs_test.flatten1 as SELECT float_col, int_col, int_list_col, float_list_col, bool_list_col, flatten(str_list_col) AS str_list_col, str_list_list_col, order_list, user_map, int_text_col, float_text_col, time_text_col, timestamp_text_col, date_text_col, splittable_col, address, text_col, bool_col\n"
              + "FROM cp.\"flatten/all_types_dremio.json\"";
      testNoResult(vds);

      final String onvds =
          "SELECT str_list_col, flatten(str_list_list_col[0]) AS A\n" + "FROM dfs_test.flatten1";

      PlanTestBase.testPlanMatchingPatterns(
          onvds,
          new String[] {
            "(?s)Flatten\\(flattenField=\\[\\$0\\]\\).*Flatten\\(flattenField=\\[\\$0\\]\\)"
          },
          new String[] {});
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }

  @Test
  public void checkInfinitePlanningDX7953() throws Exception {
    final String query =
        "SELECT nested_1.data.v AS v, tagid\n"
            + "FROM (\n"
            + "  SELECT flatten(nested_0.tagList.data) AS data, nested_0.tagList.tagId AS tagid\n"
            + "  FROM (\n"
            + "    SELECT flatten(tagList) AS tagList\n"
            + "    FROM cp.\"/store/json/doubleflatten.json\") nested_0\n"
            + ") nested_1\n"
            + "WHERE tagId = 1";
    // make sure we don't timeout.
    test(query);
  }

  @Ignore("DX-7987")
  @Test
  public void pushFilterBelowFlatten() throws Exception {
    final String query =
        "SELECT nested_1.data.v AS v, tagid\n"
            + "FROM (\n"
            + "  SELECT flatten(nested_0.tagList.data) AS data, nested_0.tagList.tagId AS tagid\n"
            + "  FROM (\n"
            + "    SELECT flatten(tagList) AS tagList\n"
            + "    FROM cp.\"/store/json/doubleflatten.json\") nested_0\n"
            + ") nested_1\n"
            + "WHERE tagId = 1";
    // make sure filter is below first flatten.
    PlanTestBase.testPlanMatchingPatterns(
        query, new String[] {"(?s)Flatten.*Filter.*Flatten"}, new String[] {});
  }

  // Test flatten nullability in RelNode. Flatten's is nullable should always be true. Incorrect
  // handling of nullability
  // may cause ExpansionNode to have different row type then its input node (DX-68750).
  @Test
  public void testFattenNullabilityDX68750() throws Exception {
    class MyAttemptObserver extends AbstractAttemptObserver {
      private RelNode convertedRel;

      public RelNode getConvertedRel() {
        return convertedRel;
      }

      @Override
      public void planConvertedToRel(RelNode converted, long millisTaken) {
        convertedRel = converted;
      }
    }

    try {
      SqlConverter converter;
      SqlHandlerConfig config;
      SabotContext context = getSabotContext();
      UserSession session =
          UserSession.Builder.newBuilder()
              .withSessionOptionManager(
                  new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
                  getSabotContext().getOptionManager())
              .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
              .withCredentials(
                  UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
              .setSupportComplexTypes(true)
              .build();
      final QueryContext queryContext =
          new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
      queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
      final MyAttemptObserver observer = new MyAttemptObserver();
      converter =
          new SqlConverter(
              queryContext.getPlannerSettings(),
              queryContext.getOperatorTable(),
              queryContext,
              queryContext.getMaterializationProvider(),
              queryContext.getFunctionRegistry(),
              queryContext.getSession(),
              observer,
              queryContext.getSubstitutionProviderFactory(),
              queryContext.getConfig(),
              queryContext.getScanResult(),
              queryContext.getRelMetadataQuerySupplier());

      config = new SqlHandlerConfig(queryContext, converter, observer, null);

      properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
      String createViewSql =
          String.format(
              "CREATE VDS dfs_test.v_test as SELECT n_nationkey, FLATTEN(CONVERT_FROM(n_name, 'JSON')) AS flat from cp.\"tpch/nation.parquet\" ");
      runSQL(createViewSql);

      String sql = String.format("select * from dfs_test.v_test");

      SqlNode node = converter.parse(sql);
      SqlToRelTransformer.validateAndConvert(config, node);
      RelNode convertedRel = observer.getConvertedRel();
      ExpansionNode expansionNode = findSingleNode(convertedRel, ExpansionNode.class, null);
      RelNode input = expansionNode.getInput();

      assertNotNull(expansionNode.getRowType());
      assertNotNull(input.getRowType());
      assertEquals(2, expansionNode.getRowType().getFieldList().size());
      assertEquals(2, input.getRowType().getFieldList().size());
      assertEquals("flat", expansionNode.getRowType().getFieldList().get(1).getKey());
      assertEquals("flat", input.getRowType().getFieldList().get(1).getKey());
      assertTrue(expansionNode.getRowType().getFieldList().get(1).getValue().isNullable());
      assertTrue(input.getRowType().getFieldList().get(1).getValue().isNullable());
      assertTrue(
          MoreRelOptUtil.areRowTypesEqual(
              expansionNode.getRowType(), input.getRowType(), true, true));
    } finally {
      properties.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    }
  }
}
