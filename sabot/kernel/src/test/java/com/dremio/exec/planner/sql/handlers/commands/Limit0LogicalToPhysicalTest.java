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
package com.dremio.exec.planner.sql.handlers.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.io.FileUtils;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.options.OptionValue;
import com.dremio.resource.basic.QueueType;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.DirectProvider;

/**
 * To test that Limit0 converter does not cause removal of the exchanges
 */
public class Limit0LogicalToPhysicalTest extends BaseTestQuery {

  private static String TEST_PATH = TestTools.getWorkingPath() + "/src/test/resources";
  private static File tblPath = null;

  @BeforeClass
  public static void createTable() throws Exception {
    tblPath = new File(getDfsTestTmpSchemaLocation(), "yelp");
    FileUtils.deleteQuietly(tblPath);
    FileUtils.copyFileToDirectory(new File(TEST_PATH + "/yelp_business.json"), tblPath);
    FileUtils.moveFile(new File(tblPath + "/yelp_business.json"), new File(tblPath + "/1.json"));
    FileUtils.copyFile(new File(tblPath + "/1.json"), new File(tblPath + "/2.json"));
  }

  @AfterClass
  public static void cleanUpTable() throws Exception {
    FileUtils.deleteQuietly(tblPath);
  }

  @Test
  public void ExchangesKeepTest() throws Exception {

    final String yelpTable = TEMP_SCHEMA + ".\"yelp\"";
    final String sql = "SELECT nested_0.review_id AS review_id, nested_0.user_id AS user_id, nested_0.votes AS votes," +
      " nested_0.stars AS stars, join_business.business_id AS business_id0, join_business.neighborhoods AS neighborhoods, join_business.city AS city, join_business.latitude AS latitude, join_business.review_count AS review_count, join_business.full_address AS full_address, join_business.stars AS stars0, join_business.categories AS categories, join_business.state AS state, join_business.longitude AS longitude\n" +
      "FROM (\n" +
      "  SELECT review_id, user_id, votes, stars, business_id\n" +
      "  FROM cp.\"yelp_review.json\" where 1 = 0\n" +
      ") nested_0\n" +
      " FULL JOIN " + yelpTable + " AS join_business ON nested_0.business_id = join_business.business_id";

    final SabotContext context = getSabotContext();
    context.getOptionManager().setOption(
      OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1)
    );
    context.getOptionManager().setOption(
      OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.width.max_per_node", 10)
    );
    context.getOptionManager().setOption(
      OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "planner.enable_mux_exchange", true)
    );

    final QueryContext queryContext = new QueryContext(session(), context, UserBitShared.QueryId.getDefaultInstance());
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter = new SqlConverter(
        queryContext.getPlannerSettings(),
        queryContext.getOperatorTable(),
        queryContext,
        queryContext.getMaterializationProvider(),
        queryContext.getFunctionRegistry(),
        queryContext.getSession(),
        observer,
        queryContext.getCatalog(),
        queryContext.getSubstitutionProviderFactory());
    final SqlNode node = converter.parse(sql);
    final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);

    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();

    final Rel drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

    final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
    final Prel prel = convertToPrel.getKey();
    final String prePhysicaltextPlan = convertToPrel.getValue();

    assertThat(prePhysicaltextPlan, CoreMatchers.containsString("HashToRandomExchange"));
    assertThat(prePhysicaltextPlan, CoreMatchers.containsString("UnorderedMuxExchange"));
    assertThat(prePhysicaltextPlan, CoreMatchers.containsString("Empty"));
    assertThat(prePhysicaltextPlan, CoreMatchers.containsString("EasyScan"));

    final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
    final PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
    final String postPhysicaltextPlan = plan.unparse(config.getContext().getLpPersistence().getMapper().writer());

    assertThat(postPhysicaltextPlan, CoreMatchers.containsString("EmptyValues"));
    assertThat(postPhysicaltextPlan, CoreMatchers.containsString("EasyGroupScan"));
    assertThat(postPhysicaltextPlan, CoreMatchers.containsString("unordered-mux-exchange"));
    assertThat(postPhysicaltextPlan, CoreMatchers.containsString("hash-to-random-exchange"));


    PhysicalPlanReader pPlanReader = new PhysicalPlanReader(
      DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT, new LogicalPlanPersistence(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT),
      CoordinationProtos.NodeEndpoint.getDefaultInstance(),
      DirectProvider.wrap(Mockito.mock(CatalogService.class)), context);

    ExecutionPlan exec = ExecutionPlanCreator.getExecutionPlan(queryContext, pPlanReader, observer, plan,
      QueueType.SMALL);
    List<CoordExecRPC.PlanFragment> fragments  = exec.getFragments();

    int scanFrags = 0;
    for (CoordExecRPC.PlanFragment fragment : fragments) {
      if (new String(fragment.getFragmentJson().toByteArray()).contains("easy-sub-scan")) {
        scanFrags++;
      }
    }
    assertEquals(2, scanFrags);

  }

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
      .withOptionManager(getSabotContext().getOptionManager())
      .setSupportComplexTypes(true)
      .build();
  }

}
