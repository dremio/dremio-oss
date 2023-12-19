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
package com.dremio.exec;

import java.io.File;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.BaseTestQuery;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Base class for tests to look at pure plans
 */
public class PlanOnlyTestBase extends BaseTestQuery {

  private static final String TEST_PATH = TestTools.getWorkingPath() + "/src/test/resources";
  private static File tblPath = null;
  protected static CloseableSchedulerThreadPool closeableSchedulerThreadPool;

  @BeforeClass
  public static void createTable() throws Exception {
    updateTestCluster(10, config);

    tblPath = new File(getDfsTestTmpSchemaLocation(), "yelp");
    FileUtils.deleteQuietly(tblPath);
    FileUtils.copyFileToDirectory(new File(TEST_PATH + "/yelp_business.json"), tblPath);
    FileUtils.moveFile(new File(tblPath + "/yelp_business.json"), new File(tblPath + "/1.json"));
    FileUtils.copyFile(new File(tblPath + "/1.json"), new File(tblPath + "/2.json"));

    closeableSchedulerThreadPool = new CloseableSchedulerThreadPool("cancel-fragment-retry-", 1);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    FileUtils.deleteQuietly(tblPath);
    closeableSchedulerThreadPool.close();
  }

  @SafeVarargs
  protected final SabotContext createSabotContext(Supplier<OptionValue>... optionValues) {
    final SabotContext context = getSabotContext();
    for (Supplier<OptionValue> optionValueSupplier : optionValues) {
      context.getOptionManager().setOption(optionValueSupplier.get());
    }
    return context;
  }

  protected QueryContext createContext(SabotContext context) {
    final QueryContext queryContext = new QueryContext(session(), context, UserBitShared.QueryId.getDefaultInstance());
    queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
    return queryContext;
  }

  protected PhysicalPlan createPlan(String sql, QueryContext queryContext) throws Exception {
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    final SqlConverter converter = new SqlConverter(
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
    final SqlNode node = converter.parse(sql);
    final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);

    final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvert(config, node);
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();

    final Rel drel = DrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

    final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
    final Prel prel = convertToPrel.getKey();

    final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
    return PrelTransformer.convertToPlan(config, pop);
  }

  private static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.ANONYMOUS).build())
      .setSupportComplexTypes(true)
      .build();
  }
}
