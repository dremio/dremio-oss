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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.CreateTableHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;

public class TestIcebergCtasPlan extends PlanTestBase {

  @Test
  public void testHashExchangeInIcebergCTAS() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String workingPath = TestTools.getWorkingPath();
      final String path = workingPath + "/src/test/resources/parquet/ctasint";
      String sql = String.format("CREATE TABLE %s.tbl as select * from dfs.\"%s\"", TEMP_SCHEMA, path);
      final SabotContext context = getSabotContext();
      final OptionManager optionManager = getSabotContext().getOptionManager();
      optionManager.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "dremio.iceberg.enabled", true));
      optionManager.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, "planner.slice_target", 1));
      final UserSession session = UserSession.Builder.newBuilder()
        .withSessionOptionManager(
          new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
          optionManager)
        .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.TEST_USER_1).build())
        .build();
      final QueryContext queryContext = new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
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
        queryContext.getSubstitutionProviderFactory(),
        queryContext.getConfig(),
        queryContext.getScanResult(),
        queryContext.getRelMetadataQuerySupplier());
      final SqlNode node = converter.parse(sql);
      final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, converter, observer, null);

      CreateTableHandler createTableHandler = new CreateTableHandler();
      createTableHandler.getPlan(config, sql, node);

      testMatchingPatterns(createTableHandler.getTextPlan(), new String[] {
        // We should have all these operators
        "WriterCommitter",
        "UnionExchange",
        "Writer",
        "HashToRandomExchange",
        "Project",
        "Writer",
        "IcebergManifestList",

        // The operators should be in this order
        "(?s)" +
          "WriterCommitter.*" +
          "UnionExchange.*" +
          "Writer.*" +
          "HashToRandomExchange.*" +
          "Project.*" + HashPrelUtil.HASH_EXPR_NAME + ".*" + // HashProject
          "Writer.*" +
          "IcebergManifestList.*"});
    }
  }
}
