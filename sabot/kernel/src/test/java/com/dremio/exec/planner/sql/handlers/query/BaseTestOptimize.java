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
package com.dremio.exec.planner.sql.handlers.query;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.sabot.rpc.user.UserSession;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseTestOptimize extends BaseTestQuery {
  private static IcebergTestTables.Table table;
  private static IcebergTestTables.Table tableWithDeletes;
  private static IcebergTestTables.Table tableWithEqDeletes;
  private static SqlConverter converter;
  private static SqlHandlerConfig config;

  // ===========================================================================
  // Test class and Test cases setUp and tearDown
  // ===========================================================================
  @BeforeClass
  public static void setUp() throws Exception {
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
    final AttemptObserver observer =
        new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));

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
  }

  @Before
  public void init() throws Exception {
    table = IcebergTestTables.V2_ORDERS.get();
    tableWithDeletes = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    tableWithEqDeletes = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  @After
  public void tearDown() throws Exception {
    table.close();
    tableWithDeletes.close();
    tableWithEqDeletes.close();
  }

  public static IcebergTestTables.Table getTable() {
    return table;
  }

  public static IcebergTestTables.Table getTableWithDeletes() {
    return tableWithDeletes;
  }

  public static IcebergTestTables.Table getTableWithEqDeletes() {
    return tableWithEqDeletes;
  }

  public static SqlConverter getConverter() {
    return converter;
  }

  public static SqlHandlerConfig getConfig() {
    return config;
  }
}
