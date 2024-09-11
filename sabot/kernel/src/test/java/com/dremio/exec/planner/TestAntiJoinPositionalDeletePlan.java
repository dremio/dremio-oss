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
package com.dremio.exec.planner;

import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.configureDmlWriteModeProperties;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testDmlQuery;
import static com.dremio.exec.store.SystemSchemas.DELETE_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.POS;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.ROW_INDEX_COLUMN_NAME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.dremio.exec.planner.sql.DmlQueryTestUtils.DmlRowwiseOperationWriteMode;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAntiJoinPositionalDeletePlan extends BaseTestQuery {
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;
  private static SqlConverter converter;
  private static SqlHandlerConfig config;

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

    setSystemOption(
        ExecConstants.ENABLE_READING_POSITIONAL_DELETE_WITH_ANTI_JOIN.getOptionName(), "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER.getOptionName(), "true");
  }

  @Before
  public void setupTest() {}

  @After
  public void tearDownTest() throws Exception {
    resetSystemOption(
        ExecConstants.ENABLE_READING_POSITIONAL_DELETE_WITH_ANTI_JOIN.getOptionName());
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_POSITIONAL_DELETE_WRITER.getOptionName());
  }

  @Test
  public void testAntiJoinPlan() throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SOURCE, 2, 10)) {
      configureDmlWriteModeProperties(table, DmlRowwiseOperationWriteMode.MERGE_ON_READ);

      testDmlQuery(
          getTestAllocator(),
          "UPDATE %s SET id = id",
          new Object[] {table.fqn},
          table,
          10,
          table.originalData);

      final String select = "Select * from " + table.fqn;

      NormalHandler handler = new NormalHandler();
      handler.getPlan(config, null, converter.parse(select));
      Prel plan = handler.getPrel();

      // find the count of user columns
      ScreenPrel screenPrel = findSingleNode(plan, ScreenPrel.class, null);
      int userColumnCount = screenPrel.getRowType().getFieldCount();

      // find the filter operator of the anti-join
      FilterPrel filterPrel = findSingleNode(plan, FilterPrel.class, null);
      assertThat(filterPrel.getCondition().toString())
          .isEqualTo(String.format("IS NULL($%s)", userColumnCount + 2));

      assertThat(filterPrel.getInput(0)).isInstanceOf(HashJoinPrel.class);
      HashJoinPrel hashJoinPrel = (HashJoinPrel) filterPrel.getInput(0);
      assertThat(hashJoinPrel.getJoinType()).isEqualTo(JoinRelType.LEFT);
      assertThat(hashJoinPrel.getCondition().toString())
          .isEqualTo(
              String.format(
                  "AND(=($%s, $%s), =($%s, $%s))",
                  userColumnCount, userColumnCount + 2, userColumnCount + 1, userColumnCount + 3));

      // check data file side
      RelNode dataFileScan = hashJoinPrel.getLeft();
      assertThat(dataFileScan).isInstanceOf(TableFunctionPrel.class);
      assertThat(dataFileScan.getRowType().getFieldNames())
          .containsAll(ImmutableList.of(FILE_PATH_COLUMN_NAME, ROW_INDEX_COLUMN_NAME));

      // check delete file side
      RelNode deleteFileScan = hashJoinPrel.getRight();
      assertThat(deleteFileScan).isInstanceOf(TableFunctionPrel.class);
      assertThat(deleteFileScan.getRowType().getFieldNames())
          .containsAll(ImmutableList.of(DELETE_FILE_PATH, POS));
    }
  }
}
