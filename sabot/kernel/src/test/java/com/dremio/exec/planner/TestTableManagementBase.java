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
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class TestTableManagementBase extends BaseTestQuery {
  private static QueryContext queryContext;
  protected static IcebergTestTables.Table table;
  protected static SqlConverter converter;
  protected static SqlHandlerConfig config;
  protected static int userColumnCount;
  protected static List<RelDataTypeField> userColumnList;

  // ===========================================================================
  // Test class and Test cases setUp and tearDown
  // ===========================================================================
  @BeforeClass
  public static void setUp() throws Exception {
    SabotContext context = getSabotContext();

    UserSession session =
        UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                new SessionOptionManagerImpl(context.getOptionValidatorListing()),
                context.getOptionManager())
            .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
            .withCredentials(
                UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
            .setSupportComplexTypes(true)
            .build();

    queryContext = new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
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
    userColumnList = getOriginalFieldList();
    userColumnCount = userColumnList.size();
    // table has at least one column
    assertThat(userColumnCount).isGreaterThan(0);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    queryContext.close();
  }

  protected static Prel validateRowCountTopProject(Prel plan) {
    Map<String, String> attributes =
        ImmutableMap.of(
            "exps",
            "[CASE(IS NULL($0), 0, $0)]",
            "rowType",
            String.format("RecordType(BIGINT %s)", RecordWriter.RECORDS.getName()));
    return findSingleNode(plan, ProjectPrel.class, attributes);
  }

  @Before
  public void init() throws Exception {
    table = IcebergTestTables.V2_ORDERS.get();
  }

  @After
  public void tearDown() throws Exception {
    table.close();
  }

  protected static List<RelDataTypeField> getOriginalFieldList() {
    final String select =
        "select * from " + IcebergTestTables.V2_ORDERS.get().getTableName() + " limit 1";
    try {
      final SqlNode node = converter.parse(select);
      final ConvertedRelNode convertedRelNode =
          SqlToRelTransformer.validateAndConvertForDml(config, node, null);
      return convertedRelNode.getValidatedRowType().getFieldList();
    } catch (Exception ex) {
      fail(String.format("Query %s failed, exception: %s", select, ex));
    }
    return Collections.emptyList();
  }

  protected void testResultColumnName(String query) throws Exception {
    TableModify.Operation operation = null;
    if (query.toUpperCase().startsWith("DELETE")) {
      operation = TableModify.Operation.DELETE;
    } else if (query.toUpperCase().startsWith("MERGE")) {
      operation = TableModify.Operation.MERGE;
    } else if (query.toUpperCase().startsWith("UPDATE")) {
      operation = TableModify.Operation.UPDATE;
    } else {
      fail("This should never happen - DELETE/UPDATE/MERGE only");
    }

    assertThat(
            SqlToRelTransformer.validateAndConvertForDml(config, converter.parse(query), null)
                .getConvertedNode()
                .getRowType()
                .getFieldNames()
                .get(0))
        .isEqualTo(DmlUtils.DML_OUTPUT_COLUMN_NAMES.get(operation));
  }

  protected Prel getDmlPlan(String sql) throws Exception {
    return TestDml.getDmlPlan(config, converter.parse(sql));
  }
}
