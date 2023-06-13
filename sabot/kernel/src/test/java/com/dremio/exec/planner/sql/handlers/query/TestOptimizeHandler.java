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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public class TestOptimizeHandler extends BaseTestQuery {

  private static SqlConverter converter;
  private static SqlHandlerConfig config;

  @BeforeClass
  public static void setup() throws Exception {
    SabotContext context = getSabotContext();

    UserSession session = UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(SYSTEM_USERNAME).build())
      .setSupportComplexTypes(true)
      .build();

    final QueryContext queryContext = new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());
    queryContext.setGroupResourceInformation(context.getClusterResourceInformation());
    final AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));

    converter = new SqlConverter(
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

    config = new SqlHandlerConfig(queryContext, converter, observer, null);
  }

  @Test
  public void testNonexistentTable() throws Exception {
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode("OPTIMIZE TABLE a.b.c");
    OptimizeHandler optimizeHandler = (OptimizeHandler) sqlOptimize.toPlanHandler();

    assertThatThrownBy(() -> optimizeHandler.getPlan(config, "OPTIMIZE TABLE a.b.c", sqlOptimize))
      .isInstanceOf(UserException.class)
      .hasMessage("Table [a.b.c] does not exist.");
  }

  @Test
  public void testV2TableWithDeletes() throws Exception {
    IcebergTestTables.Table table = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
    config.getContext().getOptions().setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM,
      ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE.getOptionName(), true));
    String query = String.format("OPTIMIZE TABLE %s", table.getTableName());
    SqlOptimize sqlOptimize = parseToSqlOptimizeNode(query);
    NamespaceKey path = sqlOptimize.getPath();
    OptimizeHandler optimizeHandler = (OptimizeHandler) sqlOptimize.toPlanHandler();

    assertThatThrownBy(() -> optimizeHandler.checkValidations(config.getContext().getCatalog(), config, path, sqlOptimize))
      .isInstanceOf(UserException.class)
      .hasMessage("OPTIMIZE TABLE command does not support tables with equality delete files.");
    table.close();
  }

  private static SqlOptimize parseToSqlOptimizeNode(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return (SqlOptimize) parser.parseStmt();
  }
}
