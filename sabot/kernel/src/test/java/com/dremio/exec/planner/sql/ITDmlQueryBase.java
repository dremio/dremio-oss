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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import com.dremio.BaseTestQuery;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Enables ENABLE_ICEBERG_ADVANCED_DML for a local filesystem-based Hadoop source.
 */
public class ITDmlQueryBase extends BaseTestQuery {
  protected ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
  protected static SqlConverter converter;
  protected static SqlDialect DREMIO_DIALECT =
    new SqlDialect(SqlDialect.DatabaseProduct.UNKNOWN, "Dremio", Character.toString(SqlUtils.QUOTE), NullCollation.FIRST);

  //===========================================================================
  // Test class and Test cases setUp and tearDown
  //===========================================================================
  @BeforeClass
  public static void setUp() throws Exception {
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
  }

  @BeforeClass
  public static void beforeClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR, "true");
    setSystemOption(ExecConstants.ENABLE_COPY_INTO, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM, "true");
  }

  @AfterClass
  public static void afterClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_COPY_INTO,
      ExecConstants.ENABLE_COPY_INTO.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM,
      ExecConstants.ENABLE_ICEBERG_VACUUM.getDefault().getBoolVal().toString());
  }

  @Before
  public void before() throws Exception {
    // Note: dfs_hadoop is immutable.
    test("USE dfs_hadoop");
  }

  protected static void parseAndValidateSqlNode(String query, String expected) throws Exception {
    if (converter == null) {
      setUp();
    }
    final SqlNode node = converter.parse(query);
    SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    node.unparse(writer, 0, 0);
    String actual = writer.toString();
    Assert.assertEquals(expected.toLowerCase(), actual.toLowerCase());
  }
}
