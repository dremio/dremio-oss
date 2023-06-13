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
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.calcite.logical.TableOptimizeCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Test OPTIMIZE query
 */
public class TestOptimize extends BaseTestQuery {

  private static IcebergTestTables.Table table;
  private static IcebergTestTables.Table tableWithDeletes;
  private static SqlConverter converter;
  private static SqlHandlerConfig config;

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


    config = new SqlHandlerConfig(queryContext, converter, observer, null);
  }

  @Before
  public void init() throws Exception {
    table = IcebergTestTables.V2_ORDERS.get();
    tableWithDeletes = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    table.enableIcebergSystemOptions();
    tableWithDeletes.enableIcebergSystemOptions();
  }

  @After
  public void tearDown() throws Exception {
    table.close();
    tableWithDeletes.close();
  }

  //===========================================================================
  // Test Cases
  //===========================================================================
  @Test
  public void testLogicalRelNodeConversion() throws Exception {
    String sql = format("OPTIMIZE TABLE %s", table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    assertThat(convertedRelNode.getValidatedRowType().getFieldCount()).isEqualTo(3);

    // find TableOptimizeRel
    assertThat(convertedRelNode.getConvertedNode() instanceof TableOptimizeCrel).as("TableOptimizeCrel node is expected").isTrue();
  }

  @Test
  public void testValidations() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);

    final String sql = "OPTIMIZE TABLE " + table.getTableName();
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlOptimize.class).getPath();
    OptimizeHandler optimizeHandler = new OptimizeHandler();

    //Disable SELECT Privilege
    Mockito.doThrow(UserException.permissionError()
      .message(String.format("User [%s] not authorized to %s [%s]", SYSTEM_USERNAME, SqlGrant.Privilege.SELECT, path))
      .buildSilently()).when(mockCatalog).validatePrivilege(path, SqlGrant.Privilege.SELECT);
    Mockito.doNothing().when(mockCatalog).validatePrivilege(path, SqlGrant.Privilege.UPDATE);
    assertThatThrownBy(() -> optimizeHandler.validatePrivileges(mockCatalog, path, node))
      .isInstanceOf(UserException.class).hasMessageContaining("not authorized to SELECT");

    //Disable UPDATE Privilege
    Mockito.doThrow(UserException.permissionError()
      .message(String.format("User [%s] not authorized to %s [%s]", SYSTEM_USERNAME, SqlGrant.Privilege.UPDATE, path))
      .buildSilently()).when(mockCatalog).validatePrivilege(path, SqlGrant.Privilege.UPDATE);
    Mockito.doNothing().when(mockCatalog).validatePrivilege(path, SqlGrant.Privilege.SELECT);
    assertThatThrownBy(() -> optimizeHandler.validatePrivileges(mockCatalog, path, node))
      .isInstanceOf(UserException.class).hasMessageContaining("not authorized to UPDATE");
  }

  @Test
  public void testOptimizePlan() throws Exception {
    final String sql = "OPTIMIZE TABLE " + table.getTableName();
    OptimizeHandler optimizeHandler = new OptimizeHandler();
    SqlNode sqlNode = converter.parse(sql);
    optimizeHandler.getPlan(config, sql, sqlNode);
    String textPlan = optimizeHandler.getTextPlan();

    //validate IcebergManifestListOperator Count
    assertThat(StringUtils.countMatches(textPlan, "IcebergManifestList")).as("Two IcebergManifestList operator is expected").isEqualTo(2);

    //validate TableFunctionDeletedFileMetadata Count
    assertThat(StringUtils.countMatches(textPlan, "Table Function Type=[DELETED_FILES_METADATA])")).as("Only one DELETED_DATA_FILES_METADATA Table Function operator is expected").isEqualTo(1);

    //validate TableFunctionSplitGenManifestScan Count
    assertThat(StringUtils.countMatches(textPlan, "Table Function Type=[SPLIT_GEN_MANIFEST_SCAN]")).as("Only one SPLIT_GEN_MANIFEST_SCAN Table Function operator is expected").isEqualTo(1);

    //validate ProjectWithIcebergMetadata
    assertThat(StringUtils.countMatches(textPlan, "RecordType(BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_C_O_U_N_T, VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, VARBINARY(65536) icebergMetadata)")).as("Only one such Project is expected").isEqualTo(1);

    //validate count aggregation on OperationType
    assertThat(textPlan).contains("Project(rewritten_data_files_count=[CASE(=($9, 1), $1, CAST(0:BIGINT):BIGINT)], rewritten_delete_files_count=[CASE(=($9, 3), $1, CAST(0:BIGINT):BIGINT)], new_data_files_count=[CASE(=($9, 0), $1, CAST(0:BIGINT):BIGINT)])");

    //validate OptimizeTableOperators
    testMatchingPatterns(textPlan, new String[] {
      // We should have all these operators
      "WriterCommitter",
      "UnionAll",
      "Writer",
      "TableFunction",
      "Project",
      "IcebergManifestList",
      "IcebergManifestScan","StreamAgg"});

    //validate OptimizeTableOperatorsOrder
    testMatchingPatterns(textPlan, new String[] {
      "(?s)" +
        "WriterCommitter.*" +
        "UnionAll.*" +
        "Writer.*" +
        "TableFunction.*" +
        "TableFunction.*" +
        "IcebergManifestList.*" +
        "Project.*" +
        "TableFunction.*" +
        "Project.*" + ColumnUtils.ROW_COUNT_COLUMN_NAME + ".*" + ColumnUtils.FILE_PATH_COLUMN_NAME + ".*" +
        "IcebergManifestScan.*" +
        "IcebergManifestList.*"});


  }

  @Test
  public void testV1OptimizePlan() throws Exception {
    IcebergTestTables.Table v1table = IcebergTestTables.NATION.get();
    test(String.format("CREATE TABLE %s.%s as select * from ", TEMP_SCHEMA_HADOOP, "v1table") + v1table.getTableName());
    final String sql = "OPTIMIZE TABLE " + TEMP_SCHEMA_HADOOP + ".v1table";
    OptimizeHandler optimizeHandler = new OptimizeHandler();
    SqlNode sqlNode = converter.parse(sql);
    optimizeHandler.getPlan(config, sql, sqlNode);
    String textPlan = optimizeHandler.getTextPlan();

    //validate IcebergManifestListOperator Count
    assertThat(StringUtils.countMatches(textPlan, "IcebergManifestList")).as("Two IcebergManifestList operator is expected").isEqualTo(2);

    //validate TableFunctionDeletedFileMetadata Count
    assertThat(StringUtils.countMatches(textPlan, "Table Function Type=[DELETED_FILES_METADATA])")).as("Only one DELETED_DATA_FILES_METADATA Table Function operator is expected").isEqualTo(1);

    //validate TableFunctionSplitGenManifestScan Count
    assertThat(StringUtils.countMatches(textPlan, "Table Function Type=[SPLIT_GEN_MANIFEST_SCAN]")).as("Only one SPLIT_GEN_MANIFEST_SCAN Table Function operator is expected").isEqualTo(1);

    //validate ProjectWithIcebergMetadata
    assertThat(StringUtils.countMatches(textPlan, "RecordType(BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_C_O_U_N_T, VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, VARBINARY(65536) icebergMetadata)")).as("Only one such Project is expected").isEqualTo(1);

    //validate count aggregation on OperationType
    assertThat(textPlan).contains("Project(rewritten_data_files_count=[CASE(=($9, 1), $1, CAST(0:BIGINT):BIGINT)], rewritten_delete_files_count=[CASE(=($9, 3), $1, CAST(0:BIGINT):BIGINT)], new_data_files_count=[CASE(=($9, 0), $1, CAST(0:BIGINT):BIGINT)])");

    //validate OptimizeTableOperators
    testMatchingPatterns(textPlan, new String[] {
      // We should have all these operators
      "WriterCommitter",
      "UnionAll",
      "Writer",
      "TableFunction",
      "Project",
      "IcebergManifestList",
      "IcebergManifestScan","StreamAgg"});

    //validate OptimizeTableOperatorsOrder
    testMatchingPatterns(textPlan, new String[] {
      "(?s)" +
        "WriterCommitter.*" +
        "UnionAll.*" +
        "Writer.*" +
        "TableFunction.*" +
        "TableFunction.*" +
        "IcebergManifestList.*" +
        "Project.*" +
        "TableFunction.*" +
        "Project.*" + ColumnUtils.ROW_COUNT_COLUMN_NAME + ".*" + ColumnUtils.FILE_PATH_COLUMN_NAME + ".*" +
        "IcebergManifestScan.*" +
        "IcebergManifestList.*"});


  }

  @Test
  public void testOptimizePlanWithDeletes() throws Exception {
    final String sql = "OPTIMIZE TABLE " + tableWithDeletes.getTableName();
    OptimizeHandler optimizeHandler = new OptimizeHandler();
    SqlNode sqlNode = converter.parse(sql);
    optimizeHandler.getPlan(config, sql, sqlNode);
    String textPlan = optimizeHandler.getTextPlan();

    //validate IcebergManifestListOperator Count
    assertThat(StringUtils.countMatches(textPlan, "IcebergManifestList")).as("Six IcebergManifestList operators are expected").isEqualTo(6);

    //validate TableFunctionDeletedFileMetadata Count
    assertThat(StringUtils.countMatches(textPlan, "Table Function Type=[DELETED_FILES_METADATA])")).as("Two DELETED_FILES_METADATA Table Function operators are expected").isEqualTo(2);

    //validate ProjectWithIcebergMetadata
    assertThat(StringUtils.countMatches(textPlan, "RecordType(BIGINT D_R_E_M_I_O_D_A_T_A_F_I_L_E_R_O_W_C_O_U_N_T, VARCHAR(65536) D_R_E_M_I_O_D_A_T_A_F_I_L_E_F_I_L_E_P_A_T_H, VARBINARY(65536) icebergMetadata)")).as("Two such Projects are expected").isEqualTo(2);

    //validate count aggregation on OperationType
    assertThat(textPlan).contains("Project(rewritten_data_files_count=[CASE(=($9, 1), $1, CAST(0:BIGINT):BIGINT)], rewritten_delete_files_count=[CASE(=($9, 3), $1, CAST(0:BIGINT):BIGINT)], new_data_files_count=[CASE(=($9, 0), $1, CAST(0:BIGINT):BIGINT)])");

    //validate OptimizeTableOperators
    testMatchingPatterns(textPlan, new String[] {
      // We should have all these operators
      "WriterCommitter",
      "UnionAll",
      "Writer",
      "TableFunction",
      "Project",
      "IcebergManifestList",
      "IcebergManifestScan","StreamAgg"});

    //validate OptimizeTableOperatorsOrder
    testMatchingPatterns(textPlan, new String[] {
      "(?s)" +
        "WriterCommitter.*" +
        "UnionAll.*" +
        "Writer.*" +
        "TableFunction.*" +
        "TableFunction.*" +
        "HashJoin.*" +
        "IcebergManifestList.*" +
        "Project.*" +
        "UnionAll.*" +
        "TableFunction.*" +
        "Project.*" + ColumnUtils.ROW_COUNT_COLUMN_NAME + ".*" + ColumnUtils.FILE_PATH_COLUMN_NAME + ".*" +
        "Filter.*" +
        "HashJoin.*" +
        "IcebergManifestList.*" +
        "HashAgg.*" +
        "TableFunction.*" +
        "IcebergManifestList.*" +
        "TableFunction.*" +
        "Project.*" + ColumnUtils.ROW_COUNT_COLUMN_NAME + ".*" + ColumnUtils.FILE_PATH_COLUMN_NAME + ".*" +
        "IcebergManifestScan.*" +
        "IcebergManifestList.*"});
  }

  private void testMatchingPatterns(String plan, String[] expectedPatterns) {
    // Check and make sure all expected patterns are in the plan
    if (expectedPatterns != null) {
      for (final String s : expectedPatterns) {
        final Pattern p = Pattern.compile(s);
        final Matcher m = p.matcher(plan);
        assertTrue("Did not find expected pattern in plan" + s + ". Plan was:\n" + plan, m.find());
      }
    }
  }

}
