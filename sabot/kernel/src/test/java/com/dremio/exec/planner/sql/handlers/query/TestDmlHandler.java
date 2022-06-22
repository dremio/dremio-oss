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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_ADVANCED_DML;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;

public class TestDmlHandler extends BaseTestQuery {

  private static final String TABLE_NAME = "test_table";
  private static final String TABLE_PATH = TEMP_SCHEMA_HADOOP + "." + TABLE_NAME;
  private static final String ID_COLUMN_NAME = "id";
  private static final String DATA_COLUMN_NAME = "data";

  private static final String CREATE_BASIC_TABLE_SQL = String.format("CREATE TABLE %s (%s BIGINT, %s VARCHAR) STORE AS (type => 'iceberg')", TABLE_PATH, ID_COLUMN_NAME, DATA_COLUMN_NAME);

  private static SqlConverter converter;
  private static SqlHandlerConfig config;

  @BeforeClass
  public static void beforeClass() throws Exception {
    SabotContext context = getSabotContext();

    UserSession session = UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.ANONYMOUS).build())
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

    try (AutoCloseable ignored = enableIcebergTables()) {
      test(CREATE_BASIC_TABLE_SQL);
    }
  }

  @Test
  public void testExtendedDelete() throws Exception {
    try (AutoCloseable ignored = enableIcebergTables()) {
      testExtendedTable(String.format("DELETE FROM %s", TABLE_PATH));
    }
  }

  @Test
  public void testExtendedUpdate() throws Exception {
    try (AutoCloseable ignored = enableIcebergTables()) {
      testExtendedTable(String.format("UPDATE %s SET id = 0", TABLE_PATH));
    }
  }

  @Test
  public void testExtendedMerge() throws Exception {
    try (AutoCloseable ignored = enableIcebergTables()) {
      testExtendedTable(
        String.format("MERGE INTO %s USING (SELECT * FROM %s) s ON (%s.id = s.id) WHEN MATCHED THEN UPDATE SET id = 0",
        TABLE_PATH, TABLE_PATH, TABLE_PATH));
    }
  }

  @Test
  public void testDeleteWithoutIcebergEnabled() throws Exception {
    testDmlWithoutIcebergEnabled(
      DeleteHandler.class.getDeclaredConstructor(),
      SqlKind.DELETE,
      String.format("DELETE FROM %s", TABLE_PATH));
  }

  @Test
  public void testUpdateWithoutIcebergEnabled() throws Exception {
    testDmlWithoutIcebergEnabled(
      UpdateHandler.class.getDeclaredConstructor(),
      SqlKind.UPDATE,
      String.format("UPDATE %s SET id = 0", TABLE_PATH));
  }

  @Test
  public void testMergeWithoutIcebergEnabled() throws Exception {
    testDmlWithoutIcebergEnabled(
      MergeHandler.class.getDeclaredConstructor(),
      SqlKind.MERGE,
      String.format("MERGE INTO %s USING (SELECT * FROM %s) s ON (%s.id = s.id) WHEN MATCHED THEN UPDATE SET id = 0",
        TABLE_PATH, TABLE_PATH, TABLE_PATH));
  }

  @Test
  public void testDmlPlanCleaner() throws Exception {
    final String query = String.format("DELETE FROM %s", TABLE_PATH);
    final SqlNode node = converter.parse(query);
    PhysicalPlan plan = DeleteHandler.class.getDeclaredConstructor().newInstance().getPlan(config, query, node);
    assertNotNull(plan.getCleaner());
  }

  @Test
  public void testDatasetRefreshUsesForceUpdate() {
    DatasetCatalog catalog = mock(DatasetCatalog.class);
    when(catalog.refreshDataset(any(), any(), anyBoolean())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);
    NamespaceKey key = new NamespaceKey(ImmutableList.of("my", "table"));

    DataAdditionCmdHandler.refreshDataset(catalog, key, false);

    ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    verify(catalog, times(1)).refreshDataset(eq(key), optionsCaptor.capture(), eq(false));
    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertThat(options.forceUpdate()).isTrue();

    DataAdditionCmdHandler.refreshDataset(catalog, key, true);

    verify(catalog, times(2)).refreshDataset(eq(key), optionsCaptor.capture(), eq(false));
    options = optionsCaptor.getValue();
    assertThat(options.forceUpdate()).isTrue();
  }

  private <T extends DmlHandler> void testDmlWithoutIcebergEnabled(Constructor<T> constructor, SqlKind sqlKind, String query) throws Exception {
    try (AutoCloseable ignored = disableIcebergAdvancedDmlSupportFlag()) {
      final SqlNode node = converter.parse(query);
      UserExceptionAssert.assertThatThrownBy(() -> constructor.newInstance().getPlan(config, query, node))
        .hasMessageContaining(String.format("%s clause is not supported in the query for this source", sqlKind));
    }
  }

  private void testExtendedTable(String query) throws Exception {
    final SqlNode node = converter.parse(query);
    SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) node;
    sqlDmlOperator.extendTableWithDataFileSystemColumns();
    final ConvertedRelNode convertedRelDeleteNode = PrelTransformer.validateAndConvert(config, node);
    Assert.assertEquals(4, convertedRelDeleteNode.getValidatedRowType().getFieldCount());
    List<RelDataTypeField> fields = convertedRelDeleteNode.getValidatedRowType().getFieldList();
    verifyNameAndType(fields.get(0), ID_COLUMN_NAME, SqlTypeName.BIGINT);
    verifyNameAndType(fields.get(1), DATA_COLUMN_NAME, SqlTypeName.VARCHAR);
    verifyNameAndType(fields.get(2), ColumnUtils.FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR);
    verifyNameAndType(fields.get(3), ColumnUtils.ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT);
  }

  private void verifyNameAndType(RelDataTypeField field, String name, SqlTypeName type) {
    Assert.assertEquals(name, field.getName());
    Assert.assertEquals(type, field.getType().getSqlTypeName());
  }

  private static AutoCloseable disableIcebergAdvancedDmlSupportFlag() {
    config.getContext().getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      ENABLE_ICEBERG_ADVANCED_DML.getOptionName(),
      false));

    return () -> {
      config.getContext().getOptions().setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM,
        ENABLE_ICEBERG_ADVANCED_DML.getOptionName(),
        ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal()));
    };
  }
}
