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

import static com.dremio.PlanTestBase.testMatchingPatterns;
import static com.dremio.exec.ExecConstants.CTAS_CAN_USE_ICEBERG;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_ADVANCED_DML;
import static com.dremio.exec.planner.common.TestPlanHelper.findFirstNode;
import static com.dremio.exec.planner.common.TestPlanHelper.findNodes;
import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.TestPlanHelper.NodeFinder;
import com.dremio.exec.planner.common.TestPlanHelper.TargetNodeDescriptor;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DmlPlanGenerator.MergeType;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.direct.TruncateTableHandler;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.handlers.query.DeleteHandler;
import com.dremio.exec.planner.sql.handlers.query.DmlHandler;
import com.dremio.exec.planner.sql.handlers.query.InsertTableHandler;
import com.dremio.exec.planner.sql.handlers.query.MergeHandler;
import com.dremio.exec.planner.sql.handlers.query.TableManagementHandler;
import com.dremio.exec.planner.sql.handlers.query.UpdateHandler;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.planner.sql.parser.SqlMergeIntoTable;
import com.dremio.exec.planner.sql.parser.SqlTruncateTable;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.sys.SystemStoragePlugin;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests Advanced DML - DELETE, UPDATE, and MERGE
 */
public class TestDml extends TestTableManagementBase {

  //===========================================================================
  // Test Cases
  //===========================================================================
  @Test
  public void testExtendedDelete() throws Exception {
    testExtendedTable(String.format("DELETE FROM %s", table.getTableName()));
  }

  @Test
  public void testExtendedUpdate() throws Exception {
    String testColumn = userColumnList.get(0).getName();
    testExtendedTable(String.format("UPDATE %s SET %s = 0", table.getTableName(), testColumn));
  }

  @Test
  public void testExtendedMerge() throws Exception {
    String testColumn = userColumnList.get(0).getName();
    testExtendedTable(
            String.format("MERGE INTO %s USING (SELECT * FROM %s) AS s ON (%s = %s) WHEN MATCHED THEN UPDATE SET %s = 0",
                    table.getTableName(),
                    table.getTableName(),
                    table.getTableName() + '.' + testColumn,
                    "s." + testColumn,
                    testColumn));
  }

  @Test
  public void testDmlPlanCleaner() throws Exception {
    final String query = String.format("DELETE FROM %s", table.getTableName());
    final SqlNode node = converter.parse(query);
    PhysicalPlan plan = DeleteHandler.class.getDeclaredConstructor().newInstance().getPlan(config, query, node);
    assertThat(plan.getCleaner()).isNotNull();
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
    assertThat(options.forceUpdate()).as("Should be a force update").isTrue();

    DataAdditionCmdHandler.refreshDataset(catalog, key, true);

    verify(catalog, times(2)).refreshDataset(eq(key), optionsCaptor.capture(), eq(false));
    options = optionsCaptor.getValue();
    assertThat(options.forceUpdate()).as("Should be a force update").isTrue();
  }

  @Test
  public void testDeleteFailedValidations() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);

    String sql = "Delete from " + table.getTableName() + " where order_id > 10";
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlDeleteFromTable.class).getPath();
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(path);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.FALSE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, catalogEntityKey, SqlDeleteFromTable.OPERATOR))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.FALSE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, catalogEntityKey, SqlDeleteFromTable.OPERATOR))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.FALSE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, catalogEntityKey, SqlDeleteFromTable.OPERATOR))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, catalogEntityKey, SqlDeleteFromTable.OPERATOR))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testTruncateTableFailedValidation() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);
    UserSession mockUserSession = Mockito.mock(UserSession.class);

    String sql = "TRUNCATE " + table.getTableName();
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlTruncateTable.class).getPath();

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getCatalog()).thenReturn(mockCatalog);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);
    when(mockQueryContext.getSession()).thenReturn(mockUserSession);
    when(mockUserSession.getSessionVersionForSource(path.getRoot())).thenReturn(VersionContext.NOT_SPECIFIED);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> new TruncateTableHandler(mockConfig).toResult(sql, node))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testInsertFailedValidation() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);
    String sql = "INSERT INTO " + table.getTableName() + " SELECT * FROM " + table.getTableName() + " where order_id > 10";
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlInsertTable.class).getPath();
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(path);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> InsertTableHandler.validateDmlRequest(mockCatalog, mockConfig, catalogEntityKey, node))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterRewriteSingleScanCrel() throws Exception {
    String sql = format("DELETE FROM %s", table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvertForDml(config, node, null);
    assertThat(convertedRelNode.getValidatedRowType().getFieldCount()).isEqualTo(userColumnCount);

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    ScanCrel scanCrel = findSingleNode(tableModifyCrel, ScanCrel.class, null);
    assertThat(scanCrel.isSubstitutable()).as("scanCrel should be substitutable before rewrite").isTrue();

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = TableManagementHandler.ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrel = findSingleNode(rewrite, ScanCrel.class, null);
    assertThat(scanCrel.isSubstitutable()).as("scanCrel should not be substitutable after rewrite").isFalse();
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterRewriteMultipleScanCrels() throws Exception {
    String sql = format("merge into %s using %s as s on s.order_id = %s.order_id when matched then update set order_id = -1 ",
      table.getTableName(), table.getTableName(), table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvertForDml(config, node, null);
    assertThat(convertedRelNode.getValidatedRowType().getFieldCount()).isEqualTo(userColumnCount);

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    List<ScanCrel> scanCrels = findNodes(tableModifyCrel, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel -> assertThat(scanCrel.isSubstitutable()).as("scanCrel should be substitutable before rewrite").isTrue());

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = TableManagementHandler.ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrels = findNodes(rewrite, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel -> assertThat(scanCrel.isSubstitutable()).as("scanCrel should not be substitutable after rewrite").isFalse());
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterMixedScanCrels() throws Exception {
    IcebergTestTables.Table nation = IcebergTestTables.NATION.get();
    nation.enableIcebergSystemOptions();

    String sql = format("merge into %s using (select * from %s) as s on s.n_nationkey = %s.order_id when matched then update set order_id = -1 ",
      table.getTableName(), nation.getTableName(), table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvertForDml(config, node, null);
    assertThat(convertedRelNode.getValidatedRowType().getFieldCount()).isEqualTo(userColumnCount);

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    List<ScanCrel> scanCrels = findNodes(tableModifyCrel, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel -> assertThat(scanCrel.isSubstitutable()).as("scanCrel should be substitutable before rewrite").isTrue());

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = TableManagementHandler.ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrels = findNodes(rewrite, ScanCrel.class, null);
    scanCrels.stream().forEach(
      scanCrel ->  {
        if (scanCrel.getTableMetadata().getName().toString().equals(table.getTableName())) {
          assertThat(scanCrel.isSubstitutable()).as("Target scanCrel should not be substitutable after rewrite").isFalse();
        } else {
          assertThat(scanCrel.isSubstitutable()).as("Source scanCrel (different from target scanCrel) should still be substitutable after rewrite").isTrue();
        }
      });
    nation.close();
  }

  @Test
  public void testInsertIntoGeneratingLogicalValuesForCast() throws Exception {
    final String insert = "INSERT INTO " + table.getTableName() + " VALUES " +
      "(1, 37, '1996-01-02', 'foo', CAST('131251.81' AS DOUBLE)), " +
      "(5, 46, '1994-07-30', 'bar', CAST('86615.25' AS DOUBLE))"  ;

    String plan = getInsertPlan(insert);
    assertThat(StringUtils.countMatches(plan, "Values(Values=")).as("Only one VALUES operator is expected").isEqualTo(1);
  }

  @Test
  public void testInsertIntoGeneratingUnionForSubquery() throws Exception {
    final String insert = "INSERT INTO " + table.getTableName() + " VALUES " +
      "(1, 37, '1996-01-02', 'foo', CAST('131251.81' AS DOUBLE)), " +
      "(5, 46, '1994-07-30', 'bar', (select amount from " +   table.getTableName() + " s where order_id = s.order_id))";

    String plan = getInsertPlan(insert);
    assertThat(StringUtils.countMatches(plan, "UnionAll(all=[true])")).as("Only one UnionAll operator is expected").isEqualTo(1);
  }

  @Test
  public void testDelete() throws Exception {
    final String delete = "Delete from " + table.getTableName() + " where order_id > 10";

    Prel plan = getDmlPlan(delete);

    validateCommonCopyOnWritePlan(plan, JoinRelType.LEFT, MergeType.INVALID);

    // validate filter on top of copy-on-write join
    validateDeleteSpecificOperationsAfterCopyOnWriteJoin(plan, true);

    // Verify column name
    testResultColumnName(delete);
  }

  @Test
  public void testDeleteAll() throws Exception {
    final String delete = "Delete from " + table.getTableName();

    Prel plan = getDmlPlan(delete);

    validateCommonCopyOnWritePlan(plan, JoinRelType.LEFT, MergeType.INVALID);

    // validate filter on top of copy-on-write join
    validateDeleteSpecificOperationsAfterCopyOnWriteJoin(plan, true);

    // Verify column name
    testResultColumnName(delete);
  }

  @Test
  public void testUpdate() throws Exception {
    final String update = "Update " + table.getTableName() + " set order_id = -1";

    Prel plan = getDmlPlan(update);

    // validate basic copy-on-write plan
    validateCommonCopyOnWritePlan(plan, JoinRelType.LEFT, MergeType.INVALID);

    // validate no delete filter
    validateDeleteSpecificOperationsAfterCopyOnWriteJoin(plan, false);

    // Verify column name
    testResultColumnName(update);
  }

  @Test
  public void testMerge_updateOnly() throws Exception {
    final String mergeUpdateOnly = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when matched then update set order_id = -1";

    testMerge(mergeUpdateOnly, 1, MergeType.UPDATE_ONLY);
  }

  @Test
  public void testMerge_insertOnly() throws Exception {
    final String query = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when not matched then INSERT(order_id) VALUES(-1)";

    testMerge(query, 0, MergeType.INSERT_ONLY);
  }

  @Test
  public void testMerge_updateWithInsert() throws Exception {
    final String mergeUpdateWithInsert = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when matched then update set order_id = -1 WHEN NOT MATCHED THEN INSERT(order_id) VALUES(-3) ";

    testMerge(mergeUpdateWithInsert, 1, MergeType.UPDATE_INSERT);
  }

  private void testDmlDistributionForWrites(PartitionDistributionStrategy distributionStrategy) throws Exception {
    setPartitionDistributionStrategy(config, distributionStrategy.toString());

    final String tableName = "supplier";
    try {
      // create a decent size partitioned table so that partition distribution can be applied
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/supplier";

      final String ctasQuery = String.format("CREATE TABLE %s.%s partition by (S_NATIONKEY) " +
        " AS SELECT * from " + "dfs_hadoop" + ".\"" + parquetFiles + "\"", TEMP_SCHEMA_HADOOP, tableName);
      test(ctasQuery);

      final String delete = String.format("Delete from %s.%s", TEMP_SCHEMA_HADOOP, tableName);
      SqlNode deleteNode = converter.parse(delete);
      DmlHandler dmlHandler = getDmlHandler(deleteNode);
      dmlHandler.getPlan(config, null, deleteNode, CatalogUtil.getResolvePathForTableManagement(config.getContext().getCatalog(), dmlHandler.getTargetTablePath(deleteNode)));
      String expectedExchange = distributionStrategy == PartitionDistributionStrategy.HASH ? "HashToRandomExchange" : "RoundRobinExchange";
      testMatchingPatterns(dmlHandler.getTextPlan(), new String[]{
        // We should have all these operators
        "WriterCommitter",
        "UnionExchange",
        "Writer",
        "Project",
        "UnionExchange",
        "Writer",
        expectedExchange,
        "HashJoin",
        "IcebergManifestList",

        // The operators should be in this order
        "(?s)" +
          "WriterCommitter.*" +
          "UnionAll.*" +
          "Project.*" +
          "UnionExchange.*" +
          "Writer.*" +
          expectedExchange + ".*" +
          "HashJoin.*" +
          "IcebergManifestList.*"});
    } finally {
      test(String.format("drop table if exists %s.%s", TEMP_SCHEMA_HADOOP, tableName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      setPartitionDistributionStrategy(config, ExecConstants.WRITER_PARTITION_DISTRIBUTION_MODE.getDefault().getStringVal());
    }
  }

  @Test
  public void testSingletonIcebergManifestWriterPrel() throws Exception {
    config.getContext().getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      "dremio.iceberg.single_manifest_writer.enabled",true));

    final String delete = "Delete from " + table.getTableName() + " where order_id > 10";
    Prel plan = getDmlPlan(delete);
    IcebergManifestWriterPrel manifestWriterPrel = findSingleNode(plan, IcebergManifestWriterPrel.class, null);
    assertTrue(manifestWriterPrel.getTraitSet().contains(DistributionTrait.SINGLETON));

    config.getContext().getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      "dremio.iceberg.single_manifest_writer.enabled",
      ExecConstants.ENABLE_ICEBERG_SINGLE_MANIFEST_WRITER.getDefault().getBoolVal()));
  }

  @Test
  public void testDmlUseHashDistributionForWrites() throws Exception {
    testDmlDistributionForWrites(PartitionDistributionStrategy.HASH);
  }

  @Test
  public void testDmlUseRoundRobinDistributionForWrites() throws Exception {
    testDmlDistributionForWrites(PartitionDistributionStrategy.ROUND_ROBIN);
  }

  //===========================================================================
  // Private Test Helper
  //===========================================================================
  private static DmlHandler getDmlHandler(SqlNode node) {
    if (node instanceof SqlDeleteFromTable) {
      return new DeleteHandler();
    } else if (node instanceof SqlUpdateTable) {
      return new UpdateHandler();
    } else if (node instanceof SqlMergeIntoTable) {
      return new MergeHandler();
    }
    fail("This should never happen - Only Delete/Update/Merge should appear here");

    return null;
  }

  private static void setPartitionDistributionStrategy(SqlHandlerConfig configuration, String strategy) {
    configuration.getContext().getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      "store.writer.partition_distribution_mode",
      strategy));
  }

  private void testResultColumnName(String query) throws Exception {
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

    assertThat(SqlToRelTransformer.validateAndConvertForDml(config, converter.parse(query), null)
            .getConvertedNode().getRowType().getFieldNames().get(0)).isEqualTo(DmlUtils.DML_OUTPUT_COLUMN_NAMES.get(operation));
  }

  private String getInsertPlan(String sql) throws Exception {
    InsertTableHandler insertHandler = new InsertTableHandler();
    SqlNode sqlNode = converter.parse(sql);
    insertHandler.getPlan(config, sql, sqlNode);
    return insertHandler.getTextPlan();
  }

  public static Prel getDmlPlan(SqlHandlerConfig config, SqlNode sqlNode) throws Exception {
    DmlHandler dmlHandler = getDmlHandler(sqlNode);
    dmlHandler.getPlan(config, null, sqlNode, CatalogUtil.getResolvePathForTableManagement(config.getContext().getCatalog(), dmlHandler.getTargetTablePath(sqlNode)));
    return dmlHandler.getPrel();
  }

  private Prel getDmlPlan(String sql) throws Exception {
    return getDmlPlan(config, converter.parse(sql));
  }

  // get CopyOnWriteJoin condition, left.file_path = right.file_path and left.row_index = right.row_index
  private static String getCopyOnWriteJoinCondition(MergeType mergeType) {
    return String.format("AND(=($%d, $%d), =($%d, $%d))",
      userColumnCount,  (mergeType == MergeType.UPDATE_INSERT ? userColumnCount : 0) + userColumnCount + 2, // file_path column
      userColumnCount + 1, (mergeType == MergeType.UPDATE_INSERT ? userColumnCount : 0) + userColumnCount + 3); // row_index column
  }

  private List<FilterPrel> findDeletePlanFilter(Prel joinPrel) {
    // Delete query plan has a filter on top of anti-join
    // filter = row_index column is null
    Map<String, String> attributes = ImmutableMap.of(
      "condition", String.format("IS NULL($%d)",  userColumnCount + 3) // row_index is null
    );

    TargetNodeDescriptor descriptor =
      new TargetNodeDescriptor(FilterPrel.class, attributes);
    return NodeFinder.find(joinPrel, descriptor);
  }

  private void validateDeleteSpecificOperationsAfterCopyOnWriteJoin(Prel plan, Boolean expectFilter) {
    List<FilterPrel> filterPrels = findDeletePlanFilter(plan);

    if (expectFilter) {
      assertThat(filterPrels).as("filter is expected").isNotNull();
      assertThat(filterPrels.size()).as("filter is expected").isEqualTo(1);

      FilterPrel filterPrel = filterPrels.get(0);
      assertThat(filterPrel).as("filter is expected").isNotNull();
    } else {
      assertThat(filterPrels.size()).as("no filter is expected").isEqualTo(0);
    }
  }

  private static void validateCommonCopyOnWritePlan(Prel plan, JoinRelType joinType, MergeType mergeType) {
    Prel writerPlan = validateRowCountPlan(plan);

    validateWriterPlan(writerPlan, joinType, mergeType);
  }

  private static int findRecordWriterFieldIndex(String fieldName) {
    Integer recordFieldIndex = null;
    for (int i = 0; i < RecordWriter.SCHEMA.getFields().size(); i++) {
      if (RecordWriter.SCHEMA.getColumn(i).getName().equals(fieldName)) {
        recordFieldIndex = i;
        break;
      }
    }
    assertThat(recordFieldIndex).as("did not find record field").isNotNull();
    return recordFieldIndex;
  }

  private static Prel validateRowCountTopProject(Prel plan) {
    Map<String, String> attributes = ImmutableMap.of(
      "exps", "[CASE(IS NULL($0), 0, $0)]",
      "rowType", String.format("RecordType(BIGINT %s)",  RecordWriter.RECORDS.getName())
    );
    return findSingleNode(plan, ProjectPrel.class, attributes);
  }

  // validate row count aggregate
  private static Prel validateRowCountAgg(RelNode plan) {
    int recordWriterFieldIndex = findRecordWriterFieldIndex(RecordWriter.RECORDS.getName());
    Map<String, String> attributes = ImmutableMap.of(
      "aggCalls", String.format("[SUM($%d)]", recordWriterFieldIndex), // count on row count column
      "rowType", String.format("RecordType(BIGINT %s)",  RecordWriter.RECORDS.getName())
    );

    return findSingleNode(plan, StreamAggPrel.class, attributes);
  }

  // validate row count filter
  private static Prel validateRowCountFilter(RelNode plan) {
    // filter: RecordWriter.OPERATION_TYPE = OperationType.DELETE_DATAFILE
    Map<String, String> attributes = ImmutableMap.of(
      "condition", String.format("=($%d, 1)", findRecordWriterFieldIndex(RecordWriter.OPERATION_TYPE.getName()))
    );

    return findSingleNode(plan, FilterPrel.class, attributes);
  }

  private static Prel validateRowCountPlan(Prel plan) {
    return validateRowCountFilter(
      validateRowCountAgg(
        validateRowCountTopProject(plan)
      )
    );
  }

  private static void validateWriterPlan(Prel plan, JoinRelType joinType, MergeType mergeType) {
    WriterCommitterPrel writerCommitterPrel = findSingleNode(plan, WriterCommitterPrel.class, null);

    RelNode union= writerCommitterPrel.getInput();

    assertThat(union).isInstanceOf(UnionAllPrel.class);
    UnionAllPrel unionAllPrel = (UnionAllPrel)union;

    assertThat(2).as("2 inputs are expected for union prel").isEqualTo(unionAllPrel.getInputs().size());

    TargetNodeDescriptor writerDescriptor =  new TargetNodeDescriptor(WriterPrel.class, null);
    List<TableFunctionPrel> writerPrels= NodeFinder.find(unionAllPrel.getInput(0), writerDescriptor);
    boolean writerIsOnZeroUnionInput = CollectionUtils.isNotEmpty(writerPrels);

    validateDeletedFilesTableFunction(unionAllPrel.getInput(writerIsOnZeroUnionInput ? 1 : 0), mergeType);

    validateBaseCopyOnWriteJoinPlan(unionAllPrel.getInput(writerIsOnZeroUnionInput ? 0 : 1), joinType, mergeType);
  }

  private static void validateDeletedFilesTableFunction(RelNode plan, MergeType mergeType) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(TableFunctionPrel.class, null);
    List<TableFunctionPrel> tableFunctionPrels= NodeFinder.find(plan, descriptor);
    assertThat(tableFunctionPrels).isNotNull();

    TableFunctionPrel deletedFilesTableFunctionPre = tableFunctionPrels.get(0);
    assertThat(TableFunctionConfig.FunctionType.DELETED_FILES_METADATA).isEqualTo(deletedFilesTableFunctionPre.getTableFunctionConfig().getType());

    validateDmlAgg(deletedFilesTableFunctionPre, mergeType);
  }

  private static void validateBaseCopyOnWriteJoinPlan(RelNode plan, JoinRelType joinType, MergeType mergeType) {
    Map<String, String> attributes = ImmutableMap.of(
      "joinType", joinType.toString(),
      "condition", getCopyOnWriteJoinCondition(mergeType)
    );

    if (mergeType == MergeType.INSERT_ONLY) {
      TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(HashJoinPrel.class, attributes);
      List<HashJoinPrel> hashJoinPrels = NodeFinder.find(plan, descriptor);
      assertThat(hashJoinPrels.size()).as("no copy-on-write join is expected to find").isEqualTo(0);
      return;
    }

    HashJoinPrel hashJoinPrel = findSingleNode(plan, HashJoinPrel.class, attributes);

    validateFilePathJoin(hashJoinPrel.getLeft(), mergeType);
  }

  private static void validateFilePathJoin(RelNode plan, MergeType mergeType) {
    Map<String, String> attributes = ImmutableMap.of(
      "joinType", "inner",
      "condition", "=($0, $8)"
    );

    HashJoinPrel hashJoinPrel = findSingleNode(plan, HashJoinPrel.class, attributes);

    validateDmlAgg(hashJoinPrel.getRight(), mergeType);
  }

  // validate aggregate on file path from Dmled result
  private static Prel validateDmlAgg(RelNode plan, MergeType mergeType) {
    Map<String, String> attributes = ImmutableMap.of(
      "groupSet", String.format("{%d}",
        mergeType == MergeType.INSERT_ONLY ||  mergeType == MergeType.UPDATE_INSERT ? userColumnCount : 0), // file_path, column 0 for Deleted/Updated data
      "aggCalls", "[COUNT()]", // count
      "rowType", String.format("RecordType(VARCHAR(65536) %s, BIGINT %s)", ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_COUNT_COLUMN_NAME)
    );

    Prel  aggPrel = findFirstNode(plan, HashAggPrel.class, attributes, false);
    RelNode dataFileScanSubTree = aggPrel;
    if (mergeType == MergeType.INSERT_ONLY) {
      attributes = ImmutableMap.of("condition", String.format("IS NULL($%d)",userColumnCount));
      Prel filterPrel = findFirstNode(aggPrel, FilterPrel.class, attributes, false);
      dataFileScanSubTree = filterPrel.getInput(0);
    }

    validateDataFileScanColumns(dataFileScanSubTree);

    return aggPrel;
  }

  private static Set<String> extractColumnsFromRowType(RelDataType rowType) {
    return rowType.getFieldList().stream()
      .map(f -> f.getName())
      .collect(Collectors.toSet());
  }

  private static void validateDataFileScanColumns(RelNode plan) {

    Set<String> expectedUsedFields = Stream.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME)
          .collect(Collectors.toCollection(HashSet::new));

    // if there are other operators between Agg and data scan TF, add their output fields
    // Those operators could be Filter, Project, Join etc.
    //  This is not a correct way to extract used columns from operators, but it is good enough for this test
    List<FilterPrel> filters = NodeFinder.find(plan, new TargetNodeDescriptor(FilterPrel.class, ImmutableMap.of()));
    if (filters.size() > 0) {
      assertThat(filters.size()).as("expect at most 1 filter").isEqualTo(1);
      expectedUsedFields.addAll(extractColumnsFromRowType(filters.get(0).getRowType()));
    }

    List<HashJoinPrel> joins = NodeFinder.find(plan, new TargetNodeDescriptor(HashJoinPrel.class, ImmutableMap.of()));
    if (joins.size() > 0) {
      assertThat(joins.size()).as("expect at most 1 join").isEqualTo(1);
      // left side of the join contains the columns from original table
      expectedUsedFields.addAll(extractColumnsFromRowType(joins.get(0).getLeft().getRowType()));
    }

    TableFunctionPrel dataFileScanTF = findFirstNode(plan, TableFunctionPrel.class, null, false);
    Set<String> actualUsedFields = extractColumnsFromRowType(dataFileScanTF.getRowType());
    assertThat(actualUsedFields).isEqualTo(expectedUsedFields);
  }

  private void testExtendedTable(String query) throws Exception {
    final SqlNode node = converter.parse(query);
    SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) node;
    sqlDmlOperator.extendTableWithDataFileSystemColumns();
    final ConvertedRelNode convertedRelDeleteNode = SqlToRelTransformer.validateAndConvertForDml(config, node, null);
    List<RelDataTypeField> fields = convertedRelDeleteNode.getValidatedRowType().getFieldList();
    int totalColumnCount = convertedRelDeleteNode.getValidatedRowType().getFieldCount();
    assertThat(totalColumnCount).isEqualTo(userColumnCount + 2);
    // Verify that the last two columns are extended columns
    verifyNameAndType(fields.get(totalColumnCount - 2), ColumnUtils.FILE_PATH_COLUMN_NAME, SqlTypeName.VARCHAR);
    verifyNameAndType(fields.get(totalColumnCount - 1), ColumnUtils.ROW_INDEX_COLUMN_NAME, SqlTypeName.BIGINT);
  }

  private void verifyNameAndType(RelDataTypeField field, String name, SqlTypeName type) {
    assertThat(field.getName()).isEqualTo(name);
    assertThat(field.getType().getSqlTypeName()).isEqualTo(type);
  }

  private void testMerge(String mergeQuery, int updatedColumns, MergeType mergeType) throws Exception {
    Prel plan = getDmlPlan(mergeQuery);

    // validate basic copy-on-write plan
    validateCommonCopyOnWritePlan(plan, JoinRelType.FULL, mergeType);

    // validate no delete filter
    validateDeleteSpecificOperationsAfterCopyOnWriteJoin(plan, false);

    // merge specific
    Prel agg = validateDmlAgg(plan, mergeType);

    RelNode aggInput = agg.getInput(0);

    switch (mergeType) {
      case INSERT_ONLY:
        assertThat(aggInput.getRowType().getFieldCount()).isEqualTo(2 + userColumnCount);
        break;
      case UPDATE_ONLY:
        assertThat(aggInput.getRowType().getFieldCount()).isEqualTo(2 + updatedColumns);
        break;
      case UPDATE_INSERT:
        assertThat(aggInput.getRowType().getFieldCount()).isEqualTo(userColumnCount + 2 + updatedColumns);
        break;
      case INVALID:
      default:
        throw new IllegalArgumentException("invalid merge type: " + mergeType);
    }
    // Verify column name
    testResultColumnName(mergeQuery);
  }
}
