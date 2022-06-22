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

import static com.dremio.exec.ExecConstants.CTAS_CAN_USE_ICEBERG;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_ADVANCED_DML;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.BaseTestQuery;
import com.dremio.common.SuppressForbidden;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.calcite.logical.TableModifyCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.observer.AttemptObserver;
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
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.direct.TruncateTableHandler;
import com.dremio.exec.planner.sql.handlers.query.DeleteHandler;
import com.dremio.exec.planner.sql.handlers.query.DmlHandler;
import com.dremio.exec.planner.sql.handlers.query.InsertTableHandler;
import com.dremio.exec.planner.sql.handlers.query.MergeHandler;
import com.dremio.exec.planner.sql.handlers.query.UpdateHandler;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.planner.sql.parser.SqlMergeIntoTable;
import com.dremio.exec.planner.sql.parser.SqlTruncateTable;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.sys.SystemStoragePlugin;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.TypeValidators;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;

/**
 * Tests assignment priorities of queries with single and multiple fragments.
 */
public class TestDml extends BaseTestQuery {

  private static IcebergTestTables.Table table;
  private static SqlConverter converter;
  private static SqlHandlerConfig config;
  private static int userColumnCount;

  @BeforeClass
  public static void setup() {
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
    turnOnBooleanOption(config, ExecConstants.ENABLE_ICEBERG_ADVANCED_DML);
    userColumnCount = getUserColumnCount();
  }

  private static void turnOnBooleanOption(SqlHandlerConfig configuration, TypeValidators.BooleanValidator option) {
    setBooleanOption(configuration, option, true);
  }

  @AfterClass
  public static void afterClass() {
    setBooleanOption(config, ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal());
  }

  private static void setBooleanOption(SqlHandlerConfig configuration, TypeValidators.BooleanValidator option,  boolean flag) {
    configuration.getContext().getOptions().setOption(OptionValue.createBoolean(
      OptionValue.OptionType.SYSTEM,
      option.getOptionName(),
      flag));
  }

  @Before
  public void beforeTest() throws Exception {
    table = IcebergTestTables.V2_ORDERS.get();
    table.enableIcebergSystemOptions();
  }

  @After
  public void afterTest() throws Exception {
    table.close();
  }

  private static int getUserColumnCount(){
    final String select = "select * from " + IcebergTestTables.V2_ORDERS.get().getTableName() + " limit 1";
    try {
      final SqlNode node = converter.parse(select);
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
      return convertedRelNode.getValidatedRowType().getFieldCount();
    } catch (Exception ex) {
      Assert.fail(String.format("Query %s failed, exception: %s", select, ex));
    }
    return 0;
  }

  @Test
  public void testFailedValidations() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);

    String sql = "Delete from " + table.getTableName() + " where order_id > 10";
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlDeleteFromTable.class).getPath();

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.FALSE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, path, SqlKind.DELETE))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.FALSE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, path, SqlKind.DELETE))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.FALSE);
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, path, SqlKind.DELETE))
      .isInstanceOf(UserException.class);

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG_ADVANCED_DML)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> DmlHandler.validateDmlRequest(mockCatalog, mockConfig, path, SqlKind.DELETE))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testTruncateTableFailedValidation() throws Exception {
    SqlHandlerConfig mockConfig = Mockito.mock(SqlHandlerConfig.class);
    QueryContext mockQueryContext = Mockito.mock(QueryContext.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);

    when(mockConfig.getContext()).thenReturn(mockQueryContext);
    when(mockQueryContext.getOptions()).thenReturn(mockOptionManager);
    String sql = "TRUNCATE " + table.getTableName();
    final SqlNode node = converter.parse(sql);
    NamespaceKey path = SqlNodeUtil.unwrap(node, SqlTruncateTable.class).getPath();

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> TruncateTableHandler.validateDmlRequest(mockCatalog, path, "TRUNCATE TABLE"))
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

    when(mockQueryContext.getOptions().getOption(ENABLE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockQueryContext.getOptions().getOption(CTAS_CAN_USE_ICEBERG)).thenReturn(Boolean.TRUE);
    when(mockCatalog.getSource(path.getRoot())).thenReturn(mock(SystemStoragePlugin.class));
    assertThatThrownBy(() -> InsertTableHandler.validateDmlRequest(mockCatalog, path, SqlKind.INSERT))
      .isInstanceOf(UserException.class);
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterRewriteSingleScanCrel() throws Exception {
    String sql = format("DELETE FROM %s", table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    Assert.assertEquals(userColumnCount, convertedRelNode.getValidatedRowType().getFieldCount());

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    ScanCrel scanCrel = findSingleNode(tableModifyCrel, ScanCrel.class, null);
    assertTrue("scanCrel should be substitutable before rewrite", scanCrel.isSubstitutable());

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = DmlHandler.TableModifyScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrel = findSingleNode(rewrite, ScanCrel.class, null);
    assertFalse("scanCrel should not be substitutable after rewrite", scanCrel.isSubstitutable());
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterRewriteMultipleScanCrels() throws Exception {
    String sql = format("merge into %s using %s as s on s.order_id = %s.order_id when matched then update set order_id = -1 ",
      table.getTableName(), table.getTableName(), table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    Assert.assertEquals(userColumnCount, convertedRelNode.getValidatedRowType().getFieldCount());

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    List<ScanCrel> scanCrels = findNodesMoreThanOne(tableModifyCrel, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel ->  assertTrue("scanCrel should be substitutable before rewrite", scanCrel.isSubstitutable()));

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = DmlHandler.TableModifyScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrels = findNodesMoreThanOne(rewrite, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel ->  assertFalse("scanCrel should not be substitutable after rewrite", scanCrel.isSubstitutable()));
  }

  @Test
  public void testTableModifyScanCrelSubstitutionRewriterMixedScanCrels() throws Exception {
    IcebergTestTables.Table nation = IcebergTestTables.NATION.get();
    nation.enableIcebergSystemOptions();

    String sql = format("merge into %s using (select * from %s) as s on s.n_nationkey = %s.order_id when matched then update set order_id = -1 ",
      table.getTableName(), nation.getTableName(), table.getTableName());
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, node);
    Assert.assertEquals(userColumnCount, convertedRelNode.getValidatedRowType().getFieldCount());

    // find TableModifyCrel
    TableModifyCrel tableModifyCrel = findSingleNode(convertedRelNode.getConvertedNode(), TableModifyCrel.class, null);

    // find the ScanCrel inside TableModifyCrel
    List<ScanCrel> scanCrels = findNodesMoreThanOne(tableModifyCrel, ScanCrel.class, null);
    scanCrels.stream().forEach(scanCrel ->  assertTrue("scanCrel should be substitutable before rewrite", scanCrel.isSubstitutable()));

    // disable substitution of TableModifyCrel's target table ScanCrel
    RelNode rewrite = DmlHandler.TableModifyScanCrelSubstitutionRewriter.disableScanCrelSubstitution(convertedRelNode.getConvertedNode());

    // find the ScanCrel from rewritten TableModifyCrel
    scanCrels = findNodesMoreThanOne(rewrite, ScanCrel.class, null);
    scanCrels.stream().forEach(
      scanCrel ->  {
        if (scanCrel.getTableMetadata().getName().toString().equals(table.getTableName())) {
          assertFalse("Target scanCrel should not be substitutable after rewrite", scanCrel.isSubstitutable());
        } else {
          assertTrue("Source scanCrel (different from target scanCrel) should still be substitutable after rewrite", scanCrel.isSubstitutable());
        }});
    nation.close();
  }

  @Test
  public void testDelete() throws Exception {
    final String delete = "Delete from " + table.getTableName() + " where order_id > 10";

    Prel plan = getDmlPlan(delete);

    validateCommonCopyOnWritePlan(plan, JoinRelType.LEFT, MergeType.INVALID);

    // validate filter on top of copy-on-write join
    validateDeleteSpecificOperationsAfterCopyOnWriteJoin(plan);
  }

  private void checkNoDeleteFilterOperationsAfterCopyOnWriteJoin(Prel plan) {
    List<FilterPrel> filterPrels = findDeletePlanFilter(plan);
    assertEquals("no filter is expected", 0, filterPrels.size());
  }

  @Test
  public void testUpdate() throws Exception {
    final String update = "Update " + table.getTableName() + " set order_id = -1";

    Prel plan = getDmlPlan(update);

    // validate basic copy-on-write plan
    validateCommonCopyOnWritePlan(plan, JoinRelType.LEFT, MergeType.INVALID);

    // validate no delete filter
    checkNoDeleteFilterOperationsAfterCopyOnWriteJoin(plan);
  }

  @Test
  public void testMerge_updateOnly() throws Exception {
    final String merge_updateOnly = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when matched then update set order_id = -1";

    testMerge(merge_updateOnly, 1, MergeType.UPDATE_ONLY);
  }

  @Test
  public void testMerge_insertOnly() throws Exception {
    final String query = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when not matched then INSERT(order_id) VALUES(-1)";

    testMerge(query, 0, MergeType.INSERT_ONLY);
  }

  @Test
  public void testMerge_updateWithInsert() throws Exception {
    final String merge_updateWithInsert = "merge into " + table.getTableName() + " using " + table.getTableName()
      + " as s on " + table.getTableName() + ".order_id = s.order_id when matched then update set order_id = -1 WHEN NOT MATCHED THEN INSERT(order_id) VALUES(-3) ";

    testMerge(merge_updateWithInsert, 1, MergeType.UPDATE_INSERT);
  }

  private void testMerge(String mergeQuery, int updatedColumns, MergeType mergeType) throws Exception {

    Prel plan = getDmlPlan(mergeQuery);

    // validate basic copy-on-write plan
    validateCommonCopyOnWritePlan(plan, JoinRelType.FULL, mergeType);

    // validate no delete filter
    checkNoDeleteFilterOperationsAfterCopyOnWriteJoin(plan);

    // merge specific
    Prel agg = validateDmlAgg(plan, mergeType);

    RelNode aggInput = agg.getInput(0);

    switch (mergeType) {
      case INSERT_ONLY:
        assertEquals( 2 + userColumnCount, aggInput.getRowType().getFieldCount());
        break;
      case UPDATE_ONLY:
        assertEquals(2 + updatedColumns, aggInput.getRowType().getFieldCount());
        break;
      case UPDATE_INSERT:
        assertEquals(userColumnCount + 2 + updatedColumns, aggInput.getRowType().getFieldCount());
    }
  }

  @Test
  public void testDelete_columnName() throws Exception {
    testResultColumnName(String.format("DELETE FROM %s", table.getTableName()));
  }

  @Test
  public void testMerge_columnName() throws Exception {
    testResultColumnName(
      String.format("MERGE INTO %s USING %s AS s ON %s.order_id = s.order_id WHEN MATCHED THEN UPDATE SET order_id = -1",
        table.getTableName(), table.getTableName(), table.getTableName()));
  }

  @Test
  public void testUpdate_columnName() throws Exception {
    testResultColumnName(String.format("UPDATE %s SET order_id = -1", table.getTableName()));
  }

  private void testResultColumnName(String query) throws Exception {
    TableModify.Operation operation = null;
    if (query.startsWith("DELETE")) {
      operation = TableModify.Operation.DELETE;
    } else if (query.startsWith("MERGE")) {
      operation = TableModify.Operation.MERGE;
    } else if (query.startsWith("UPDATE")) {
      operation = TableModify.Operation.UPDATE;
    } else {
      fail();
    }

    assertEquals(DmlUtils.DML_OUTPUT_COLUMN_NAMES.get(operation),
      PrelTransformer.validateAndConvert(config, converter.parse(query))
        .getConvertedNode().getRowType().getFieldNames().get(0));
  }

  @Test
  public void testDmlUseHashDistributionForWrites() throws Exception {
    turnOnBooleanOption(config, ExecConstants.ENABLE_ICEBERG_DML_USE_HASH_DISTRIBUTION_FOR_WRITES);
    final String delete = "Delete from " + table.getTableName() + " where order_id > 10";
    Prel plan = getDmlPlan(delete);
    validateHashDistributionTraitsForWrites(plan);

    setBooleanOption(config, ExecConstants.ENABLE_ICEBERG_DML_USE_HASH_DISTRIBUTION_FOR_WRITES, ExecConstants.ENABLE_ICEBERG_DML_USE_HASH_DISTRIBUTION_FOR_WRITES.getDefault().getBoolVal());
  }

  private static DmlHandler getDmlHandler(SqlNode node) {
    if (node instanceof SqlDeleteFromTable) {
      return new DeleteHandler();
    } else if (node instanceof SqlUpdateTable) {
      return new UpdateHandler();
    } else if (node instanceof SqlMergeIntoTable) {
      return new MergeHandler();
    }
    Assert.fail("This should never happen");

    return null;
  }

  private Prel getDmlPlan(Catalog catalog, SqlNode sqlNode) throws Exception {
    DmlHandler dmlHandler = getDmlHandler(sqlNode);
    return dmlHandler.getNonPhysicalPlan(catalog, config, sqlNode);
  }

  private Prel getDmlPlan(String sql) throws Exception {
    return getDmlPlan(config.getContext().getCatalog(), converter.parse(sql));
  }

  // get CopyOnWriteJoin condition, left.file_path = right.file_path and left.row_index = right.row_index
  private static String getCopyOnWriteJoinCondition(MergeType mergeType) {
    return String.format("AND(=($%d, $%d), =($%d, $%d))",
      userColumnCount,  (mergeType == MergeType.UPDATE_INSERT ? userColumnCount : 0) + userColumnCount + 2, // file_path column
      userColumnCount + 1, (mergeType == MergeType.UPDATE_INSERT ? userColumnCount : 0) + userColumnCount + 3); // row_index column
  }

  private static <TPlan extends RelNode, TClass> TClass findSingleNode(TPlan plan, Class<TClass> clazz, Map<String, String> attributes) {
    return findFirstNode(plan, clazz, attributes, true);
  }

  private static <TPlan extends RelNode, TClass> TClass findFirstNode(TPlan plan, Class<TClass> clazz, Map<String, String> attributes, boolean isSingle) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(clazz, attributes);
    List<TClass> nodes= NodeFinder.find(plan, descriptor);
    assertNotNull(nodes);
    if (isSingle) {
      assertEquals("1 node is expected", 1, nodes.size());
    }

    TClass node = nodes.get(0);
    assertNotNull("Node is expected",  node);

    return node;
  }

  private static <TPlan extends RelNode, TClass> List<TClass> findNodesMoreThanOne(TPlan plan, Class<TClass> clazz, Map<String, String> attributes) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(clazz, attributes);
    List<TClass> nodes= NodeFinder.find(plan, descriptor);
    assertNotNull(nodes);
    assertTrue("Multiple nodes are expected", nodes.size() > 1);

    return nodes;
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

  private void validateHashDistributionTraitsForWrites(Prel plan) {
    Map<String, String> attributes = Collections.EMPTY_MAP;
    TargetNodeDescriptor manifestWriteDescriptor =
      new TargetNodeDescriptor(IcebergManifestWriterPrel.class, attributes);
    List<IcebergManifestWriterPrel> manifestWriterPrels =NodeFinder.find(plan, manifestWriteDescriptor);

    // ManifestWriterPrel has an input ProjectPrel that has distribution trait on the RecordWriter.ICEBERG_METADATA field.
    // WriterPrule.getManifestWriterPrelIfNeeded adds the hash distribution trait.
    assertNotNull("Manifest writer is expected",  manifestWriterPrels);
    assertEquals("Manifest writer is expected", 1, manifestWriterPrels.size());
    IcebergManifestWriterPrel manifestWriterPrel = manifestWriterPrels.get(0);
    assertNotNull("Manifest writer is expected",  manifestWriterPrels);
    validateHashDistributionInProjectPrel(manifestWriterPrel.getInput(0));

    // WriterUpdater.getTableFunctionOnPartitionColumns adds a TableFunctionPrel on partition columns with
    // hash distribution trait. We need to verify that as well.
    attributes = ImmutableMap.of(
      "rowType", "RecordType(INTEGER order_id, INTEGER order_year, TIMESTAMP(3) order_date, VARCHAR(65536) product_name, DOUBLE amount, INTEGER order_year_identity)");
    TargetNodeDescriptor tableFunctionDescriptor =
      new TargetNodeDescriptor(TableFunctionPrel.class, attributes);
    List<TableFunctionPrel> tableFunctionPrels =NodeFinder.find(plan, tableFunctionDescriptor);
    assertNotNull("Table function is expected",  tableFunctionPrels);
    assertEquals("Table function is expected", 1, tableFunctionPrels.size());

    validateHashDistributionInProjectPrel(tableFunctionPrels.get(0).getInput(0));
  }

  private void validateHashDistributionInProjectPrel(RelNode relNode) {
    assertTrue("Project is expected", relNode instanceof ProjectPrel);
    ProjectPrel prel = (ProjectPrel) relNode;
    RelTraitSet traits = prel.getTraitSet();
    assertTrue("Project has at least three trait", prel.getTraitSet().size() >= 3);
    assertTrue("Second trait is DistributionTrait", traits.getTrait(1) instanceof  DistributionTrait);
    DistributionTrait distributionTrait = (DistributionTrait) traits.getTrait(1);
    assertTrue("HASH_DISTRIBUTED is expected", distributionTrait.getType().equals(DistributionTrait.DistributionType.HASH_DISTRIBUTED));
  }

  private void validateDeleteSpecificOperationsAfterCopyOnWriteJoin(Prel plan) {
    List<FilterPrel> filterPrels = findDeletePlanFilter(plan);

    assertNotNull("filter is expected",  filterPrels);
    assertEquals("filter is expected", 1, filterPrels.size());

    FilterPrel filterPrel = filterPrels.get(0);
    assertNotNull("filter is expected",  filterPrel);
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
    assertNotNull("did not find record fild", recordFieldIndex);
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

    assertEquals("2 inputs are expected for union prel", unionAllPrel.getInputs().size(), 2);

    TargetNodeDescriptor writerDescriptor =  new TargetNodeDescriptor(WriterPrel.class, null);
    List<TableFunctionPrel> writerPrels= NodeFinder.find(unionAllPrel.getInput(0), writerDescriptor);
    boolean writerIsOnZeroUnionInput = CollectionUtils.isNotEmpty(writerPrels);

    validateDeletedDataFilesTableFunction(unionAllPrel.getInput(writerIsOnZeroUnionInput ? 1 : 0), mergeType);

    validateBaseCopyOnWriteJoinPlan(unionAllPrel.getInput(writerIsOnZeroUnionInput ? 0 : 1), joinType, mergeType);
  }

  private static void validateDeletedDataFilesTableFunction(RelNode plan, MergeType mergeType) {
    TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(TableFunctionPrel.class, null);
    List<TableFunctionPrel> tableFunctionPrels= NodeFinder.find(plan, descriptor);
    assertNotNull(tableFunctionPrels);

    TableFunctionPrel deletedDataFilesTableFunctionPre = tableFunctionPrels.get(0);
    assertEquals(deletedDataFilesTableFunctionPre.getTableFunctionConfig().getType(), TableFunctionConfig.FunctionType.DELETED_DATA_FILES_METADATA);

    validateDmlAgg(deletedDataFilesTableFunctionPre, mergeType);
  }

  private static void validateBaseCopyOnWriteJoinPlan(RelNode plan, JoinRelType joinType, MergeType mergeType) {
    Map<String, String> attributes = ImmutableMap.of(
      "joinType", joinType.toString(),
      "condition", getCopyOnWriteJoinCondition(mergeType)
    );

    if (mergeType == MergeType.INSERT_ONLY) {
      TargetNodeDescriptor descriptor =  new TargetNodeDescriptor(HashJoinPrel.class, attributes);
      List<HashJoinPrel> hashJoinPrels = NodeFinder.find(plan, descriptor);
      assertEquals("no copy-on-write join is expected to find", 0, hashJoinPrels.size());
      return;
    }

    HashJoinPrel hashJoinPrel = findSingleNode(plan, HashJoinPrel.class, attributes);

    validateFilePathJoin(hashJoinPrel.getLeft(), mergeType);
  }

  private static void validateFilePathJoin(RelNode plan, MergeType mergeType) {
    Map<String, String> attributes = ImmutableMap.of(
      "joinType", "inner",
      "condition", "=($0, $7)"
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

    validateDataFileScanColumns(dataFileScanSubTree, mergeType);

    return aggPrel;
  }

  public static Set<String> extractColumnsFromRowType(RelDataType rowType) {
    return rowType.getFieldList().stream()
      .map(f -> f.getName())
      .collect(Collectors.toSet());
  }

  public static void validateDataFileScanColumns(RelNode plan,  MergeType mergeType) {

    Set<String> expectedUsedFields = Stream.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME)
          .collect(Collectors.toCollection(HashSet::new));

    // if there are other operators between Agg and data scan TF, add their output fields
    // Those operators could be Filter, Project, Join etc.
    //  This is not a correct way to extract used columns from operators, but it is good enough for this test
    List<FilterPrel> filters = NodeFinder.find(plan, new TargetNodeDescriptor(FilterPrel.class, ImmutableMap.of()));
    if (filters.size() > 0) {
      assertEquals("expect at most 1 filter", 1, filters.size());
      expectedUsedFields.addAll(extractColumnsFromRowType(filters.get(0).getRowType()));
    }

    List<HashJoinPrel> joins = NodeFinder.find(plan, new TargetNodeDescriptor(HashJoinPrel.class, ImmutableMap.of()));
    if (joins.size() > 0) {
      assertEquals("expect at most 1 join", 1, joins.size());
      // left side of the join contains the columns from original table
      expectedUsedFields.addAll(extractColumnsFromRowType(joins.get(0).getLeft().getRowType()));
    }

    TableFunctionPrel dataFileScanTF = findFirstNode(plan, TableFunctionPrel.class, null, false);
    Set<String> actualUsedFields = extractColumnsFromRowType(dataFileScanTF.getRowType());
    assertEquals(expectedUsedFields, actualUsedFields);
  }

  public static class TargetNodeDescriptor {
    public Class clazz;
    public Map<String, String> attributes;

    public TargetNodeDescriptor(Class clazz, Map<String, String> attributes) {
      this.clazz = clazz;
      this.attributes = attributes;
    }
  }

  @SuppressForbidden
  private static class NodeFinder<T extends RelNode> extends StatelessRelShuttleImpl {
    private final List<T> collectedNodes = new ArrayList<>();
    private final TargetNodeDescriptor targetNodeDescriptor;
    public static<T> List<T> find(RelNode input, TargetNodeDescriptor targetNodeDescriptor) {
      NodeFinder finder = new NodeFinder(targetNodeDescriptor);
      finder.visit(input);
      return finder.collect();
    }

    public NodeFinder(TargetNodeDescriptor targetNodeDescriptor) {
      this.targetNodeDescriptor = targetNodeDescriptor;
    }

    public List<T> collect() {
      return collectedNodes;
    }

    private static Field getField(Class clazz, String fieldName)
      throws NoSuchFieldException {
      try {
        return clazz.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        Class superClass = clazz.getSuperclass();
        if (superClass==null) {
          throw e;
        } else {
          return getField(superClass, fieldName);
        }
      }
    }

    private boolean matchAttributes(T node) {
      if (targetNodeDescriptor.attributes == null) {
        return true;
      }

      try {
        for (Map.Entry<String, String> entry : targetNodeDescriptor.attributes.entrySet()) {
          String attribue = entry.getKey();
          Field field = getField(targetNodeDescriptor.clazz, attribue);
          field.setAccessible(true);
          Object value = field.get(node);
          if (!value.toString().toLowerCase().contains(entry.getValue().toLowerCase())) {
            return false;
          }
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        Assert.fail("This should never happen");
      }

      return true;
    }

    @Override
    public RelNode visit(RelNode node) {
      return visitNode(node);
    }

    @Override
    public RelNode visit(TableScan scan) {
      return visitNode(scan);
    }

    private RelNode visitNode(RelNode node) {
      if (targetNodeDescriptor.clazz.isAssignableFrom(node.getClass())  && matchAttributes((T)node)) {
        collectedNodes.add((T)node);
      }
      return super.visit(node);
    }
  }
}
