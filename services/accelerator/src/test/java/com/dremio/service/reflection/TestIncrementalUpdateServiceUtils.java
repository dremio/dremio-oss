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
package com.dremio.service.reflection;

import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestIncrementalUpdateServiceUtils extends BaseTestQuery {
  private static IcebergTestTables.Table icebergTable;
  private static SqlConverter converter;
  private static SqlHandlerConfig sqlHandlerConfig;

  private ReflectionSettings reflectionSettings = Mockito.mock(ReflectionSettings.class);
  private ReflectionService reflectionService = Mockito.mock(ReflectionService.class);
  private OptionManager optionManager = Mockito.mock(OptionManager.class);

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

    sqlHandlerConfig = new SqlHandlerConfig(queryContext, converter, observer, null);
  }

  @Before
  public void init() throws Exception {
    icebergTable = IcebergTestTables.NATION.get();
  }

  @After
  public void tearDown() throws Exception {
    icebergTable.close();
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan native iceberg table - Source
   * level configured reflection refresh method: FULL or INCREMENTAL with non-null refresh field -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.INCREMENTAL with non-null refresh field. getSnapshotBased() is true.
   */
  @Test
  public void testScanNativeIcebergSnapshotBasedEnabled() throws Exception {
    String sql = String.format("SELECT * FROM %s", icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);

    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);

    RelNode relNode = convertedRelNode.getConvertedNode();
    ScanCrel scanCrel = findSingleNode(relNode, ScanCrel.class, null);
    TableMetadata tableMetadata = scanCrel.getTableMetadata();
    String snapshotId =
        tableMetadata
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .toString();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertTrue(refreshDetails.getSnapshotBased());
    assertEquals(tableMetadata, refreshDetails.getBaseTableMetadata());
    assertEquals(snapshotId, refreshDetails.getBaseTableSnapshotId());

    // When REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED is true, refresh field should be
    // preserved by IncrementalUpdateServiceUtils#extractRefreshDetails
    accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshField("abc");
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());
    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertEquals(refreshDetails.getRefreshField(), "abc");
    assertTrue(refreshDetails.getSnapshotBased());
    assertEquals(tableMetadata, refreshDetails.getBaseTableMetadata());
    assertEquals(snapshotId, refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan native iceberg table - Source
   * level configured reflection refresh method: FULL or INCREMENTAL with non-null refresh field -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: false -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: true - Expected result:
   * RefreshMethod.FULL or INCREMENTAL with non-null refresh field. getSnapshotBased() is false.
   */
  @Test
  public void testScanNativeIcebergSnapshotBasedDisabled() throws Exception {
    String sql = String.format("SELECT * FROM %s", icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertEquals(null, refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());

    // When REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED is false, refresh field should be
    // preserved.
    accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshField("abc");
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertEquals("abc", refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (unlimited split) - Source level configured reflection refresh method: FULL -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: false -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: true - Expected result:
   * RefreshMethod.INCREMENTAL. getSnapshotBased() is true.
   */
  @Test
  public void testUnlimitedSplitsSnapshotBasedEnabled() throws Exception {
    String sql = "SELECT * FROM cp.\"test_table.parquet\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();
    ScanCrel scanCrel = findSingleNode(relNode, ScanCrel.class, null);
    TableMetadata tableMetadata = scanCrel.getTableMetadata();
    String snapshotId =
        tableMetadata
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .toString();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertTrue(refreshDetails.getSnapshotBased());
    assertEquals(tableMetadata, refreshDetails.getBaseTableMetadata());
    assertEquals(snapshotId, refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (unlimited split) - Source level configured reflection refresh method: FULL -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.INCREMENTAL. getSnapshotBased() is false.
   */
  @Test
  public void testUnlimitedSplitsSnapshotBasedDisabled() throws Exception {
    String sql = "SELECT * FROM cp.\"test_table.parquet\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /** Test incremental refresh is disabled if PDS is file (not folder) based. */
  @Test
  public void testFileBasedDataset() {
    final DatasetConfig datasetConfig =
        new DatasetConfig().setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    assertFalse(
        IncrementalUpdateServiceUtils.isIncrementalRefreshBySnapshotEnabled(datasetConfig, null));
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (json) - Source level configured reflection refresh method: FULL or INCREMENTAL with non-null
   * refresh field - REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: true - Expected result:
   * RefreshMethod.FULL or INCREMENTAL with non-null refresh field
   */
  @Test
  public void testScanJsonSnapshotBasedEnabled() throws Exception {
    String sql = "SELECT * FROM cp.\"employees.json\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());

    // REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED shouldn't affect dataset refresh
    // setting
    accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshField("abc");
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertEquals("abc", refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (json) - Source level configured reflection refresh method: FULL or INCREMENTAL with non-null
   * refresh field - REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: false -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.FULL or INCREMENTAL with non-null refresh field
   */
  @Test
  public void testScanJsonSnapshotBasedDisabled() throws Exception {
    String sql = "SELECT * FROM cp.\"employees.json\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());

    accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshField("col1");
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertEquals("col1", refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan native iceberg table - Source
   * level configured reflection refresh method: FULL -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: true - There is unsupported operator in
   * plan: Join - Expected result: RefreshMethod.FULL
   */
  @Test
  public void testSnapshotBasedEnabledUnsupportedOp() throws Exception {
    String sql =
        String.format(
            "SELECT * FROM %s AS n1 FULL OUTER JOIN %s AS n2 ON n1.n_nationkey = n2.n_nationkey",
            icebergTable.getTableName(), icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan native iceberg table - Source
   * level configured reflection refresh method: AUTO -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.INCREMENTAL. getSnapshotBased() is true.
   */
  @Test
  public void testScanNativeIcebergSnapshotBasedEnabledRefreshSettingMethodAuto() throws Exception {
    String sql = String.format("SELECT * FROM %s", icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();
    ScanCrel scanCrel = findSingleNode(relNode, ScanCrel.class, null);
    TableMetadata tableMetadata = scanCrel.getTableMetadata();
    String snapshotId =
        tableMetadata
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .toString();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.AUTO);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertTrue(refreshDetails.getSnapshotBased());
    assertEquals(tableMetadata, refreshDetails.getBaseTableMetadata());
    assertEquals(snapshotId, refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (unlimited split) - Source level configured reflection refresh method: AUTO -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: true -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.FULL. getSnapshotBased() is false.
   */
  @Test
  public void testUnlimitedSplitsSnapshotBasedDisabledRefreshSettingMethodAuto() throws Exception {
    String sql = "SELECT * FROM cp.\"test_table.parquet\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.AUTO);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Scan non-native iceberg table
   * (json) - Source level configured reflection refresh method: AUTO -
   * REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED: false -
   * REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL: false - Expected result:
   * RefreshMethod.FULL
   */
  @Test
  public void testScanJsonSnapshotBasedDisabledRefreshSettingMethodAuto() throws Exception {
    String sql = "SELECT * FROM cp.\"employees.json\";";
    final SqlNode node = converter.parse(sql);
    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.AUTO);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(false);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(false);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            relNode,
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Dynamic function is disallowed for
   * incremental refresh.
   */
  @Test
  public void testDynamicFunctionDisallowed() throws Exception {
    final String sql = String.format("SELECT now() n, * FROM %s", icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);

    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.INCREMENTAL).setRefreshField("abc");
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            convertedRelNode.getConvertedNode(),
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of(DremioSqlOperatorTable.NOW));

    // ASSERT
    assertEquals(RefreshMethod.FULL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertFalse(refreshDetails.getSnapshotBased());
    assertNull(refreshDetails.getBaseTableMetadata());
    assertNull(refreshDetails.getBaseTableSnapshotId());
    assertEquals(
        "Cannot do incremental update because the reflection has dynamic function(s): NOW",
        refreshDetails.getFullRefreshReason());
  }

  /**
   * Test IncrementalUpdateServiceUtils.extractRefreshDetails() - Non-deterministic function is
   * disallowed for incremental refresh.
   */
  @Test
  public void testNonDeterministicFunctionAllowed() throws Exception {
    final String sql = String.format("SELECT RANDOM() n, * FROM %s", icebergTable.getTableName());

    final SqlNode node = converter.parse(sql);

    final ConvertedRelNode convertedRelNode =
        SqlToRelTransformer.validateAndConvert(sqlHandlerConfig, node);
    RelNode relNode = convertedRelNode.getConvertedNode();
    ScanCrel scanCrel = findSingleNode(relNode, ScanCrel.class, null);
    TableMetadata tableMetadata = scanCrel.getTableMetadata();
    String snapshotId =
        tableMetadata
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getSnapshotId()
            .toString();

    AccelerationSettings accelerationSettings =
        new AccelerationSettings().setMethod(RefreshMethod.FULL);
    when(reflectionSettings.getReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(accelerationSettings);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED))
        .thenReturn(true);
    when(optionManager.getOption(
            ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL))
        .thenReturn(true);

    // TEST
    IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            convertedRelNode.getConvertedNode(),
            reflectionSettings,
            reflectionService,
            optionManager,
            config,
            ImmutableList.of());

    // ASSERT
    assertEquals(RefreshMethod.INCREMENTAL, refreshDetails.getRefreshMethod());
    assertNull(refreshDetails.getRefreshField());
    assertTrue(refreshDetails.getSnapshotBased());
    assertEquals(tableMetadata, refreshDetails.getBaseTableMetadata());
    assertEquals(snapshotId, refreshDetails.getBaseTableSnapshotId());
  }
}
