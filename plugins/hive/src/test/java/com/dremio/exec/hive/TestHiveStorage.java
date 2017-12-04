/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.hive;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin2.UpdateStatus;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.hive.proto.HiveReaderProto.FileSystemCachedEntity;
import com.dremio.hive.proto.HiveReaderProto.FileSystemPartitionUpdateKey;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignature;
import com.dremio.hive.proto.HiveReaderProto.HiveReadSignatureType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestHiveStorage extends HiveTestBase {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  @BeforeClass
  public static void setupOptions() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }


  @Test // DRILL-4083
  public void testNativeScanWhenNoColumnIsRead() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));

      String query = "SELECT count(*) as col FROM hive.kv_parquet";
      testPhysicalPlan(query, "mode=[NATIVE_PARQUET");

      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(5L)
          .go();
    } finally {
      test(String.format("alter session set `%s` = %s",
          ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS,
              ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR.getDefault().bool_val ? "true" : "false"));
    }
  }

  @Test
  public void testTimestampNulls() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));

      String query = "SELECT * FROM hive.parquet_timestamp_nulls";
      test(query);
    } finally {
      test(String.format("alter session set `%s` = %s",
        ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS,
        ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR.getDefault().bool_val ? "true" : "false"));
    }
  }

  @Test
  public void hiveReadWithDb() throws Exception {
    test("select * from hive.kv");
  }

  @Test
  public void queryEmptyHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.empty_table")
        .expectsEmptyResultSet()
        .go();
  }

  @Test
  public void queryPartitionedEmptyHiveTable() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * FROM hive.partitioned_empty_table")
      .expectsEmptyResultSet()
      .go();
  }

  @Test // DRILL-3328
  public void convertFromOnHiveBinaryType() throws Exception {
    testBuilder()
        .sqlQuery("SELECT convert_from(binary_field, 'UTF8') col1 from hive.readtest")
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("binaryfield")
        .baselineValues(new Object[]{null})
        .go();
  }

  /**
   * Test to ensure Dremio reads the all supported types correctly both normal fields (converted to Nullable types) and
   * partition fields (converted to Required types).
   * @throws Exception
   */
  @Test
  public void readAllSupportedHiveDataTypes() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.readtest")
        .ordered()
        .baselineColumns(
            "binary_field",
            "boolean_field",
            "tinyint_field",
            "decimal0_field",
            "decimal9_field",
            "decimal18_field",
            "decimal28_field",
            "decimal38_field",
            "double_field",
            "float_field",
            "int_field",
            "bigint_field",
            "smallint_field",
            "string_field",
            "varchar_field",
            "timestamp_field",
            "date_field",
            "char_field",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary_part",
            "boolean_part",
            "tinyint_part",
            "decimal0_part",
            "decimal9_part",
            "decimal18_part",
            "decimal28_part",
            "decimal38_part",
            "double_part",
            "float_part",
            "int_part",
            "bigint_part",
            "smallint_part",
            "string_part",
            "varchar_part",
            "timestamp_part",
            "date_part",
            "char_part")
        .baselineValues(
            "binaryfield".getBytes(),
            false,
            34,
            new BigDecimal("66"),
            new BigDecimal("2347.92"),
            new BigDecimal("2758725827.99990"),
            new BigDecimal("29375892739852.8"),
            new BigDecimal("89853749534593985.783"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "stringfield",
            "varcharfield",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "charfield",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .baselineValues( // All fields are null, but partition fields have non-null values
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .go();
  }

  @Test
  public void testLowUpperCasingForParquet() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      final String query = "SELECT * FROM hive.parquet_region";

      // Make sure the plan has Hive scan with native parquet reader
      testPhysicalPlan(query, "mode=[NATIVE_PARQUET");

      testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns(
          "r_regionkey",
          "r_name",
          "r_comment")
        .baselineValues(
          0L,
          "AFRICA",
          "lar deposits. blithe"
        )
        .baselineValues(
          1L,
          "AMERICA",
          "hs use ironic, even "
        )
        .baselineValues(
          2L,
          "ASIA",
          "ges. thinly even pin"
        )
        .baselineValues(
          3L,
          "EUROPE",
          "ly final courts cajo"
        )
        .baselineValues(
          4L,
          "MIDDLE EAST",
          "uickly special accou"
        ).go();
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  /**
   * Test to ensure Dremio reads the all supported types through native Parquet readers.
   * NOTE: As part of Hive 1.2 upgrade, make sure this test and {@link #readAllSupportedHiveDataTypes()} are merged
   * into one test.
   */
  @Test
  public void readAllSupportedHiveDataTypesNativeParquet() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      final String query = "SELECT * FROM hive.readtest_parquet";

      // Make sure the plan has Hive scan with native parquet reader
      testPhysicalPlan(query, "mode=[NATIVE_PARQUET");

      testBuilder().sqlQuery(query)
          .ordered()
          .baselineColumns(
              "binary_field",
              "boolean_field",
              "tinyint_field",
              "decimal0_field",
              "decimal9_field",
              "decimal18_field",
              "decimal28_field",
              "decimal38_field",
              "double_field",
              "float_field",
              "int_field",
              "bigint_field",
              "smallint_field",
              "string_field",
              "varchar_field",
              "timestamp_field",
              "char_field",
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary_part",
              "boolean_part",
              "tinyint_part",
              "decimal0_part",
              "decimal9_part",
              "decimal18_part",
              "decimal28_part",
              "decimal38_part",
              "double_part",
              "float_part",
              "int_part",
              "bigint_part",
              "smallint_part",
              "string_part",
              "varchar_part",
              "timestamp_part",
              "date_part",
              "char_part")
          .baselineValues(
              "binaryfield".getBytes(),
              false,
              34,
              new BigDecimal("66"),
              new BigDecimal("2347.92"),
              new BigDecimal("2758725827.99990"),
              new BigDecimal("29375892739852.8"),
              new BigDecimal("89853749534593985.783"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "stringfield",
              "varcharfield",
              new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              "charfield",
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary",
              true,
              64,
              new BigDecimal("37"),
              new BigDecimal("36.90"),
              new BigDecimal("3289379872.94565"),
              new BigDecimal("39579334534534.4"),
              new BigDecimal("363945093845093890.900"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "string",
              "varchar",
              new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
              "char")
          .baselineValues( // All fields are null, but partition fields have non-null values
              null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary",
              true,
              64,
              new BigDecimal("37"),
              new BigDecimal("36.90"),
              new BigDecimal("3289379872.94565"),
              new BigDecimal("39579334534534.4"),
              new BigDecimal("363945093845093890.900"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "string",
              "varchar",
              new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
              "char")
          .go();
    } finally {
        test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void orderByOnHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv ORDER BY `value` DESC")
        .ordered()
        .baselineColumns("key", "value")
        .baselineValues(5, " key_5")
        .baselineValues(4, " key_4")
        .baselineValues(3, " key_3")
        .baselineValues(2, " key_2")
        .baselineValues(1, " key_1")
        .go();
  }

  @Test
  public void countStar() throws Exception {
    testPhysicalPlan("SELECT count(*) FROM hive.kv", "columns=[]");
    testPhysicalPlan("SELECT count(*) FROM hive.kv_parquet", "columns=[]");

    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hive.kv")
        .unOrdered()
        .sqlBaselineQuery("SELECT count(key) as cnt FROM hive.kv")
        .go();

    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hive.kv_parquet")
        .unOrdered()
        .sqlBaselineQuery("SELECT count(key) as cnt FROM hive.kv_parquet")
        .go();
  }

  @Test
  public void queryingTablesInNonDefaultFS() throws Exception {
    // Update the default FS settings in Hive test storage plugin to non-local FS
    hiveTest.updatePluginConfig(getSabotContext().getStorage(),
        ImmutableMap.of(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9001"));

    testBuilder()
        .sqlQuery("SELECT * FROM hive.`default`.kv LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .go();
  }

  @Test // DRILL-745
  public void queryingHiveAvroTable() throws Exception {
      testBuilder()
          .sqlQuery("SELECT * FROM hive.db1.avro ORDER BY key DESC LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues(5, " key_5")
        .go();
  }

  @Test // DRILL-3266
  public void queryingTableWithSerDeInHiveContribJar() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db1.kv_db1 ORDER BY key DESC LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues("5", " key_5")
        .go();
  }


  @Test // DRILL-3746
  public void readFromPartitionWithCustomLocation() throws Exception {
    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hive.partition_pruning_test WHERE c=99 AND d=98 AND e=97")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .go();
  }

  @Test // DRILL-3938
  public void readFromAlteredPartitionedTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT key, `value`, newcol FROM hive.kv_parquet ORDER BY key LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value", "newcol")
        .baselineValues(1, " key_1", null)
        .go();
  }

  @Test // DRILL-3938
  public void nativeReaderIsDisabledForAlteredPartitionedTable() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      final String query = "EXPLAIN PLAN FOR SELECT key, `value`, newcol FROM hive.kv_parquet ORDER BY key LIMIT 1";

      // Make sure the HiveScan in plan has no native parquet reader
      final String planStr = getPlanInString(query, OPTIQ_FORMAT);
      assertFalse("Hive native is not expected in the plan", planStr.contains("hive-native-parquet-scan"));
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void readFromMixedSchema() throws Exception {
    testBuilder()
        .sqlQuery("SELECT key, `value` FROM hive.kv_mixedschema")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues("1", " key_1")
        .baselineValues("2", " key_2")
        .baselineValues("5", " key_5")
        .baselineValues("4", " key_4")
        .go();
  }

  @Test // DRILL-3739
  public void readingFromStorageHandleBasedTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv_sh ORDER BY key LIMIT 2")
        .ordered()
        .baselineColumns("key", "value")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DRILL-3739
  public void readingFromStorageHandleBasedTable2() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));

      testBuilder()
          .sqlQuery("SELECT * FROM hive.kv_sh ORDER BY key LIMIT 2")
          .ordered()
          .baselineColumns("key", "value")
          .expectsEmptyResultSet()
          .go();
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test // DRILL-3688
  public void readingFromSmallTableWithSkipHeaderAndFooter() throws Exception {
   testBuilder()
        .sqlQuery("select key, `value` from hive.skipper.kv_text_small order by key asc")
        .ordered()
        .baselineColumns("key", "value")
        .baselineValues(1, "key_1")
        .baselineValues(2, "key_2")
        .baselineValues(3, "key_3")
        .baselineValues(4, "key_4")
        .baselineValues(5, "key_5")
        .go();

    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_text_small")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5L)
        .go();
  }

  @Test // DRILL-3688
  public void readingFromLargeTableWithSkipHeaderAndFooter() throws Exception {
    testBuilder()
        .sqlQuery("select sum(key) as sum_keys from hive.skipper.kv_text_large")
        .unOrdered()
        .baselineColumns("sum_keys")
        .baselineValues((long)(5000*(5000 + 1)/2))
        .go();

    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_text_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIncorrectHeaderFooterProperty() throws Exception {
    Map<String, String> testData = ImmutableMap.<String, String>builder()
        .put("hive.skipper.kv_incorrect_skip_header","skip.header.line.count")
        .put("hive.skipper.kv_incorrect_skip_footer", "skip.footer.line.count")
        .build();

    String query = "select * from %s";
    String exceptionMessage = "Hive table property %s value 'A' is non-numeric";

    for (Map.Entry<String, String> entry : testData.entrySet()) {
      try {
        test(String.format(query, entry.getKey()));
      } catch (UserRemoteException e) {
        assertThat(e.getMessage(), containsString(String.format(exceptionMessage, entry.getValue())));
      }
    }
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForRcfile() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_rcfile_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForParquet() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_parquet_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForSequencefile() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_sequencefile_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test
  public void testQueryNonExistingTable() throws Exception {
    errorMsgTestHelper("SELECT * FROM hive.nonExistedTable", "Table 'hive.nonExistedTable' not found");
    errorMsgTestHelper("SELECT * FROM hive.`default`.nonExistedTable", "Table 'hive.default.nonExistedTable' not found");
    errorMsgTestHelper("SELECT * FROM hive.db1.nonExistedTable", "Table 'hive.db1.nonExistedTable' not found");
  }

  @AfterClass
  public static void shutdownOptions() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test
  public void testReadSignatures() throws Exception {
    getSabotContext().getCatalogService().refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.avro")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.dummy")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.skipper.kv_parquet_large")))).size());

    assertEquals(3, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.readtest")))).size());
    assertEquals(3, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.readtest_parquet")))).size());

    assertEquals(10, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.kv_parquet")))).size());
    assertEquals(54, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.partition_with_few_schemas")))).size());
    assertEquals(56, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.partition_pruning_test")))).size());
  }

  @Test
  public void testCheckReadSignature() throws Exception {
    getSabotContext().getCatalogService().refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);

    DatasetConfig datasetConfig = ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1")));
    assertEquals(UpdateStatus.UNCHANGED, getSabotContext().getCatalogService().getStoragePlugin("hive").checkReadSignature(
        datasetConfig.getReadDefinition().getReadSignature(), datasetConfig).getStatus());

    datasetConfig = ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.partition_with_few_schemas")));
    assertEquals(UpdateStatus.UNCHANGED, getSabotContext().getCatalogService().getStoragePlugin("hive").checkReadSignature(
      datasetConfig.getReadDefinition().getReadSignature(), datasetConfig).getStatus());

    new File(hiveTest.getWhDir() + "/db1.db/kv_db1", "000000_0").setLastModified(System.currentTimeMillis());

    File newFile = new File(hiveTest.getWhDir() + "/partition_with_few_schemas/c=1/d=1/e=1/", "empty_file");
    try {
      newFile.createNewFile();

      datasetConfig = ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1")));
      assertEquals(UpdateStatus.CHANGED, getSabotContext().getCatalogService().getStoragePlugin("hive").checkReadSignature(
        datasetConfig.getReadDefinition().getReadSignature(), datasetConfig).getStatus());

      datasetConfig = ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.`default`.partition_with_few_schemas")));
      assertEquals(UpdateStatus.CHANGED, getSabotContext().getCatalogService().getStoragePlugin("hive").checkReadSignature(
        datasetConfig.getReadDefinition().getReadSignature(), datasetConfig).getStatus());
    } finally {
      newFile.delete();
    }
  }

  @Test
  public void testCheckHasPermission() throws Exception {
    getSabotContext().getCatalogService().refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);


    NamespaceKey dataset = new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1"));
    DatasetConfig datasetConfig = ns.getDataset(dataset);
    assertTrue(getSabotContext().getCatalogService().getStoragePlugin("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));

    final Path tableFile = new Path(hiveTest.getWhDir() + "/db1.db/kv_db1/000000_0");
    final Path tableDir = new Path(hiveTest.getWhDir() + "/db1.db/kv_db1");
    final FileSystem localFs = FileSystem.getLocal(new Configuration());

    try {
      // no read on file
      localFs.setPermission(tableFile, new FsPermission(FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE));
      assertFalse(getSabotContext().getCatalogService().getStoragePlugin("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
    } finally {
      localFs.setPermission(tableFile, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }

    try {
      // no exec on dir
      localFs.setPermission(tableDir, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
      assertFalse(getSabotContext().getCatalogService().getStoragePlugin("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
    } finally {
      localFs.setPermission(tableDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  private List<FileSystemCachedEntity> getCachedEntities(DatasetConfig datasetConfig) throws Exception{
    final HiveReadSignature readSignature = HiveReadSignature.parseFrom(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
    // for now we only support fs based read signatures
    if (readSignature.getType() == HiveReadSignatureType.FILESYSTEM) {
      List<FileSystemCachedEntity> cachedEntities = Lists.newArrayList();
      for (FileSystemPartitionUpdateKey updateKey: readSignature.getFsPartitionUpdateKeysList()) {
        cachedEntities.addAll(updateKey.getCachedEntitiesList());
      }
      return cachedEntities;
    }
    return null;
  }

  @Test
  public void testAddRemoveHiveTable() throws Exception {
    List<NamespaceKey> tables0 = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getAllDatasets(new NamespaceKey("hive"));

    getSabotContext().getCatalogService().refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0l)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0l)
        .setNamesRefreshMs(0l));

    List<NamespaceKey> tables1 = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getAllDatasets(new NamespaceKey("hive"));
    assertEquals(tables0.size(), tables1.size());

    // create an empty table
    hiveTest.executeDDL("CREATE TABLE IF NOT EXISTS foo_bar(a INT, b STRING)");

    getSabotContext().getCatalogService().refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0l)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0l)
        .setNamesRefreshMs(0l));

    // make sure new table is visible
    List<NamespaceKey> tables2 = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getAllDatasets(new NamespaceKey("hive"));
    assertEquals(tables1.size() + 1, tables2.size());

    assertTrue(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.`default`.foo_bar")), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.foo_bar")), Type.DATASET));

    // run query on table with short name
    testBuilder()
      .sqlQuery("SELECT * FROM hive.foo_bar")
      .expectsEmptyResultSet()
      .go();

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.foo_bar")), Type.DATASET));

    // no new table is added
    List<NamespaceKey> tables3 = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getAllDatasets(new NamespaceKey("hive"));
    assertEquals(tables2.size(), tables3.size());

    // drop table
    hiveTest.executeDDL("DROP TABLE foo_bar");

    getSabotContext().getCatalogService().refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
      .setAuthTtlMs(0l)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setDatasetDefinitionTtlMs(0l)
      .setNamesRefreshMs(0l));

    // make sure table is deleted from namespace
    List<NamespaceKey> tables4 = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getAllDatasets(new NamespaceKey("hive"));
    assertEquals(tables3.size() - 1, tables4.size());

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.`default`.foo_bar")), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.foo_bar")), Type.DATASET));
  }
}
