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
package com.dremio.exec.hive;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.common.utils.PathUtils.parseFullPath;
import static com.dremio.exec.store.hive.exec.HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.DatasetMetadataAdapter;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.ImpersonationUtil;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ITHiveStorage extends HiveTestBase {
  protected static Boolean runWithUnlimitedSplitSupport = false;
  private static AutoCloseable mapEnabled;

  @BeforeClass
  public static void enableMapFeature() {
    mapEnabled = enableMapDataType();
  }

  @AfterClass
  public static void resetMapFeature() throws Exception {
    mapEnabled.close();
  }

  @Test // DRILL-4083
  public void testNativeScanWhenNoColumnIsRead() throws Exception {
    String query = "SELECT count(*) as col FROM hive.kv_parquet";
    testPhysicalPlan(query, "IcebergManifestList(table=[");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(5L)
        .go();
  }

  @Test
  public void testTimestampNulls() throws Exception {
    test("SELECT * FROM hive.parquet_timestamp_nulls");
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

  @Test
  public void readAllSupportedHiveDataTypesText() throws Exception {
    readAllSupportedHiveDataTypes("readtest");
  }

  @Test
  public void orcTestMoreColumnsInExtTable() throws Exception {
    Assume.assumeFalse(runWithUnlimitedSplitSupport);
    String query = "SELECT col2, col3 FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col2", "col3")
      .baselineValues(2, null)
      .go();

    String query2 = "SELECT col3, col2 FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query2)
      .ordered()
      .baselineColumns("col3", "col2")
      .baselineValues(null, 2)
      .go();

    String query3 = "SELECT * FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query3)
      .ordered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, 2, null, null)
      .go();

    String query4 = "SELECT col4 FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query4)
      .ordered()
      .baselineColumns("col4")
      .baselineValues(null)
      .go();
  }

  @Test
  public void orcTestDecimalConversion() throws Exception {
    String query = "SELECT * FROM hive.decimal_conversion_test_orc";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3")
      .baselineValues(new BigDecimal("111111111111111111111.111111111"), new BigDecimal("22222222222222222.222222"), new BigDecimal("333.00"))
      .go();

    query = "SELECT * FROM hive.decimal_conversion_test_orc_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3")
      .baselineValues(new BigDecimal("111111111111111111111.11"), "22222222222222222.222222", "333")
      .go();

    query = "SELECT * FROM hive.decimal_conversion_test_orc_ext_2";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3")
      .baselineValues(null, null, new BigDecimal("333.0"))
      .go();

    query = "SELECT * FROM hive.decimal_conversion_test_orc_rev_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3")
      .baselineValues(null, null, null)
      .go();

    query = "SELECT * FROM hive.decimal_conversion_test_orc_decimal";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9")
      .baselineValues(
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.1234567891"))
      .go();

    query = "SELECT * FROM hive.decimal_conversion_test_orc_decimal_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9")
      .baselineValues(
        new BigDecimal("12345678912345678912.1234567891"),
        null,
        new BigDecimal("12345678912345678912.12346"),
        new BigDecimal("12345678912345678912.1234567891"),
        new BigDecimal("12345678912345678912.123456789100000"),
        new BigDecimal("12345678912345678912.12346"),
        null,
        null,
        new BigDecimal("12345678912345678912.12346"))
      .go();
  }

  @Test
  public void parquetTestDecimalConversion() throws Exception {

    String query = "SELECT * FROM hive.decimal_conversion_test_parquet";
    testBuilder().sqlQuery(query)
            .ordered()
            .baselineColumns("col1", "col2", "col3")
            .baselineValues(new BigDecimal("111111111111111111111.111111111"), new BigDecimal("22222222222222222.222222"), new BigDecimal("333.00"))
            .go();

    // these are decimal to string coversions.
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_ext";
    errorMsgWithTypeTestHelper(query, UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION,
            "Field [col2] has incompatible types in file and table.");

    // these are decimal to decimal overflow for col1 and col2
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_ext_2";
    testBuilder().sqlQuery(query)
            .ordered()
            .baselineColumns("col1", "col2", "col3")
            .baselineValues(null , null, new BigDecimal("333.0"))
            .go();

    // convert int,string,double to decimals
    // string to decimal(col2) is an overflow
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_rev_ext";
    String expectedErrorMsg = "(.*)Field \\[col2] has incompatible types in file and table\\.(.*)file Path: (.*)";
    try {
      test(query);
    } catch (Exception e) {
      if (!(e instanceof UserRemoteException)) {
        fail("Unexpected Error");
      }
      boolean errorMsgMatched = Pattern.compile(expectedErrorMsg, Pattern.DOTALL).matcher(e.getMessage()).matches();
      assertTrue("error Message didn't match",errorMsgMatched);
    }
    // all conversions are valid
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_decimal";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9")
        .baselineValues(
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.1234567891"))
        .go();

    // some overflow conversions returned as null
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_decimal_ext";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9")
        .baselineValues(
            new BigDecimal("12345678912345678912.1234567891"),
            null,
            new BigDecimal("12345678912345678912.12346"),
            new BigDecimal("12345678912345678912.1234567891"),
            new BigDecimal("12345678912345678912.123456789100000"),
            new BigDecimal("12345678912345678912.12346"),
            null,
            null,
            new BigDecimal("12345678912345678912.12346"))
        .go();
  }

  @Test
  public void readStringFieldSizeLimitText() throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test", "col1");
  }

  @Test
  public void readStringFieldSizeLimitORC()  throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test_orc", "col1");
  }

  @Test
  public void readVarcharFieldSizeLimitText()  throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test", "col2");
  }

  @Test
  public void readVarcharFieldSizeLimitORC()  throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test_orc", "col2");
  }

  @Test
  public void readBinaryFieldSizeLimitText()  throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test", "col3");
  }

  @Test
  public void readBinaryFieldSizeLimitORC()  throws Exception {
    readFieldSizeLimit("hive.field_size_limit_test_orc", "col3");
  }

  @Test
  public void readTimestampToStringORC() throws Exception {
    String query = "SELECT col1 FROM hive.timestamptostring_orc_ext order by col1 limit 1";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1")
      .baselineValues(Long.toString(new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime(), UTC).getMillis()))
      .go();
  }

  @Test
  public void readDoubleToStringORC() throws Exception {
    String query = "SELECT col1 FROM hive.doubletostring_orc_ext order by col1 limit 1";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1")
      .baselineValues("1.0")
      .go();
  }

  @Test
  public void readAllSupportedHiveDataTypesParquet() throws Exception {
    readAllSupportedHiveDataTypes("readtest_parquet");
  }

  @Test
  public void readMixedTypePartitionedTable() throws Exception {
    Assume.assumeFalse(runWithUnlimitedSplitSupport);
    String query = "SELECT * FROM hive.parquet_mixed_partition_type";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, 1)
      .baselineValues(2, 2)
      .baselineValues(3, 3)
      .go();

    query = "SELECT * FROM hive.parquet_mixed_partition_type_with_decimal";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(null, 1)
      .baselineValues(new BigDecimal("4.50"), 2)
      .baselineValues(null, 3)
      .baselineValues(new BigDecimal("3.40"), 4)
      .go();
  }

  @Test
  public void testLowUpperCasingForParquet() throws Exception {
    final String query = "SELECT * FROM hive.parquet_region";

    // Make sure the plan has Hive scan with native parquet reader
    testPhysicalPlan(query, "IcebergManifestList(table=[");

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
  }

  @Test
  public void testHiveParquetRefreshOnMissingFile() throws Exception {
    setEnableReAttempts(true);
    try {
      final String query = "SELECT count(r_regionkey) c FROM hive.parquet_with_two_files";

      // Make sure the plan has Hive scan with native parquet reader
      testPhysicalPlan(query, "IcebergManifestList(table=[");

      // With two files, the expected count is 10 (i.e 2 * 5).
      testBuilder().sqlQuery(query).ordered().baselineColumns("c").baselineValues(10L).go();

      // Move one file out of the dir and run again, the expected count is 5 now.
      File secondFile = new File(dataGenerator.getWhDir() + "/parquet_with_two_files/", "region2.parquet");
      File tmpPath = new File(dataGenerator.getWhDir(), "region2.parquet.tmp");
      secondFile.renameTo(tmpPath);

      try {
        testBuilder().sqlQuery(query).ordered().baselineColumns("c").baselineValues(5L).go();
      } finally{
        // Restore the file back to it's original place.
        tmpPath.renameTo(secondFile);
      }

    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testHiveOrcRefreshOnMissingFile() throws Exception {
    setEnableReAttempts(true);
    try {
      final String query = "SELECT count(r_regionkey) c FROM hive.orc_with_two_files";

      // With two files, the expected count is 10 (i.e 2 * 5).
      testBuilder().sqlQuery(query).ordered().baselineColumns("c").baselineValues(10L).go();

      // Move one file out of the dir and run again, the expected count is 5 now.
      File secondFile = new File(dataGenerator.getWhDir() + "/orc_with_two_files/", "region2.orc");
      File tmpPath = new File(dataGenerator.getWhDir(), "region2.orc.tmp");
      secondFile.renameTo(tmpPath);

      try {
        testBuilder().sqlQuery(query).ordered().baselineColumns("c").baselineValues(5L).go();
      } finally{
        // Restore the file back to it's original place.
        tmpPath.renameTo(secondFile);
      }

    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void orderByOnHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv ORDER BY \"value\" DESC")
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
    testPhysicalPlan("SELECT count(*) FROM hive.kv_parquet", "IcebergManifestList(table=[");

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
    dataGenerator.updatePluginConfig((getSabotContext().getCatalogService()),
        ImmutableMap.of(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9001"));

    testBuilder()
        .sqlQuery("SELECT * FROM hive.\"default\".kv LIMIT 1")
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
        .sqlQuery("SELECT key, \"value\", newcol FROM hive.kv_parquet ORDER BY key LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value", "newcol")
        .baselineValues(1, " key_1", null)
        .go();
  }

  @Test // DRILL-3938
  public void nativeReaderIsDisabledForAlteredPartitionedTable() throws Exception {
    final String query = "EXPLAIN PLAN FOR SELECT key, \"value\", newcol FROM hive.kv_parquet ORDER BY key LIMIT 1";

    // Make sure the HiveScan in plan has no native parquet reader
    final String planStr = getPlanInString(query, OPTIQ_FORMAT);
    assertFalse("Hive native is not expected in the plan", planStr.contains("hive-native-parquet-scan"));
  }

  @Test
  public void readFromMixedSchema() throws Exception {
    testBuilder()
        .sqlQuery("SELECT key, \"value\" FROM hive.kv_mixedschema")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues("1", " key_1")
        .baselineValues("2", " key_2")
        .baselineValues("5", " key_5")
        .baselineValues("4", " key_4")
        .go();
  }

  @Test
  public void testParquetLearnSchema2() {
    String query = "SELECT col1 FROM hive.parqschematest_table";
    errorMsgTestHelper(query, "Field [col1] has incompatible types in file and table.");
  }

  @Test
  public void testParquetLearnSchema() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * FROM hive.parquetschemalearntest")
      .unOrdered()
      .sqlBaselineQuery("select r_regionkey from hive.parquetschemalearntest")
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
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv_sh ORDER BY key LIMIT 2")
        .ordered()
        .baselineColumns("key", "value")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DRILL-3688
  public void readingFromSmallTableWithSkipHeaderAndFooter() throws Exception {
   testBuilder()
        .sqlQuery("select key, \"value\" from hive.skipper.kv_text_small order by key asc")
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
      assertThatThrownBy(() -> test(String.format(query, entry.getKey())))
        .hasMessageContaining(String.format(exceptionMessage, entry.getValue()));
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
  public void testQueryNonExistingTable() {
    errorMsgTestHelper("SELECT * FROM hive.nonExistedTable", "'nonExistedTable' not found within 'hive'");
    errorMsgTestHelper("SELECT * FROM hive.\"default\".nonExistedTable", "'nonExistedTable' not found within 'hive.default'");
    errorMsgTestHelper("SELECT * FROM hive.db1.nonExistedTable", "'nonExistedTable' not found within 'hive.db1'");
  }

  @Test
  public void testReadSignatures() throws Exception {
    Assume.assumeFalse(runWithUnlimitedSplitSupport);
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.db1.kv_db1")))).size());
    assertEquals(1, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.db1.avro")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".dummy")))).size());
    assertEquals(1, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.skipper.kv_parquet_large")))).size());

    assertEquals(3, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".readtest")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".readtest_parquet")))).size());

    assertEquals(5, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".kv_parquet")))).size());
    assertEquals(54, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".partition_with_few_schemas")))).size());
    assertEquals(56, getCachedEntities(ns.getDataset(new NamespaceKey(parseFullPath("hive.\"default\".partition_pruning_test")))).size());
  }

  @Test
  public void testCheckReadSignatureValid() throws Exception {
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "db1", "kv_db1")), SupportsReadSignature.MetadataValidity.VALID);
    testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "default", "partition_with_few_schemas")), SupportsReadSignature.MetadataValidity.VALID);
  }

  @Test
  public void testCheckReadSignatureInvalid() throws Exception {
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    new File(dataGenerator.getWhDir() + "/db1.db/kv_db1", "000000_0").setLastModified(System.currentTimeMillis());

    File newFile = new File(dataGenerator.getWhDir() + "/partition_with_few_schemas/c=1/d=1/e=1/", "empty_file");
    try {
      newFile.createNewFile();

      testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "db1", "kv_db1")), SupportsReadSignature.MetadataValidity.INVALID);
      testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "default", "partition_with_few_schemas")), SupportsReadSignature.MetadataValidity.INVALID);
    } finally {
      newFile.delete();
    }
  }

  @Test
  public void testCheckHasPermission() throws Exception {
    // TODO: enable back for MapR once DX-21902 is fixed
    assumeNonMaprProfile();

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);

    NamespaceKey dataset = new NamespaceKey(parseFullPath("hive.db1.kv_db1"));
    DatasetConfig datasetConfig = ns.getDataset(dataset);
    assertTrue(getSabotContext().getCatalogService().getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));

    final Path tableFile = new Path(dataGenerator.getWhDir() + "/db1.db/kv_db1/000000_0");
    final Path tableDir = new Path(dataGenerator.getWhDir() + "/db1.db/kv_db1");
    final FileSystem localFs = FileSystem.getLocal(new Configuration());

    try {
      // no read on file
      localFs.setPermission(tableFile, new FsPermission(FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE));
      assertFalse(getSabotContext().getCatalogService().getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
    } finally {
      localFs.setPermission(tableFile, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }

    try {
      // no exec on dir
      localFs.setPermission(tableDir, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
      assertFalse(getSabotContext().getCatalogService().getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
    } finally {
      localFs.setPermission(tableDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  @Test
  public void testAddRemoveHiveTable() throws Exception {
    List<NamespaceKey> tables0 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0L)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);

    List<NamespaceKey> tables1 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables0.size(), tables1.size());

    // create an empty table
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS foo_bar(a INT, b STRING)");

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0L)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);

    // make sure new table is visible
    List<NamespaceKey> tables2 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables1.size() + 1, tables2.size());

    assertTrue(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.\"default\".foo_bar")), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.foo_bar")), Type.DATASET));

    // run query on table with short name
    testBuilder()
      .sqlQuery("SELECT * FROM hive.foo_bar")
      .expectsEmptyResultSet()
      .go();

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.foo_bar")), Type.DATASET));

    // no new table is added
    List<NamespaceKey> tables3 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables2.size(), tables3.size());

    // drop table
    dataGenerator.executeDDL("DROP TABLE foo_bar");

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
      .setAuthTtlMs(0L)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setDatasetDefinitionTtlMs(0L)
      .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);

    // make sure table is deleted from namespace
    List<NamespaceKey> tables4 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables3.size() - 1, tables4.size());

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.\"default\".foo_bar")), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.foo_bar")), Type.DATASET));
  }

  @Test
  public void testShortNamesInAlterTableSet() throws Exception {
    final String tableName = "foo_bar2";
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);
    List<NamespaceKey> tables1 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));

    // create an empty table
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS " + tableName + "(a INT, b STRING)");

    // verify that the new table is visible
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);
    List<NamespaceKey> tables2 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables1.size() + 1, tables2.size());

    // verify that the newly created table is under default namespace
    assertTrue(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.\"default\"." + tableName)), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive." + tableName)), Type.DATASET));

    // execute queries & verify responses
    testBuilder()
      .sqlQuery("ALTER TABLE hive.\"default\"." + tableName + " SET enable_varchar_truncation = true")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table [hive.\"default\"." + tableName + "] options updated")
      .build().run();

    testBuilder()
      .sqlQuery("ALTER TABLE hive." + tableName + " SET enable_varchar_truncation = true")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table [hive." + tableName + "] options did not change")
      .build().run();

    testBuilder()
      .sqlQuery("ALTER TABLE hive." + tableName + " SET enable_varchar_truncation = false")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table [hive." + tableName + "] options updated")
      .build().run();

    // drop table
    dataGenerator.executeDDL("DROP TABLE " + tableName);

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);

    // check if table is deleted from namespace
    List<NamespaceKey> tables3 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables1.size(), tables3.size());
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.\"default\"." + tableName)), Type.DATASET));
  }

  @Test
  public void testNonDefaultHiveTableRefresh() throws Exception {
    String tableName = "test_non_default_refresh";
    List<NamespaceKey> tables0 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));

    List<NamespaceKey> tables1 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables0.size(), tables1.size());

    // create an empty table
    dataGenerator.executeDDL("CREATE TABLE IF NOT EXISTS " + tableName + "(a INT, b STRING)");

    //run alter table refresh with short name to make the table visible
    testBuilder()
      .sqlQuery("ALTER TABLE hive." + tableName + " REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'hive." + tableName + "' refreshed.")
      .build().run();

    // make sure new table is visible
    List<NamespaceKey> tables2 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables1.size() + 1, tables2.size());

    assertTrue(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive.\"default\"." + tableName)), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive." + tableName)), Type.DATASET));

    // run query on table with short name
    testBuilder()
      .sqlQuery("SELECT * FROM hive." + tableName)
      .expectsEmptyResultSet()
      .go();

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive." + tableName)), Type.DATASET));

    // no new table is added
    List<NamespaceKey> tables3 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables2.size(), tables3.size());

    //run alter again - should succeed. The short name should get resolved and recoginzed as already existing and no refresh should be done
    testBuilder()
      .sqlQuery("ALTER TABLE hive." + tableName + " REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'hive." + tableName + "' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(parseFullPath("hive." + tableName)), Type.DATASET));
    // no new table is added
    tables3 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables2.size(), tables3.size());

    // drop table
    dataGenerator.executeDDL("DROP TABLE " + tableName);
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0L)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0L)
        .setNamesRefreshMs(0L), CatalogServiceImpl.UpdateType.FULL);

    // make sure table is deleted from namespace
    List<NamespaceKey> tables4 = Lists.newArrayList(getSabotContext()
      .getNamespaceService(SystemUser.SYSTEM_USERNAME)
      .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables3.size() - 1, tables4.size());
  }

  @Test
  public void testTextAndOrcTableWithDateColumn() throws Exception {
    LocalDateTime dateTime1 = LocalDateTime.parse("0001-01-01");
    LocalDateTime dateTime2 = LocalDateTime.parse("1299-01-01");
    LocalDateTime dateTime3 = LocalDateTime.parse("1499-01-01");
    LocalDateTime dateTime4 = LocalDateTime.parse("1582-01-01");
    LocalDateTime dateTime5 = LocalDateTime.parse("1699-01-01");

    String textQuery = "SELECT date_col from hive.text_date";
    String orcQuery = "SELECT date_col from hive.orc_date";

    // execute query for Hive text table
    testBuilder()
      .sqlQuery(textQuery)
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime1)
      .baselineValues(dateTime2)
      .baselineValues(dateTime3)
      .baselineValues(dateTime4)
      .baselineValues(dateTime5)
      .go();

    // execute query for Hive non-vectorized ORC table
    final String hiveOrcVectorize = "store.hive.orc.vectorize";
    final String hive3OrcVectorize = "store.hive3.orc.vectorize";
    try {
      runSQL(getSystemOptionQueryString(hiveOrcVectorize, Boolean.FALSE.toString()));
    } catch (Exception e) {
      runSQL(getSystemOptionQueryString(hive3OrcVectorize, Boolean.FALSE.toString()));
    }
    try {
      testBuilder()
        .sqlQuery(orcQuery)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(dateTime1)
        .baselineValues(dateTime2)
        .baselineValues(dateTime3)
        .baselineValues(dateTime4)
        .baselineValues(dateTime5)
        .go();
    } finally {
      try {
        runSQL(getSystemOptionQueryString(hiveOrcVectorize, Boolean.TRUE.toString()));
      } catch (Exception e) {
        runSQL(getSystemOptionQueryString(hive3OrcVectorize, Boolean.TRUE.toString()));
      }
    }
  }

  @Test // DX-23234
  public void testOrcTableWithAncientDates() throws Exception {
    LocalDateTime dateTime1 = LocalDateTime.parse("0001-01-01");
    LocalDateTime dateTime2 = LocalDateTime.parse("0110-05-12");
    LocalDateTime dateTime3 = LocalDateTime.parse("1105-10-06");
    LocalDateTime dateTime4 = LocalDateTime.parse("1301-01-01");
    LocalDateTime dateTime5 = LocalDateTime.parse("1476-05-31");
    LocalDateTime dateTime6 = LocalDateTime.parse("1582-10-01");
    LocalDateTime dateTime7 = LocalDateTime.parse("1790-07-17");
    LocalDateTime dateTime8 = LocalDateTime.parse("2015-01-01");

    testBuilder()
      .sqlQuery("SELECT date_col FROM hive.orc_date_table")
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime1)
      .baselineValues(dateTime2)
      .baselineValues(dateTime3)
      .baselineValues(dateTime4)
      .baselineValues(dateTime5)
      .baselineValues(dateTime6)
      .baselineValues(dateTime7)
      .baselineValues(dateTime8)
      .go();

    testBuilder()
      .sqlQuery("SELECT date_col FROM hive.orc_date_table WHERE date_col = '1301-01-01'")
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime4)
      .go();

    testBuilder()
      .sqlQuery("SELECT date_col FROM hive.orc_date_table WHERE date_col <= '1105-10-06'")
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime1)
      .baselineValues(dateTime2)
      .baselineValues(dateTime3)
      .go();

    testBuilder()
      .sqlQuery("SELECT date_col FROM hive.orc_date_table WHERE date_col > '1105-10-06'")
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime4)
      .baselineValues(dateTime5)
      .baselineValues(dateTime6)
      .baselineValues(dateTime7)
      .baselineValues(dateTime8)
      .go();

    testBuilder()
      .sqlQuery("SELECT date_col FROM hive.orc_date_table WHERE date_col = '2015-01-01'")
      .unOrdered()
      .baselineColumns("date_col")
      .baselineValues(dateTime8)
      .go();
  }

  @Test // DX-11011
  public void parquetSkipAllMultipleRowGroups() throws Exception {
    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hive.parquet_mult_rowgroups")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(128L)
        .go();
  }

  @Test
  public void readImpalaParquetFile() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM hive.db1.impala_parquet")
        .baselineColumns("id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col",
            "double_col", "date_string_col", "string_col", "timestamp_col"
        )
        .baselineValues(0, true, 0, 0, 0, 0L, 0.0F, 0.0D, "01/01/09", "0", new LocalDateTime(1230768000000L, UTC))
        .baselineValues(1, false, 1, 1, 1, 10L, 1.1F, 10.1D, "01/01/09", "1", new LocalDateTime(1230768060000L, UTC))
        .go();
  }

  @Test
  public void orcVectorizedTest() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * from hive.orc_region")
        .unOrdered()
        .sqlBaselineQuery("SELECT * FROM hive.parquet_region")
        .go();

    // project only few columns
    testBuilder()
        .sqlQuery("SELECT r_comment, r_regionkey from hive.orc_region")
        .unOrdered()
        .sqlBaselineQuery("SELECT r_comment, r_regionkey FROM hive.parquet_region")
        .go();
  }

  @Test
  public void testParquetDecimalUnion() throws Exception {
    String query = "SELECT col1 FROM hive.parqdecunion_table";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(new BigDecimal("123"))
      .baselineValues(new BigDecimal("-543"))
      .go();
  }

  @Test
  public void testORCDecimalCompareWithIntegerLiteral() throws Exception {
    // float column
    testBuilder()
      .sqlQuery("SELECT col1 from hive.orcdecimalcompare where col1 < 0")
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(-0.1F)
      .go();

    //double column
    testBuilder()
      .sqlQuery("SELECT col2 from hive.orcdecimalcompare where col2 < 0")
      .unOrdered()
      .baselineColumns("col2")
      .baselineValues(-0.1D)
      .go();

    //decimal column
    testBuilder()
      .sqlQuery("SELECT col3 from hive.orcdecimalcompare where col3 < 0")
      .unOrdered()
      .baselineColumns("col3")
      .baselineValues(new BigDecimal("-0.1"))
      .go();
  }

  @Test
  public void testPartitionValueFormatException() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * from hive.partition_format_exception_orc")
      .unOrdered()
      .sqlBaselineQuery("SELECT col1, col2 from hive.partition_format_exception_orc")
      .go();

    testBuilder()
      .sqlQuery("SELECT * from hive.partition_format_exception_orc where col1 = 1")
      .unOrdered()
      .sqlBaselineQuery("SELECT * from hive.partition_format_exception_orc where col2 is null")
      .go();
  }

  @Test
  public void testStructWithNullsORC() throws Exception {
    testBuilder()
      .sqlQuery("SELECT * from hive.orcnullstruct")
      .unOrdered()
      .sqlBaselineQuery("SELECT id, emp_name, city, prm_borr from hive.orcnullstruct")
      .go();
  }

  @Test
  public void testEmptyDoubleFieldORC() throws Exception {
    testBuilder()
      .sqlQuery("SELECT itm_array[0].price_in_usd as price FROM hive.empty_float_field limit 1")
      .unOrdered()
      .baselineColumns("price")
      .baselineValues(null)
      .go();
  }

  @Test
  public void testHiveFlattenORC() throws Exception {
    String aaa = "aaa 1", ccc = "ccc 1";
    JsonStringHashMap<String, Object> bb = new JsonStringHashMap<>();
    Double f1 = 1.6789, f2 = 54331.0;
    Long in_col = Long.valueOf(1);

    testBuilder()
      .sqlQuery("select t.flatten_list.fl.f1, t.flatten_list.fl.f2 from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .baselineColumns("f1","f2")
      .baselineValues(null, null)
      .baselineValues(f1, f2)
      .baselineValues(null, null)
      .go();

    testBuilder()
      .sqlQuery("select t.flatten_list.in_col from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .baselineColumns("in_col")
      .baselineValues(in_col)
      .baselineValues(in_col)
      .baselineValues(null)
      .go();

    testBuilder()
      .sqlQuery("select t.flatten_list.a.aa.aaa from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .baselineColumns("aaa")
      .baselineValues(null)
      .baselineValues(null)
      .baselineValues(aaa)
      .go();

    testBuilder()
      .sqlQuery("select t.flatten_list.b.bb from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .baselineColumns("bb")
      .baselineValues(null)
      .baselineValues(null)
      .baselineValues(bb)
      .go();

    testBuilder()
      .sqlQuery("select t.flatten_list.c.cc.ccc from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .baselineColumns("ccc")
      .baselineValues(null)
      .baselineValues(null)
      .baselineValues(ccc)
      .go();

    testBuilder()
      .sqlQuery("select t.flatten_list from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .sqlBaselineQuery("select t.flatten_list from (select flatten(ooa) flatten_list from hive.flatten_parquet) t")
      .build()
      .run();

    testBuilder()
      .sqlQuery("select t.flatten_list.in_col, t.flatten_list.fl, t.flatten_list.a, t.flatten_list.b, t.flatten_list.c  from (select flatten(ooa) flatten_list from hive.flatten_orc) t")
      .unOrdered()
      .sqlBaselineQuery("select t.flatten_list.in_col, t.flatten_list.fl, t.flatten_list.a, t.flatten_list.b, t.flatten_list.c from (select flatten(ooa) flatten_list from hive.flatten_parquet) t")
      .build()
      .run();
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

  private void testCheckReadSignature(EntityPath datasetPath, SupportsReadSignature.MetadataValidity expectedResult) throws Exception {
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);

    DatasetConfig datasetConfig = ns.getDataset(new NamespaceKey(datasetPath.getComponents()));

    BytesOutput signature = os -> os.write(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
    DatasetHandle datasetHandle = (getSabotContext().getCatalogService().getSource("hive")).getDatasetHandle(datasetPath).get();
    DatasetMetadata metadata = new DatasetMetadataAdapter(datasetConfig);

    assertEquals(expectedResult, ((SupportsReadSignature) getSabotContext().getCatalogService().getSource("hive")).validateMetadata(
        signature, datasetHandle, metadata));
  }

  private void readFieldSizeLimit(String table, String column) throws Exception {
    String exceptionMessage = "UNSUPPORTED_OPERATION ERROR: Field exceeds the size limit of 32000 bytes, actual size is 32001 bytes.";
    String query = "SELECT " + column + " FROM " + table;
    assertThatThrownBy(() -> testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns(column)
        .baselineValues("")
        .go())
      .hasMessageContaining(exceptionMessage);
  }

  /**
   * Test to ensure Dremio reads the all supported types correctly both normal fields (converted to Nullable types) and
   * partition fields (converted to Required types).
   * @throws Exception
   */
  private void readAllSupportedHiveDataTypes(String table) throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive." + table)
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
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime(), UTC),
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

  private void testVersionPlan(String query) throws Exception {
      testPhysicalPlan(query, "IcebergManifestList(table=[");
  }
}
