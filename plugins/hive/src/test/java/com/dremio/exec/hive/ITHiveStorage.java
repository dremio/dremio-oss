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
import static org.hamcrest.CoreMatchers.containsString;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.DatasetMetadataAdapter;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.ImpersonationUtil;
import com.dremio.exec.store.hive.exec.HiveDatasetOptions;
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

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  @BeforeClass
  public static void setupOptions() throws Exception {
    test(String.format("alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }


  @Test // DRILL-4083
  public void testNativeScanWhenNoColumnIsRead() throws Exception {
    String query = "SELECT count(*) as col FROM hive.kv_parquet";
    testPhysicalPlan(query, "mode=[NATIVE_PARQUET");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col")
        .baselineValues(5L)
        .go();
  }

  @Test
  public void testTimestampNulls() throws Exception {
    String query = "SELECT * FROM hive.parquet_timestamp_nulls";
    test(query);
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
  public void readAllSupportedHiveDataTypesORC() throws Exception {
    readAllSupportedHiveDataTypes("readtest_orc");
  }

  @Test
  public void orcTestTinyIntToString() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.tinyint_to_string_orc_ext")
      .ordered()
      .baselineColumns("col1")
      .baselineValues(new StringBuilder().append("90").toString())
      .go();
  }

  @Test
  public void orcTestTinyIntToBigInt() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.tinyint_to_bigint_orc_ext")
      .ordered()
      .baselineColumns("col1")
      .baselineValues(new Long(90))
      .go();
  }

  @Test
  public void orcTestMoreColumnsInExtTable() throws Exception {
    String query = "SELECT col2, col3 FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col2", "col3")
      .baselineValues(new Integer(2), null)
      .go();
    String query2 = "SELECT col3, col2 FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query2)
      .ordered()
      .baselineColumns("col3", "col2")
      .baselineValues(null, new Integer(2))
      .go();
    String query3 = "SELECT * FROM hive.orc_more_columns_ext";
    testBuilder().sqlQuery(query3)
      .ordered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(new Integer(1), new Integer(2), null, null)
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
    errorMsgTestHelper(query, "Field [col2] has incompatible types in file and table.");

    // all conversions are valid
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_decimal";
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

    // some overflow conversions returned as null
    query = "SELECT * FROM hive.decimal_conversion_test_parquet_decimal_ext";
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
  public void testParquetHiveFixedLenVarchar() throws Exception {
    String setOptionQuery = setTableOptionQuery("hive.\"default\".parq_varchar", HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true");
    testBuilder().sqlQuery(setOptionQuery)
                 .unOrdered()
                 .baselineColumns("ok", "summary")
                 .baselineValues(true, "Table [hive.\"default\".parq_varchar] options updated")
                 .go();

    String query = "SELECT R_NAME FROM hive.parq_varchar";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("R_NAME")
      .baselineValues("AFRICA")
      .baselineValues("AMERIC")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .baselineValues("MIDDLE")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parq_varchar_no_trunc", HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT R_NAME FROM hive.parq_varchar_no_trunc";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("R_NAME")
      .baselineValues("AFRICA")
      .baselineValues("AMERICA")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .baselineValues("MIDDLE EAST")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parq_char", HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT R_NAME FROM hive.parq_char";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("R_NAME")
      .baselineValues("AFRICA")
      .baselineValues("AMERIC")
      .baselineValues("ASIA")
      .baselineValues("EUROPE")
      .baselineValues("MIDDLE")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parq_varchar_more_types_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT Country, Capital, Lang FROM hive.parq_varchar_more_types_ext";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("Country", "Capital", "Lang")
      .baselineValues("United Kingdom", "London", "Eng")
      .go();

    testBuilder().sqlQuery(setTableOptionQuery("hive.\"default\".parq_varchar_more_types_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"))
                 .unOrdered()
                 .baselineColumns("ok", "summary")
                 .baselineValues(true, "Table [hive.\"default\".parq_varchar_more_types_ext] options did not change")
                 .go();

    query = "SELECT A, B, C FROM hive.parq_varchar_more_types_ext";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("A", "B", "C")
      .baselineValues(1, 2, 3)
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parq_varchar_complex_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT Country, Capital FROM hive.parq_varchar_complex_ext";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("Country", "Capital")
      .baselineValues("Uni", "Lon")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parquet_fixed_length_varchar_partition_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT * FROM hive.parquet_fixed_length_varchar_partition_ext";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(100, "abcd")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parquet_fixed_length_varchar_partition_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "false"));
    query = "SELECT * FROM hive.parquet_fixed_length_varchar_partition_ext";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(100, "abcdefgh")
      .go();

    runSQL(setTableOptionQuery("hive.\"default\".parquet_fixed_length_char_partition_ext",
        HiveDatasetOptions.HIVE_PARQUET_ENFORCE_VARCHAR_WIDTH, "true"));
    query = "SELECT * FROM hive.parquet_fixed_length_char_partition_ext";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(100, "abcd")
      .go();
  }

  @Test
  public void orcTestTimestampMilli() throws Exception {
    String query = "SELECT col1 FROM hive." + "timestamp" + "_orc";
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns("col1")
      .baselineValues(new LocalDateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC))
      .go();
  }

  @Test
  public void orcTestTypeConversions() throws Exception {
    Object[][] testcases = {
      //tinyint
      {"tinyint", "smallint", new Integer(90), "90"},
      {"tinyint", "int", new Integer(90), "90"},
      {"tinyint", "bigint", new Long(90), "90"},
      {"tinyint", "float", new Float(90), "90.0"},
      {"tinyint", "double", new Double(90), "90.0"},
      {"tinyint", "decimal", new BigDecimal(90), "90.0"},
      {"tinyint", "string", "90"},
      {"tinyint", "varchar", "90"},
      //smallint
      {"smallint", "int", new Integer(90), "90"},
      {"smallint", "bigint", new Long(90), "90"},
      {"smallint", "float", new Float(90), "90.0"},
      {"smallint", "double", new Double(90), "90.0"},
      {"smallint", "decimal", new BigDecimal(90), "90.0"},
      {"smallint", "string", "90"},
      {"smallint", "varchar", "90"},
      //int
      {"int", "bigint", new Long(90), "90"},
      {"int", "float", new Float(90), "90.0"},
      {"int", "double", new Double(90), "90.0"},
      {"int", "decimal", new BigDecimal(90), "90.0"},
      {"int", "string", "90"},
      {"int", "varchar", "90"},
      //bigint
      {"bigint", "float", new Float(90), "90.0"},
      {"bigint", "double", new Double(90), "90.0"},
      {"bigint", "decimal", new BigDecimal(90), "90.0"},
      {"bigint", "string", "90"},
      {"bigint", "varchar", "90"},
      //float
      {"float", "double", new Double(90), "90.0"},
      {"float", "decimal", new BigDecimal(90), "90.0"},
      {"float", "string", "90.0"},
      {"float", "varchar", "90.0"},
      //double
      {"double", "decimal", new BigDecimal(90), "90.0"},
      {"double", "string", "90.0"},
      {"double", "varchar", "90.0"},
      //decimal
      {"decimal", "string", "90"},
      {"decimal", "varchar", "90"},
      //string
      {"string", "double", new Double(90), "90.0"},
      {"string", "decimal", new BigDecimal(90), "90.0"},
      {"string", "varchar", "90"},
      //varchar
      {"varchar", "double", new Double(90), "90.0"},
      {"varchar", "decimal", new BigDecimal(90), "90.0"},
      {"varchar", "string", "90"},
      //timestamp
      {"timestamp", "string", Long.toString(new DateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC).getMillis())},
      {"timestamp", "varchar", Long.toString(new DateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC).getMillis())},
      //date
      {"date", "string", "17969"},
      {"date", "varchar", "17969"}
    };
    hiveTestTypeConversions(testcases,"orc");
  }

  @Test
  public void parquetTestTypeConversions() throws Exception {
    Object[][] testcases = {
        //tinyint
        {"tinyint", "smallint", new Integer(90), "90"},
        {"tinyint", "int", new Integer(90), "90"},
        {"tinyint", "bigint", new Long(90), "90"},
        //smallint
        {"smallint", "int", new Integer(90), "90"},
        {"smallint", "bigint", new Long(90), "90"},
        //int
        {"int", "bigint", new Long(90), "90"},
        //float
        {"float", "double", new Double(90), "90.0"},
        //string
        {"string", "varchar", "90", "'90'"},
        //varchar
        {"varchar", "string", "90", "'90'"},
    };
    hiveTestTypeConversions(testcases, "parquet");
  }

  private void hiveTestTypeConversions(Object[][] testcases, String tableFormat) throws Exception {
    for (Object[] testcase : testcases) {
      String query = "SELECT * FROM hive." + testcase[0] + "_to_" + testcase[1] + "_" + tableFormat + "_ext";
      testBuilder().sqlQuery(query)
          .ordered()
          .baselineColumns("col1")
          .baselineValues(testcase[2])
          .go();
    }

    for (Object[] testcase : testcases) {
      if (testcase.length == 4) {
        String query = "SELECT * FROM hive." + testcase[0] + "_to_" + testcase[1] + "_" + tableFormat + "_ext where col1 = " + testcase[3];
        testBuilder().sqlQuery(query)
            .ordered()
            .baselineColumns("col1")
            .baselineValues(testcase[2])
            .go();
      }
    }
  }

  @Test
  public void parquetTestIncompatibleTypeConversions() throws Exception {
    Object[][] testcases = {
        //tinyint
        {"tinyint", "float", new Float(90), "90.0"},
        {"tinyint", "double", new Double(90), "90.0"},
        {"tinyint", "decimal", new BigDecimal(90), "90.0"},
        //smallint
        {"smallint", "float", new Float(90), "90.0"},
        {"smallint", "double", new Double(90), "90.0"},
        {"smallint", "decimal", new BigDecimal(90), "90.0"},
        //int
        {"int", "float", new Float(90), "90.0"},
        {"int", "double", new Double(90), "90.0"},
        {"int", "decimal", new BigDecimal(90), "90.0"},
        //bigint
        {"bigint", "float", new Float(90), "90.0"},
        {"bigint", "double", new Double(90), "90.0"},
        {"bigint", "decimal", new BigDecimal(90), "90.0"},
        //float
        {"float", "decimal", new BigDecimal(90), "90.0"},
        //double
        {"double", "decimal", new BigDecimal(90), "90.0"},
        //string
        {"string", "double", new Double(90)},
        {"string", "decimal", new BigDecimal(90)},
        //varchar
        {"varchar", "double", new Double(90)},
        {"varchar", "decimal", new BigDecimal(90)},
        //decimal
        {"decimal", "string", "90"},
        {"decimal", "varchar", "90"}
    };
    hiveTestIncompatibleTypeConversions(testcases, "parquet");
  }

  private void hiveTestIncompatibleTypeConversions(Object[][] testcases, String tableFormat) throws Exception {
    for (Object[] testcase : testcases) {
      String query = "SELECT * FROM hive." + testcase[0] + "_to_" + testcase[1] + "_" + tableFormat + "_ext";

      errorMsgTestHelper(query, "Field [col1] has incompatible types in file and table.");
    }
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

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

  private void readFieldSizeLimit(String table, String column) throws Exception {
    String exceptionMessage = "UNSUPPORTED_OPERATION ERROR: Field exceeds the size limit of 32000 bytes.";
    exception.expect(java.lang.Exception.class);
    exception.expectMessage(exceptionMessage);
    String query = "SELECT " + column + " FROM " + table;
    testBuilder().sqlQuery(query)
      .ordered()
      .baselineColumns(column)
      .baselineValues("")
      .go();
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
  public void readComplexHiveDataTypesORC() throws Exception {
    readComplexHiveDataTypes("orccomplexorc");
  }

  // DX-16748: dropping support for map data type in ORC
  @Ignore
  public void readMapValuesTest() throws Exception {
    readMapValues("orcmaporc");
  }

  @Test
  public void readListHiveDataTypesORC() throws Exception {
    readListHiveDataTypes("orclistorc");
  }

  @Test
  public void readStructHiveDataTypesORC() throws Exception {
    readStructHiveDataTypes("orcstructorc");
  }

  @Test
  public void readUnionHiveDataTypesORC() throws Exception {
    readUnionHiveDataTypes("orcunionorc");
  }

  @Test
  public void readAllSupportedHiveDataTypesParquet() throws Exception {
    readAllSupportedHiveDataTypes("readtest_parquet");
  }

  @Test
  public void readMixedTypePartitionedTable() throws Exception {
    String query = "SELECT * FROM hive.parquet_mixed_partition_type";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(new Integer(1), new Integer(1))
      .baselineValues(new Integer(2), new Integer(2))
      .baselineValues(new Integer(3), new Integer(3))
      .go();

    query = "SELECT * FROM hive.parquet_mixed_partition_type_with_decimal";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(null, new Integer(1))
      .baselineValues(new BigDecimal("4.50"), new Integer(2))
      .baselineValues(null, new Integer(3))
      .baselineValues(new BigDecimal("3.40"), new Integer(4))
      .go();
  }

  /**
   * Test to ensure Dremio reads union data from hive
   * @throws Exception
   */
  private void readUnionHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int testcase = 0; testcase < testrows.length; ++testcase) {
      Integer row = new Integer(testrows[testcase]);
      Object expected = null;
      String rowStr = row.toString();

      if ( row % 3 == 0 ) {
        expected = new Integer(row);
      } else if ( row % 3 == 1 ) {
        expected = Double.parseDouble(rowStr + "." + rowStr);
      } else {
        expected = rowStr + "." + rowStr;
      }
      testBuilder().sqlQuery("SELECT * FROM hive." + table +
        " order by rownum limit 1 offset " + row.toString())
        .ordered()
        .baselineColumns("rownum", "union_field")
        .baselineValues(new Integer(row), expected)
        .go();
    }
  }

  /**
   * Test to ensure Dremio reads list of primitive data types
   * @throws Exception
   */
  private void readStructHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int i = 0; i < testrows.length; ++i) {
      Integer index = new Integer(testrows[i]);
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<String, Object>();
      structrow1.put("tinyint_field", new Integer(1));
      structrow1.put("smallint_field", new Integer(1024));
      structrow1.put("int_field", new Integer(testrows[i]));
      structrow1.put("bigint_field", new Long(90000000000L));
      structrow1.put("float_field", new Float(testrows[i]));
      structrow1.put("double_field", new Double(testrows[i]));
      structrow1.put("string_field", new Text(Integer.toString(testrows[i])));


      testBuilder().sqlQuery("SELECT * FROM hive." + table +
        " order by rownum limit 1 offset " + index.toString())
        .ordered()
        .baselineColumns("rownum", "struct_field")
        .baselineValues(new Integer(index), structrow1)
        .go();

      testBuilder().sqlQuery("SELECT rownum, struct_field['string_field'] AS string_field, struct_field['int_field'] AS int_field FROM hive." + table +
        " order by rownum limit 1 offset " + index.toString())
        .ordered()
        .baselineColumns("rownum", "string_field", "int_field")
        .baselineValues(new Integer(index), Integer.toString(testrows[i]), new Integer(index))
        .go();
    }
  }

  /**
   * Test to ensure Dremio reads list of primitive data types
   * @throws Exception
   */
  private void readListHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int i = 0; i < testrows.length; ++i) {
      if ( testrows[i] % 7 == 0 ) {
        Integer index = new Integer(testrows[i]);
        testBuilder().sqlQuery("SELECT rownum,double_field,string_field FROM hive." + table +
          " order by rownum limit 1 offset " + index.toString())
          .ordered()
          .baselineColumns("rownum", "double_field", "string_field")
          .baselineValues(new Integer(index), null,
            null)
          .go();
      } else {
        Integer index = new Integer(testrows[i]);
        JsonStringArrayList<Text> string_field = new JsonStringArrayList<>();
        string_field.add(new Text(Integer.toString(index)));
        string_field.add(new Text(Integer.toString(index + 1)));
        string_field.add(new Text(Integer.toString(index + 2)));
        string_field.add(new Text(Integer.toString(index + 3)));
        string_field.add(new Text(Integer.toString(index + 4)));


        testBuilder().sqlQuery("SELECT rownum,double_field,string_field FROM hive." + table +
          " order by rownum limit 1 offset " + index.toString())
          .ordered()
          .baselineColumns("rownum", "double_field", "string_field")
          .baselineValues(new Integer(index), new ArrayList<Double>(Arrays.asList(new Double(index),
            new Double(index + 1), new Double(index + 2),
            new Double(index + 3), new Double(index + 4))),
            string_field)
          .go();
      }
    }
  }

  /**
   * Test to ensure Dremio reads the all ORC complex types correctly
   * @throws Exception
   */
  private void readComplexHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for(int i=0; i < testrows.length; ++i) {
      Integer index = new Integer(testrows[i]);
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<String, Object>();
      structrow1.put("name", new Text("name" + index.toString()));
      structrow1.put("age", new Integer(index));

      JsonStringHashMap<String, Object> structlistrow1 = new JsonStringHashMap<String, Object>();
      structlistrow1.put("type", new Text("type" + index.toString()));
      structlistrow1.put("value", new ArrayList<Text>(Arrays.asList(new Text("elem" + index.toString()))));

      JsonStringHashMap<String, Object> liststruct1 = new JsonStringHashMap<String, Object>();
      liststruct1.put("name", new Text("name" + Integer.toString(index)));
      liststruct1.put("age", new Integer(index));
      JsonStringHashMap<String, Object> liststruct2 = new JsonStringHashMap<String, Object>();
      liststruct2.put("name", new Text("name" + Integer.toString(index+1)));
      liststruct2.put("age", new Integer(index+1));

      JsonStringHashMap<String, Object> map1 = new JsonStringHashMap<String, Object>();
      map1.put("key", new Text("name" + Integer.toString(index)));
      map1.put("value", new Integer(index));
      JsonStringHashMap<String, Object> map2 = new JsonStringHashMap<String, Object>();
      map2.put("key", new Text("name" + Integer.toString(index+1)));
      map2.put("value", new Integer(index+1));
      JsonStringHashMap<String, Object> map3 = new JsonStringHashMap<String, Object>();
      map3.put("key", new Text("name" + Integer.toString(index+2)));
      map3.put("value", new Integer(index+2));

      JsonStringArrayList<JsonStringHashMap> mapValue = new JsonStringArrayList<>();
      mapValue.add(map1);
      mapValue.add(map2);
      if(index % 2 == 0) {
        mapValue.add(map3);
      }

      JsonStringHashMap<String, Object> mapstruct = new JsonStringHashMap<String, Object>();
      mapstruct.put("key", new Text("key" + Integer.toString(index)));

      JsonStringHashMap<String, Object> mapstructrow1 = new JsonStringHashMap<String, Object>();
      mapstructrow1.put("type", new Text("struct" + index.toString()));

      mapstruct.put("value", mapstructrow1);
      JsonStringArrayList<JsonStringHashMap> mapStructValue = new JsonStringArrayList<>();
      mapStructValue.add(mapstruct);


      testBuilder().sqlQuery("SELECT * FROM hive." + table +
        " order by rownum limit 1 offset " + index.toString())
        .ordered()
        .baselineColumns("rownum", "list_field",
          "struct_field",
          "struct_list_field",
          "list_struct_field"
          )
        .baselineValues(new Integer(index), new ArrayList<Integer>(
          Arrays.asList(new Integer(index), new Integer(index + 1),
            new Integer(index + 2), new Integer(index + 3),
            new Integer(index + 4))),
          structrow1, structlistrow1,
          Arrays.asList(liststruct1, liststruct2)
          )
        .go();
    }
  }

  private void readMapValues(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for(int i=0; i < testrows.length; ++i) {
      Integer index = new Integer(testrows[i]);
      String mapquery = "WITH flatten_" + table + " AS (SELECT flatten(map_field) AS flatten_map_field from hive." + table + " ) " +
        " select flatten_map_field['key'] as key_field from flatten_" + table + " order by flatten_map_field['key'] limit 1 offset " + index.toString();

      testBuilder().sqlQuery(mapquery)
        .ordered()
        .baselineColumns("key_field")
        .baselineValues(index)
        .go();

      mapquery = "WITH flatten" + table + " AS (SELECT flatten(map_field) AS flatten_map_field from hive." + table + " ) " +
        " select flatten_map_field['value'] as value_field from flatten" + table + " order by flatten_map_field['value'] limit 1 offset " + index.toString();

      testBuilder().sqlQuery(mapquery)
        .ordered()
        .baselineColumns("value_field")
        .baselineValues(index)
        .go();
    }
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

  @Test
  public void testLowUpperCasingForParquet() throws Exception {
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
  }

  @Test
  public void testHiveParquetRefreshOnMissingFile() throws Exception {
    setEnableReAttempts(true);
    try {
      final String query = "SELECT count(r_regionkey) c FROM hive.parquet_with_two_files";

      // Make sure the plan has Hive scan with native parquet reader
      testPhysicalPlan(query, "mode=[NATIVE_PARQUET");

      // With two files, the expected count is 10 (i.e 2 * 5).
      testBuilder().sqlQuery(query).ordered().baselineColumns("c").baselineValues(10L).go();

      // Move one file out of the dir and run again, the expected count is 5 now.
      File secondFile =
          new File(hiveTest.getWhDir() + "/parquet_with_two_files/", "region2.parquet");
      File tmpPath = new File(hiveTest.getWhDir(), "region2.parquet.tmp");
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
      File secondFile =
        new File(hiveTest.getWhDir() + "/orc_with_two_files/", "region2.orc");
      File tmpPath = new File(hiveTest.getWhDir(), "region2.orc.tmp");
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
    hiveTest.updatePluginConfig((getSabotContext().getCatalogService()),
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
    errorMsgTestHelper("SELECT * FROM hive.\"default\".nonExistedTable", "Table 'hive.default.nonExistedTable' not found");
    errorMsgTestHelper("SELECT * FROM hive.db1.nonExistedTable", "Table 'hive.db1.nonExistedTable' not found");
  }

  @AfterClass
  public static void shutdownOptions() throws Exception {
    test(String.format("alter session set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test
  public void testReadSignatures() throws Exception {
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.db1.avro")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".dummy")))).size());
    assertEquals(2, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.skipper.kv_parquet_large")))).size());

    assertEquals(3, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".readtest")))).size());
    assertEquals(3, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".readtest_parquet")))).size());

    assertEquals(10, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".kv_parquet")))).size());
    assertEquals(54, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".partition_with_few_schemas")))).size());
    assertEquals(56, getCachedEntities(ns.getDataset(new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".partition_pruning_test")))).size());
  }

  private void testCheckReadSignature(EntityPath datasetPath, SupportsReadSignature.MetadataValidity expectedResult) throws Exception {
    NamespaceService ns = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);

    DatasetConfig datasetConfig = ns.getDataset(new NamespaceKey(datasetPath.getComponents()));

    BytesOutput signature = os -> os.write(datasetConfig.getReadDefinition().getReadSignature().toByteArray());
    DatasetHandle datasetHandle = (((CatalogServiceImpl)getSabotContext().getCatalogService()).getSource("hive")).getDatasetHandle(datasetPath).get();
    DatasetMetadata metadata = new DatasetMetadataAdapter(datasetConfig);

    assertEquals(expectedResult, ((SupportsReadSignature) ((CatalogServiceImpl)getSabotContext().getCatalogService()).getSource("hive")).validateMetadata(
      signature, datasetHandle, metadata));
  }

  @Test
  public void testCheckReadSignatureValid() throws Exception {
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "db1", "kv_db1")), SupportsReadSignature.MetadataValidity.VALID);
    testCheckReadSignature(new EntityPath(ImmutableList.of("hive", "default", "partition_with_few_schemas")), SupportsReadSignature.MetadataValidity.VALID);
  }

  @Test
  public void testCheckReadSignatureInvalid() throws Exception {
    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    new File(hiveTest.getWhDir() + "/db1.db/kv_db1", "000000_0").setLastModified(System.currentTimeMillis());

    File newFile = new File(hiveTest.getWhDir() + "/partition_with_few_schemas/c=1/d=1/e=1/", "empty_file");
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


    NamespaceKey dataset = new NamespaceKey(PathUtils.parseFullPath("hive.db1.kv_db1"));
    DatasetConfig datasetConfig = ns.getDataset(dataset);
    assertTrue(((CatalogServiceImpl)getSabotContext().getCatalogService()).getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));

    final Path tableFile = new Path(hiveTest.getWhDir() + "/db1.db/kv_db1/000000_0");
    final Path tableDir = new Path(hiveTest.getWhDir() + "/db1.db/kv_db1");
    final FileSystem localFs = FileSystem.getLocal(new Configuration());

    try {
      // no read on file
      localFs.setPermission(tableFile, new FsPermission(FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE));
      assertFalse(((CatalogServiceImpl)getSabotContext().getCatalogService()).getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
    } finally {
      localFs.setPermission(tableFile, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }

    try {
      // no exec on dir
      localFs.setPermission(tableDir, new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
      assertFalse(((CatalogServiceImpl)getSabotContext().getCatalogService()).getSource("hive").hasAccessPermission(ImpersonationUtil.getProcessUserName(), dataset, datasetConfig));
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
    List<NamespaceKey> tables0 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0l)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0l)
        .setNamesRefreshMs(0l), CatalogServiceImpl.UpdateType.FULL);

    List<NamespaceKey> tables1 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables0.size(), tables1.size());

    // create an empty table
    hiveTest.executeDDL("CREATE TABLE IF NOT EXISTS foo_bar(a INT, b STRING)");

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
        .setAuthTtlMs(0l)
        .setDatasetUpdateMode(UpdateMode.PREFETCH)
        .setDatasetDefinitionTtlMs(0l)
        .setNamesRefreshMs(0l), CatalogServiceImpl.UpdateType.FULL);

    // make sure new table is visible
    List<NamespaceKey> tables2 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables1.size() + 1, tables2.size());

    assertTrue(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".foo_bar")), Type.DATASET));
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
    List<NamespaceKey> tables3 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables2.size(), tables3.size());

    // drop table
    hiveTest.executeDDL("DROP TABLE foo_bar");

    ((CatalogServiceImpl)getSabotContext().getCatalogService()).refreshSource(
      new NamespaceKey("hive"),
      new MetadataPolicy()
      .setAuthTtlMs(0l)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setDatasetDefinitionTtlMs(0l)
      .setNamesRefreshMs(0l), CatalogServiceImpl.UpdateType.FULL);

    // make sure table is deleted from namespace
    List<NamespaceKey> tables4 = Lists.newArrayList(getSabotContext()
        .getNamespaceService(SystemUser.SYSTEM_USERNAME)
        .getAllDatasets(new NamespaceKey("hive")));
    assertEquals(tables3.size() - 1, tables4.size());

    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.\"default\".foo_bar")), Type.DATASET));
    assertFalse(getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).exists(
      new NamespaceKey(PathUtils.parseFullPath("hive.foo_bar")), Type.DATASET));
  }

  private LocalDateTime getLocalDateTime(String dateString) {
    return LocalDateTime.parse(dateString);
  }

  @Test
  public void testTextAndOrcTableWithDateColumn() throws Exception {
    LocalDateTime dateTime1 = getLocalDateTime("0001-01-01");
    LocalDateTime dateTime2 = getLocalDateTime("1299-01-01");
    LocalDateTime dateTime3 = getLocalDateTime("1499-01-01");
    LocalDateTime dateTime4 = getLocalDateTime("1582-01-01");
    LocalDateTime dateTime5 = getLocalDateTime("1699-01-01");

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
      .baselineValues(new Float("-0.1"))
      .go();

    //double column
    testBuilder()
      .sqlQuery("SELECT col2 from hive.orcdecimalcompare where col2 < 0")
      .unOrdered()
      .baselineColumns("col2")
      .baselineValues(new Double("-0.1"))
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
}
