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
package com.dremio.exec.physical.impl.writer;

import static com.dremio.exec.store.parquet.ParquetRecordWriter.DREMIO_VERSION_PROPERTY;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.PageHeaderUtil;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.fn.interp.TestConstantFolding;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.test.AllocatorRule;
import com.google.common.base.Joiner;

public class TestParquetWriter extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  static FileSystem fs;

  // Map storing a convenient name as well as the cast type necessary
  // to produce it casting from a varchar
  private static final Map<String, String> allTypes = new HashMap<>();

  // Select statement for all supported Dremio types, for use in conjunction with
  // the file parquet/alltypes.json in the resources directory
  private static final String allTypesSelection;

  static {
    allTypes.put("int",                "int");
    allTypes.put("bigint",             "bigint");
    // TODO(DRILL-3367)
//    allTypes.put("decimal(9, 4)",      "decimal9");
//    allTypes.put("decimal(18,9)",      "decimal18");
//    allTypes.put("decimal(28, 14)",    "decimal28sparse");
//    allTypes.put("decimal(38, 19)",    "decimal38sparse");
    allTypes.put("date",               "date");
    allTypes.put("timestamp",          "timestamp");
    allTypes.put("float",              "float4");
    allTypes.put("double",             "float8");
    allTypes.put("varbinary",   "varbinary");
    // TODO(DRILL-2297)
    // TODO: Can not read back interval types for now.
//    allTypes.put("interval year",      "intervalyear");
//    allTypes.put("interval day",       "intervalday");
    allTypes.put("boolean",            "bit");
    allTypes.put("varchar",            "varchar");
    allTypes.put("time",               "time");

    List<String> allTypeSelectsAndCasts = new ArrayList<>();
    for (String s : allTypes.keySet()) {
      // don't need to cast a varchar, just add the column reference
      if ("varchar".equals(s)) {
        allTypeSelectsAndCasts.add(String.format("\"%s_col\"", allTypes.get(s)));
      } else if ("varbinary".equals(s)) {
        allTypeSelectsAndCasts.add(String.format("convert_to(\"%s_col\", 'UTF8') \"%s_col\"", s, allTypes.get(s)));
      } else {
        allTypeSelectsAndCasts.add(String.format("cast(\"%s_col\" AS %S) \"%s_col\"", allTypes.get(s), s, allTypes.get(s)));
      }
    }
    allTypesSelection = Joiner.on(",").join(allTypeSelectsAndCasts);
  }


  private String allTypesTable = "cp.\"/parquet/alltypes.json\"";

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
    test(String.format("alter system set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter system set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test
  public void testSmallFileValueReadWrite() throws Exception {
    String selection = "key";
    String inputTable = "cp.\"/store/json/intData.json\"";
    runTestAndValidate(selection, selection, inputTable, "smallFileTest", false);
  }

  @Test
  public void testSimple() throws Exception {
    String selection = "*";
    String inputTable = "cp.\"employee.json\"";
    runTestAndValidate(selection, selection, inputTable, "employee_parquet", false);
  }

  @Test
  public void testDrill1058() throws Exception {
    runTestAndValidate("*", "*", "cp.\"/store/json/complex.json\"", "complex_parquet", false);
  }

  @Test
  public void testLargeFooter() throws Exception {
    StringBuffer sb = new StringBuffer();
    // create a JSON document with a lot of columns
    sb.append("{");
    final int numCols = 799;
    String[] colNames = new String[numCols];
    Object[] values = new Object[numCols];
    for (int i = 0 ; i < numCols - 1; i++) {
      sb.append(String.format("\"col_%d\" : 100,", i));
      colNames[i] = "col_" + i;
      values[i] = 100L;
    }
    // add one column without a comma after it
    sb.append(String.format("\"col_%d\" : 100", numCols - 1));
    sb.append("}");
    colNames[numCols - 1] = "col_" + (numCols - 1);
    values[numCols - 1] = 100L;

    // write it to a file in the temp directory for the test
    new TestConstantFolding.SmallFileCreator(folder).setRecord(sb.toString()).createFiles(1, 1, "json");

    String path = folder.getRoot().toPath().toString();
    test("use dfs_test");
    test("create table WIDE_PARQUET_TABLE_TestParquetWriter_testLargeFooter as select * from dfs.\"" + path + "/smallfile/smallfile.json\"");
    testBuilder()
        .sqlQuery("select * from dfs_test.WIDE_PARQUET_TABLE_TestParquetWriter_testLargeFooter")
        .unOrdered()
        .baselineColumns(colNames)
        .baselineValues(values)
        .build().run();
  }

  @Test
  public void testAllScalarTypes() throws Exception {
    /// read once with the flat reader
    runTestAndValidate(allTypesSelection, "*", allTypesTable, "testAllScalarTypes_donuts_json", false);

    try {
      // read all of the types with the complex reader
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_NEW_RECORD_READER));
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "donuts_json", false);
    } finally {
      test(String.format("alter session set %s = false", ExecConstants.PARQUET_NEW_RECORD_READER));
    }
  }

  @Test
  public void testNullAndEmptyMaps() throws Exception {
    runTestAndValidate("map", "*", "cp.\"/json/null_map.json\"", "null_empty_maps_json", false);
  }

  @Test
  public void testNullAndEmptyLists() throws Exception {
    runTestAndValidate("mylist", "*", "cp.\"/json/null_list.json\"", "null_empty_lists_json", false);
  }

  @Test
  public void testMapListMap() throws Exception {
    runTestAndValidate("mymap", "*", "cp.\"/json/map_list_map.json\"", "map_list_map", false);
  }

  @Test
  public void testListOfList() throws Exception {
    runTestAndValidate("mylist", "mylist", "cp.\"/json/list_list.json\"", "list_list_json", false);
  }

  @Test
  public void testAllScalarTypesDictionary() throws Exception {
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
      /// read once with the flat reader
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "AllScalarTypesDictionary_json", false);

      // read all of the types with the complex reader
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_NEW_RECORD_READER));
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "AllScalarTypesDictionary2_json", false);
    } finally {
      test(String.format("alter session set %s = false", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
    }
  }

  @Test
  public void testDictionaryError() throws Exception {
    compareParquetReadersColumnar("*", "cp.\"parquet/required_dictionary.parquet\"");
    runTestAndValidate("*", "*", "cp.\"parquet/required_dictionary.parquet\"", "required_dictionary", false);
  }

  @Test
  public void testDictionaryEncoding() throws Exception {
    String selection = "type";
    String inputTable = "cp.\"donuts.json\"";
    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
      runTestAndValidate(selection, selection, inputTable, "testDictionaryEncoding_donuts_json", false);
    } finally {
      test(String.format("alter session set %s = false", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
    }
  }

  @Test
  public void testComplex() throws Exception {
    String selection = "batters";
    String inputTable = "cp.\"donuts.json\"";
    runTestAndValidate(selection, selection, inputTable, "testComplex_donuts_json", false);
  }

  @Test
  public void testComplexRepeated() throws Exception {
    String selection = "*";
    String inputTable = "cp.\"testRepeatedWrite.json\"";
    runTestAndValidate(selection, selection, inputTable, "repeated_json", false);
  }

  @Test
  public void testCastProjectBug_Drill_929() throws Exception {
    String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as COMMITDATE, cast(L_RECEIPTDATE as DATE) AS RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,COMMITDATE ,RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";

    String inputTable = "cp.\"tpch/lineitem.parquet\"";
    runTestAndValidate(selection, validationSelection, inputTable, "drill_929", false);
}

  @Test
  public void testTPCHReadWrite1() throws Exception {
    String inputTable = "cp.\"tpch/lineitem.parquet\"";
    runTestAndValidate("*", "*", inputTable, "lineitem_parquet_all", false);
  }

  @Test
  public void testTPCHReadWrite1_date_convertedType() throws Exception {
    try {
      test("alter session set \"%s\" = false", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);
      String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as L_COMMITDATE, cast(L_RECEIPTDATE as DATE) AS L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
      String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,L_COMMITDATE ,L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
      String inputTable = "cp.\"tpch/lineitem.parquet\"";
      runTestAndValidate(selection, validationSelection, inputTable, "lineitem_parquet_converted", false);
    } finally {
      test("alter session set \"%s\" = %b", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, ExecConstants
        .PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR.getDefault().getBoolVal());
    }
  }

  @Test
  public void testTPCHReadWrite2() throws Exception {
    String inputTable = "cp.\"tpch/customer.parquet\"";
    runTestAndValidate("*", "*", inputTable, "customer_parquet", false);
  }

  @Test
  public void testTPCHReadWrite3() throws Exception {
    String inputTable = "cp.\"tpch/nation.parquet\"";
    runTestAndValidate("*", "*", inputTable, "nation_parquet", false);
  }

  @Test
  public void testTPCHReadWrite4() throws Exception {
    String inputTable = "cp.\"tpch/orders.parquet\"";
    runTestAndValidate("*", "*", inputTable, "orders_parquet", false);
  }

  @Test
  public void testTPCHReadWrite5() throws Exception {
    String inputTable = "cp.\"tpch/part.parquet\"";
    runTestAndValidate("*", "*", inputTable, "part_parquet", false);
  }

  @Test
  public void testTPCHReadWrite6() throws Exception {
    String inputTable = "cp.\"tpch/partsupp.parquet\"";
    runTestAndValidate("*", "*", inputTable, "partsupp_parquet", false);
  }

  @Test
  public void testTPCHReadWrite7() throws Exception {
    String inputTable = "cp.\"tpch/region.parquet\"";
    runTestAndValidate("*", "*", inputTable, "region_parquet", false);
  }

  @Test
  public void testTPCHReadWrite8() throws Exception {
    String inputTable = "cp.\"tpch/supplier.parquet\"";
    runTestAndValidate("*", "*", inputTable, "supplier_parquet", false);
  }

  @Test
  public void testTPCHReadWriteNoDictUncompressed() throws Exception {
    try {
      test(String.format("alter session set \"%s\" = false", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
      test(String.format("alter session set \"%s\" = 'none'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
      String inputTable = "cp.\"tpch/supplier.parquet\"";
      runTestAndValidate("*", "*", inputTable, "supplier_parquet_no_dict_uncompressed", false);
    } finally {
      test(String.format("alter session set \"%s\" = %b", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING,
        ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR.getDefault().getBoolVal()));
      test(String.format("alter session set \"%s\" = '%s'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE,
        ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR.getDefault().getStringVal()));
    }
  }

  @Test
  public void testTPCHReadWriteDictGzip() throws Exception {
    try {
      test(String.format("alter session set \"%s\" = 'gzip'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
      String inputTable = "cp.\"tpch/supplier.parquet\"";
      runTestAndValidate("*", "*", inputTable, "supplier_parquet_dict_gzip", false);
    } finally {
      test(String.format("alter session set \"%s\" = '%s'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE,
        ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR.getDefault().getStringVal()));
    }
  }

  @Test
  public void testTPCHReadWriteDictZstd() throws Exception {
    String outputTableMinLevel = "supplier_parquet_dict_zstd_minlevel";
    String outputTableMaxLevel = "supplier_parquet_dict_zstd_maxlevel";
    try {
      test(String.format("ALTER SESSION SET \"%s\" = 'zstd'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
      String inputTable = "cp.\"tpch/supplier.parquet\"";

      test(String.format("ALTER SESSION SET \"%s\" = %d", ExecConstants.PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL, Integer.MIN_VALUE));
      runTestAndValidate("*", "*", inputTable, outputTableMinLevel, false, true);

      test(String.format("ALTER SESSION SET \"%s\" = %d", ExecConstants.PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL, Integer.MAX_VALUE));
      runTestAndValidate("*", "*", inputTable, outputTableMaxLevel, false, true);

      // The only way to check if the level arrives to the compressors is to check the sizes of the generated files
      long minLevelSize = calculateSize(new Path(getDfsTestTmpSchemaLocation(), outputTableMinLevel));
      long maxLevelSize = calculateSize(new Path(getDfsTestTmpSchemaLocation(), outputTableMaxLevel));
      assertTrue("The parquet files generated with minimum ZSTD compression level should be bigger than the" +
        " ones generated with maximum compression level", minLevelSize > maxLevelSize);
    } finally {
      test(String.format("ALTER SESSION SET \"%s\" = '%s'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE,
        ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR.getDefault().getStringVal()));
      test(String.format("ALTER SESSION SET \"%s\" = %d", ExecConstants.PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL,
        ExecConstants.PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL_VALIDATOR.getDefault().getNumVal()));
      deleteTableIfExists(outputTableMinLevel);
      deleteTableIfExists(outputTableMaxLevel);
    }
  }

  private long calculateSize(Path path) throws IOException {
    long size = 0L;
    RemoteIterator<LocatedFileStatus> it = path.getFileSystem(new Configuration()).listFiles(path, true);
    while (it.hasNext()) {
      size += it.next().getLen();
    }
    return size;
  }

  // working to create an exhaustive test of the format for this one. including all convertedTypes
  // will not be supporting interval for Beta as of current schedule
  // Types left out:
  // "TIMESTAMPTZ_col"
  @Test
  public void testRepeated() throws Exception {
    String inputTable = "cp.\"parquet/basic_repeated.json\"";
    runTestAndValidate("*", "*", inputTable, "basic_repeated", false);
  }

  @Test
  public void testRepeatedDouble() throws Exception {
    String inputTable = "cp.\"parquet/repeated_double_data.json\"";
    runTestAndValidate("*", "*", inputTable, "repeated_double_parquet", false);
  }

  @Test
  public void testRepeatedLong() throws Exception {
    String inputTable = "cp.\"parquet/repeated_integer_data.json\"";
    runTestAndValidate("*", "*", inputTable, "repeated_int_parquet", false);
  }

  @Test
  public void testRepeatedBool() throws Exception {
    String inputTable = "cp.\"parquet/repeated_bool_data.json\"";
    runTestAndValidate("*", "*", inputTable, "repeated_bool_parquet", false);
  }

  @Test
  public void testNullReadWrite() throws Exception {
    String inputTable = "cp.\"parquet/null_test_data.json\"";
    runTestAndValidate("*", "*", inputTable, "nullable_test", false);
  }

  @Test
  public void testDecimal() throws Exception {
    String selection = "cast(salary as decimal(8,2)) as decimal8, cast(salary as decimal(15,2)) as decimal15, " +
        "cast(salary as decimal(24,2)) as decimal24, cast(salary as decimal(38,2)) as decimal38";
    String validateSelection = "decimal8, decimal15, decimal24, decimal38";
    String inputTable = "cp.\"employee.json\"";
    runTestAndValidate(selection, validateSelection, inputTable,
        "parquet_decimal", false);
  }

  @Test
  public void testMulipleRowGroups() throws Exception {
    try (AutoCloseable ac = withOption(ExecConstants.PARQUET_READER_VECTORIZE, false)) {
      test(String.format("ALTER SESSION SET \"%s\" = %d", ExecConstants.PARQUET_BLOCK_SIZE, 1024*1024));
      String selection = "mi";
      String inputTable = "cp.\"customer.json\"";
      runTestAndValidate(selection, selection, inputTable, "foodmart_customer_parquet", false);
    } finally {
      test(String.format("ALTER SESSION SET \"%s\" = %d", ExecConstants.PARQUET_BLOCK_SIZE, 512*1024*1024));
    }
  }


  @Test
  public void testDate() throws Exception {
    String selection = "cast(hire_date as DATE) as hire_date";
    String validateSelection = "hire_date";
    String inputTable = "cp.\"employee.json\"";
    runTestAndValidate(selection, validateSelection, inputTable, "foodmart_employee_parquet", false);
  }

  @Test
  public void testBoolean() throws Exception {
    String selection = "true as x, false as y";
    String validateSelection = "x, y";
    String inputTable = "cp.\"tpch/region.parquet\"";
    runTestAndValidate(selection, validateSelection, inputTable, "region_boolean_parquet", false);
  }

  @Test //DRILL-2030
  public void testWriterWithStarAndExp() throws Exception {
    String selection = " *, r_regionkey + 1 r_regionkey2";
    String validateSelection = "r_regionkey, r_name, r_comment, r_regionkey + 1 r_regionkey2";
    String inputTable = "cp.\"tpch/region.parquet\"";
    runTestAndValidate(selection, validateSelection, inputTable, "region_star_exp", false);
  }

  @Test // DRILL-2458
  public void testWriterWithStarAndRegluarCol() throws Exception {
    String outputFile = "region_sort";
    String ctasStmt = "create table " + outputFile + " as select *, r_regionkey + 1 as key1 from cp.\"tpch/region.parquet\" order by r_name";
    String query = "select r_regionkey, r_name, r_comment, r_regionkey +1 as key1 from cp.\"tpch/region.parquet\" order by r_name";
    String queryFromWriteOut = "select * from " + outputFile;

    try {
      test("use dfs_test");
      test(ctasStmt);
      testBuilder()
          .ordered()
          .sqlQuery(queryFromWriteOut)
          .sqlBaselineQuery(query)
          .build().run();
    } finally {
      deleteTableIfExists(outputFile);
    }
  }

  public void compareParquetReadersColumnar(String selection, String table) throws Exception {
    String query = "select " + selection + " from " + table;

    try(AutoCloseable o = withSystemOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR, false)) {
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery(
            "alter system set \"store.parquet.use_new_reader\" = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline(
            "alter system set \"store.parquet.use_new_reader\" = true")
        .build().run();
    } finally {
      test("alter system set \"%s\" = %b",
          ExecConstants.PARQUET_NEW_RECORD_READER,
          ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR
              .getDefault().getBoolVal());
    }
  }

  public void compareParquetReadersHyperVector(String selection, String table) throws Exception {

    String query = "select " + selection + " from " + table;
    try {
      testBuilder()
        .ordered()
        .highPerformanceComparison()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery(
            "alter system set \"store.parquet.use_new_reader\" = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline(
            "alter system set \"store.parquet.use_new_reader\" = true")
        .build().run();
    } finally {
      test("alter system set \"%s\" = %b",
          ExecConstants.PARQUET_NEW_RECORD_READER,
          ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR
              .getDefault().getBoolVal());
    }
  }

  @Ignore("DX-4852")
  @Test
  public void testWriteDecimal() throws Exception {
    String outputTable = "decimal_test";

    try {
      String ctas = String.format("use dfs_test; " +
          "create table %s as select " +
          "cast('1.2' as decimal(38, 2)) col1, cast('1.2' as decimal(28, 2)) col2 " +
          "from cp.\"employee.json\" limit 1", outputTable);

      test(ctas);

      BigDecimal result = new BigDecimal("1.20");

      testBuilder()
          .unOrdered()
          .sqlQuery(String.format("select col1, col2 from %s ", outputTable))
          .baselineColumns("col1", "col2")
          .baselineValues(result, result)
          .go();
    } finally {
      deleteTableIfExists(outputTable);
    }
  }

  @Test
  public void testWriteDouble() throws Exception {
    String outputTable = "double_test";

    try {
      String ctas = String.format("use dfs_test; " +
        "create table %s as select " +
        "cast('1.2' as double) col1, cast('1.2' as double) col2 " +
        "from cp.\"employee.json\" limit 1", outputTable);

      test(ctas);

      testBuilder()
        .unOrdered()
        .sqlQuery(String.format("select col1, col2 from %s ", outputTable))
        .baselineColumns("col1", "col2")
        .baselineValues(1.2D, 1.2D)
        .go();
    } finally {
      deleteTableIfExists(outputTable);
    }
  }

  @Test // DRILL-2341
  public void tableSchemaWhenSelectFieldsInDef_SelectFieldsInView() throws Exception {
    final String newTblName = "testTableOutputSchema";

    try {
      final String ctas = String.format("CREATE TABLE dfs_test.%s(id, name, bday) AS SELECT " +
          "cast(\"employee_id\" as integer), " +
          "cast(\"full_name\" as varchar(100)), " +
          "cast(\"birth_date\" as date) " +
          "FROM cp.\"employee.json\" ORDER BY \"employee_id\" LIMIT 1", newTblName);

      test(ctas);

      testBuilder()
          .unOrdered()
          .sqlQuery(String.format("SELECT * FROM dfs_test.\"%s\"", newTblName))
          .baselineColumns("id", "name", "bday")
          .baselineValues(1, "Sheri Nowmer",
              DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD").parseLocalDateTime("1961-08-26"))
          .go();
    } finally {
      deleteTableIfExists(newTblName);
    }
  }

  /*
 * Method tests CTAS with interval data type. We also verify reading back the data to ensure we
 * have written the correct type. For every CTAS operation we use both the readers to verify results.
 */
  @Test
  @Ignore
  public void testCTASWithIntervalTypes() throws Exception {
    test("use dfs_test");

    String tableName = "drill_1980_t1";
    // test required interval day type
    test(String.format("create table %s as " +
        "select " +
        "interval '10 20:30:40.123' day to second col1, " +
        "interval '-1000000000 20:12:23.999' day(10) to second col2 " +
        "from cp.\"employee.json\" limit 2", tableName));

    Period row1Col1 = new Period(0, 0, 0, 10, 0, 0, 0, 73840123);
    Period row1Col2 = new Period(0, 0, 0, -1000000000, 0, 0, 0, -72743999);
    testParquetReaderHelper(tableName, row1Col1, row1Col2, row1Col1, row1Col2);

    tableName = "drill_1980_2";

    // test required interval year type
    test(String.format("create table %s as " +
        "select " +
        "interval '10-2' year to month col1, " +
        "interval '-100-8' year(3) to month col2 " +
        "from cp.\"employee.json\" limit 2", tableName));

    row1Col1 = new Period(0, 122, 0, 0, 0, 0, 0, 0);
    row1Col2 = new Period(0, -1208, 0, 0, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row1Col1, row1Col2);
    // test nullable interval year type
    tableName = "drill_1980_t3";
    test(String.format("create table %s as " +
        "select " +
        "cast (intervalyear_col as interval year) col1," +
        "cast(intervalyear_col as interval year) + interval '2' year col2 " +
        "from cp.\"parquet/alltypes.json\" where tinyint_col = 1 or tinyint_col = 2", tableName));

    row1Col1 = new Period(0, 12, 0, 0, 0, 0, 0, 0);
    row1Col2 = new Period(0, 36, 0, 0, 0, 0, 0, 0);
    Period row2Col1 = new Period(0, 24, 0, 0, 0, 0, 0, 0);
    Period row2Col2 = new Period(0, 48, 0, 0, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row2Col1, row2Col2);

    // test nullable interval day type
    tableName = "drill_1980_t4";
    test(String.format("create table %s as " +
        "select " +
        "cast(intervalday_col as interval day) col1, " +
        "cast(intervalday_col as interval day) + interval '1' day col2 " +
        "from cp.\"parquet/alltypes.json\" where tinyint_col = 1 or tinyint_col = 2", tableName));

    row1Col1 = new Period(0, 0, 0, 1, 0, 0, 0, 0);
    row1Col2 = new Period(0, 0, 0, 2, 0, 0, 0, 0);
    row2Col1 = new Period(0, 0, 0, 2, 0, 0, 0, 0);
    row2Col2 = new Period(0, 0, 0, 3, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row2Col1, row2Col2);
  }

  private void testParquetReaderHelper(String tableName, Period row1Col1, Period row1Col2,
                                       Period row2Col1, Period row2Col2) throws Exception {

    final String switchReader = "alter session set \"store.parquet.use_new_reader\" = %s; ";
    final String enableVectorizedReader = String.format(switchReader, true);
    final String disableVectorizedReader = String.format(switchReader, false);
    String query = String.format("select * from %s", tableName);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery(enableVectorizedReader)
        .baselineColumns("col1", "col2")
        .baselineValues(row1Col1, row1Col2)
        .baselineValues(row2Col1, row2Col2)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery(disableVectorizedReader)
        .baselineColumns("col1", "col2")
        .baselineValues(row1Col1, row1Col2)
        .baselineValues(row2Col1, row2Col2)
        .go();
  }

  private static void deleteTableIfExists(String tableName) {
    try {
      Path path = new Path(getDfsTestTmpSchemaLocation(), tableName);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      // ignore exceptions.
      e.printStackTrace();
    }
  }

  private String select(String selection, String input, boolean sort) {
    if (sort) {
      return String.format("SELECT %s FROM %s ORDER BY 1", selection, input);
    } else {
      return String.format("SELECT %s FROM %s", selection, input);
    }
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable, String outputFile, boolean sort) throws Exception {
    runTestAndValidate(selection, validationSelection, inputTable, outputFile, sort, false);
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable, String outputFile,
    boolean sort, boolean keepOutput) throws Exception {
    runTestAndValidate(selection, validationSelection, inputTable, outputFile, sort, keepOutput, file -> {});
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable, String outputFile,
    boolean sort, boolean keepOutput, Consumer<Path> fileValidator) throws Exception {
    try {
      deleteTableIfExists(outputFile);
      test("use dfs_test");
  //    test("ALTER SESSION SET \"planner.add_producer_consumer\" = false");
      String query = select(selection, inputTable, sort);
      System.out.println(outputFile);
      String create = "CREATE TABLE " + outputFile + " AS " + query;
      String validateQuery = select(validationSelection, outputFile, sort);
      test(create);
      test(validateQuery); // TODO: remove
      testBuilder()
          .unOrdered()
          .sqlQuery(validateQuery)
          .sqlBaselineQuery(query)
          .go();

      Configuration hadoopConf = new Configuration();
      Path output = new Path(getDfsTestTmpSchemaLocation(), outputFile);
      FileSystem fs = output.getFileSystem(hadoopConf);
      for (FileStatus file : fs.listStatus(output)) {
        ParquetMetadata footer = ParquetFileReader.readFooter(hadoopConf, file, SKIP_ROW_GROUPS);
        String version = footer.getFileMetaData().getKeyValueMetaData().get(DREMIO_VERSION_PROPERTY);
        assertEquals(DremioVersionInfo.getVersion(), version);
        PageHeaderUtil.validatePageHeaders(file.getPath(), footer);
        fileValidator.accept(file.getPath());
      }
    } finally {
      if (!keepOutput) {
        deleteTableIfExists(outputFile);
      }
    }
  }

  /*
  Test the reading of a binary field as varbinary where data is in dictionary _and_ non-dictionary encoded pages
   */
  @Test
  public void testImpalaParquetBinaryAsVarBinary_DictChange() throws Exception {
    compareParquetReadersColumnar("int96_ts", "cp.\"parquet/int96_dict_change.parquet\"");
  }

   /*
     Test the conversion from int96 to impala timestamp
   */
  @Test
  public void testTimestampImpalaConvertFrom() throws Exception {
    compareParquetReadersColumnar("convert_from(field_impala_ts, 'TIMESTAMP_IMPALA')", "cp.\"parquet/int96_impala_1.parquet\"");
  }

  /*
    Test a file with partitions and an int96 column. (Data generated using Hive)
   */
  @Test
  public void testImpalaParquetInt96Partitioned() throws Exception {
    compareParquetReadersColumnar("timestamp_field", "cp.\"parquet/part1/hive_all_types.parquet\"");
  }

  /*
  Test the conversion from int96 to impala timestamp with hive data including nulls. Validate against old reader
  */
  @Test
  public void testHiveParquetTimestampAsInt96_compare() throws Exception {
    compareParquetReadersColumnar("convert_from(timestamp_field, 'TIMESTAMP_IMPALA')", "cp.\"parquet/part1/hive_all_types.parquet\"");
  }

  /*
  Test the conversion from int96 to impala timestamp with hive data including nulls. Validate against expected values
  */
  @Test
  public void testHiveParquetTimestampAsInt96_basic() throws Exception {
    final String q = "SELECT cast(convert_from(timestamp_field, 'TIMESTAMP_IMPALA') as varchar(19))  as timestamp_field "
            + "from cp.\"parquet/part1/hive_all_types.parquet\" ";

    try(AutoCloseable o = withSystemOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR, false)) {
      testBuilder()
          .unOrdered()
          .sqlQuery(q)
          .baselineColumns("timestamp_field")
          .baselineValues("2013-07-06 00:01:00")
          .baselineValues((Object) null)
          .go();
    }
  }

  //
  // This fails with varying errors
  //
  @Test
  public void testSchemaChange() throws Exception {
    File dir = new File("target/tests/" + this.getClass().getName() + "/testSchemaChange");
    if ((!dir.exists() && !dir.mkdirs()) || (dir.exists() && !dir.isDirectory())) {
      throw new RuntimeException("can't create dir " + dir);
    }
    // CAUTION: There's no ordering guarantee about which file will be read first
    // so make sure that each file has an incompatible schema compared to the other one.
    File input1 = new File(dir, "1.json");
    File input2 = new File(dir, "2.json");
    try (FileWriter fw = new FileWriter(input1)) {
      fw.append("{\"a\":\"foo\"}\n");
      fw.append("{\"a\":\"bar\"}\n");
      fw.append("{\"a\":\"baz\", \"c\": \"foz\"}\n");
    }
    try (FileWriter fw = new FileWriter(input2)) {
      fw.append("{\"b\":\"foo\"}\n");
      fw.append("{\"a\":\"baz\",\"b\":\"baz\"}\n");
      fw.append("{\"a\":\"baz\",\"b\":\"boz\"}\n");
    }
    // we don't retry in test, so let the schema learn the first time around
    try {
      test("select * from " + "dfs.\"" + dir.getAbsolutePath() + "\"");
      fail("query should have failed with schema change error");
    } catch (UserRemoteException e) {
      assertTrue(e.toString(), e.getMessage().contains("SCHEMA_CHANGE ERROR"));
    }
    // and it should work the second time
    runTestAndValidate("*", "*", "dfs.\"" + dir.getAbsolutePath() + "\"", "schema_change_parquet", false);
  }

/*
  The following test boundary conditions for null values occurring on page boundaries. All files have at least one dictionary
  encoded page for all columns
  */
  @Test
  public void testAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/all_nulls.parquet\"");
  }

  @Test
  public void testNoNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/no_nulls.parquet\"");
  }

  @Test
  public void testFirstPageAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/first_page_all_nulls.parquet\"");
  }
  @Test
  public void testLastPageAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/first_page_all_nulls.parquet\"");
  }
  @Test
  public void testFirstPageOneNull() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/first_page_one_null.parquet\"");
  }
  @Test
  public void testLastPageOneNull() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.\"parquet/last_page_one_null.parquet\"");
  }

  @Test
  public void testComplex243() throws Exception {
    final String jsonFile = "cp.\"parquet/complex243.json\"";
    final String parquetTable = "dfs_test.\"complex243_json\"";
    final String ctas = "CREATE TABLE " + parquetTable + " AS SELECT id, ooa FROM " + jsonFile;
    runSQL(ctas);

    final String query = "SELECT t.id, t.ooa[2].a.aa.aaa as aaa FROM %s t";

    testBuilder()
            .unOrdered()
            .sqlQuery(query, parquetTable)
            .sqlBaselineQuery(query, jsonFile)
            .go();
  }

  @Test
  public void testWriterVersionV1() throws Exception {
    // Parquet writer version "v1" is currently the default; no additional setting is required
    runTestAndValidateFirstPageType(PageType.DATA_PAGE);
  }

  @Test
  public void testWriterVersionV2() throws Exception {
    try {
      test(String.format("ALTER SESSION SET %s = 'v2'", ExecConstants.PARQUET_WRITER_VERSION.getOptionName()));
      runTestAndValidateFirstPageType(PageType.DATA_PAGE_V2);
    } finally {
      test(String.format("ALTER SESSION RESET %s", ExecConstants.PARQUET_WRITER_VERSION.getOptionName()));
    }
  }

  private void runTestAndValidateFirstPageType(PageType pageType) throws Exception {
    try {
      // Disable dictionary encoding, so we can check the data page type
      test(String.format("ALTER SESSION SET %s = false", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));

      runTestAndValidate("*", "*", "cp.\"donuts.json\"", "testParquetWriterVersion_donuts_json_" + pageType.name(),
        false, false, file -> {
        try (InputStream is = file.getFileSystem(new Configuration()).open(file)) {
          is.skip(4L); // Skip magic header
          PageHeader pageHeader = Util.readPageHeader(is);
          Assert.assertEquals(pageType, pageHeader.getType());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } finally {
      test(String.format("ALTER SESSION RESET %s", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
    }
  }
}
