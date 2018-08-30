/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestParquetTimestampInt96 extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  static FileSystem fs;

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

  public void compareParquetReadersColumnar(String selection, String table) throws Exception {
    String query = "select " + selection + " from " + table;

    try {
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

  /*
    Impala encodes timestamp values as int96 fields. Test the reading of an int96 field with two converters:
    the first one converts parquet INT96 into Dremio VARBINARY and the second one (works while
    store.parquet.reader.int96_as_timestamp option is enabled) converts parquet INT96 into Dremio TIMESTAMP.
   */
  @Test
  public void testImpalaParquetInt96() throws Exception {
    compareParquetReadersColumnar("field_impala_ts", "cp.\"parquet/int96_impala_1.parquet\"");
  }

  /*
   Test the reading of a binary field as timestamp where data is in dictionary _and_ non-dictionary encoded pages
    */
  @Test
  public void testImpalaParquetBinaryAsTimeStamp_DictChange() throws Exception {
    final String WORKING_PATH = TestTools.getWorkingPath();
    final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";
    testBuilder()
      .sqlQuery("select int96_ts from dfs.\"%s/parquet/int96_dict_change\" order by int96_ts", TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("testframework/testParquetReader/testInt96DictChange/q1.tsv")
      .baselineTypes(TypeProtos.MinorType.TIMESTAMP)
      .baselineColumns("int96_ts")
      .build().run();
  }

  @Test // DRILL-5097
  public void testInt96TimeStampValueWidth() throws Exception {
    testBuilder()
    .ordered()
    .sqlQuery("select c, d from cp.\"parquet/data.snappy.parquet\" where d = '2015-07-18 13:52:51'")
    .baselineColumns("c", "d")
    .baselineValues(DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD").parseLocalDateTime("2011-04-11"),
        DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH24:MI:SS").parseLocalDateTime("2015-07-18 13:52:51"))
    .build()
    .run();
  }

  @Test
  public void testInt96TimeStampDeltaReader() throws Exception {
    // time in UTC
    testBuilder()
      .ordered()
      .sqlQuery("SELECT varchar_field, timestamp_field\n" +
        "FROM cp.\"parquet/int96.parquet\"\n" +
        "where varchar_field = 'c'")
      .baselineColumns("varchar_field", "timestamp_field")
      .baselineValues("c",
        DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH24:MI:SS.FFF").parseLocalDateTime("1969-12-31 08:03:00.000"))
      .build()
      .run();
  }
}

