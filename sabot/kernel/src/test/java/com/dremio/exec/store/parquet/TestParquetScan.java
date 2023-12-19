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
package com.dremio.exec.store.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.google.common.io.Resources;

public class TestParquetScan extends BaseTestQuery {

  static FileSystem fs;
  private static final String DISABLE_VECTORIZED_READ = "alter system set \"store.parquet.vectorize\" = %s";

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
  }

  @Test
  public void testSuccessFile() throws Exception {
    Path p = new Path("/tmp/nation_test_parquet_scan");
    if (fs.exists(p)) {
      fs.delete(p, true);
    }

    fs.mkdirs(p);

    byte[] bytes = Resources.toByteArray(Resources.getResource("tpch/nation.parquet"));

    FSDataOutputStream os = fs.create(new Path(p, "nation.parquet"));
    os.write(bytes);
    os.close();
    fs.create(new Path(p, "_SUCCESS")).close();
    fs.create(new Path(p, "_logs")).close();

    testBuilder()
        .sqlQuery("select count(*) c from dfs.tmp.nation_test_parquet_scan where 1 = 1")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(25L)
        .build()
        .run();
  }

  @Test
  public void testDataPageV2() throws Exception {
    final String sql = "select count(*) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/datapage_v2.parquet\" where extractmonth(Kommtzeit) = 10";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(570696L)
      .build()
      .run();
  }

  @Test
  public void testRefreshOnFileNotFound() throws Exception {
    setEnableReAttempts(true);
    try {
      // create directory
      Path dir = new Path("/tmp/parquet_scan_file_refresh");
      if (fs.exists(dir)) {
        fs.delete(dir, true);
      }
      fs.mkdirs(dir);

      // Create 10 parquet files in the directory.
      byte[] bytes = Resources.toByteArray(Resources.getResource("tpch/nation.parquet"));
      for (int i = 0; i < 10; ++i) {
        FSDataOutputStream os = fs.create(new Path(dir, i + "nation.parquet"));
        os.write(bytes);
        os.close();
      }

      // query on all 10 files.
      testBuilder()
          .sqlQuery(
              "select count(*) c from dfs.tmp.parquet_scan_file_refresh where length(n_comment) > 0")
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(250L)
          .build()
          .run();

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // delete every alternate file.
      for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
          fs.delete(new Path(dir, i + "nation.parquet"), false);
        }
      }

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // re-run the query. Should trigger a metadata refresh and succeed.
      testBuilder()
          .sqlQuery(
              "select count(*) c from dfs.tmp.parquet_scan_file_refresh where length(n_comment) > 0")
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(125L)
          .build()
          .run();

      // cleanup
      fs.delete(dir, true);
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testRefreshOnDirNotFound() throws Exception {
    setEnableReAttempts(true);
    try {
      Path dir = new Path("/tmp/parquet_scan_dir_refresh");

      // create directory
      if (fs.exists(dir)) {
        fs.delete(dir, true);
      }
      fs.mkdirs(dir);

      // Create 9 parquet files in sub-directories of the main directory.
      byte[] bytes = Resources.toByteArray(Resources.getResource("tpch/nation.parquet"));
      for (int i = 0; i < 9; ++i) {
        Path subdir = new Path(dir, "subdir" + i);
        fs.mkdirs(subdir);

        FSDataOutputStream os = fs.create(new Path(subdir, "nation.parquet"));
        os.write(bytes);
        os.close();
      }

      // query on all 9 files.
      testBuilder()
        .sqlQuery(
          "select count(*) c from dfs.tmp.parquet_scan_dir_refresh where length(n_comment) > 0")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(225L)
        .build()
        .run();

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // delete every third subdir.
      for (int i = 0; i < 9; ++i) {
        if (i % 3 == 0) {
          fs.delete(new Path(dir, "subdir" + i), true);
        }
      }

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // re-run the query. Should trigger a metadata refresh and succeed.
      testBuilder()
        .sqlQuery(
          "select count(*) c from dfs.tmp.parquet_scan_dir_refresh where length(n_comment) > 0")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(150L)
        .build()
        .run();

      // cleanup
      fs.delete(dir, true);
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testMaxFooterSizeLimit() throws Exception {
    final String sql = "select count(*) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/datapage_v2.snappy.parquet\"";
    test("ALTER SYSTEM SET \"" + ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getOptionName() + "\" = 32");

    try {
      testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5L)
        .build()
        .run();
      Assert.fail("Did not throw expected footer size exception!");
    } catch (Exception e) {
      System.out.println("Encountered footer size exception");
    } finally {
      test("ALTER SYSTEM RESET \"" + ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR.getOptionName() + "\"");
    }
  }

  @Test
  public void testDX15475() throws Exception {
    final String sql = "select count(*) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/datapage_v2.snappy.parquet\"";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(5L)
      .build()
      .run();
  }

  @Test
  public void testEmptyParquetFile() throws Exception {
    final String sql = "select * from dfs.\"${WORKING_PATH}/src/test/resources/zero-rows.parquet\"";
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("ad_id", "creative_id", "url_tags", "load_date")
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void testNoExtParquetFile() throws Exception {
    final String sql = "select count(*) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/parquet/parquet_without_ext_dir/no_extension\"";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(5L)
      .build()
      .run();
  }

  @Test
  public void testNoExtParquetDir() throws Exception {
    final String sql = "select count(*) as cnt from dfs.\"${WORKING_PATH}/src/test/resources/parquet/parquet_without_ext_dir/no_extension_dir\"";

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(5L)
      .build()
      .run();
  }

  @Test
  public void testParquetRecordReaderWithPruning() throws Exception {
    testBuilder()
            .optionSettingQueriesForTestQuery(DISABLE_VECTORIZED_READ, false)
            .sqlQuery("select int_col c from cp.\"parquet/alltypes_required.parquet\" where int_col > 1")
            .unOrdered()
            .expectsEmptyResultSet()
            .build()
            .run();
  }

}
