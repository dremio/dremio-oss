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
package com.dremio.exec.store.json;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.io.Resources;

public class TestJsonRecordReader extends BaseTestQuery {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonRecordReader.class);

  static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
  }

  @Test
  public void testComplexJsonInput() throws Exception {
//  test("select z[0]['orange']  from cp.\"jsoninput/input2.json\" limit 10");
    test("select \"integer\", x['y'] as x1, x['y'] as x2, z[0], z[0]['orange'], z[1]['pink']  from cp.\"jsoninput/input2.json\" limit 10 ");
//    test("select x from cp.\"jsoninput/input2.json\"");

//    test("select z[0]  from cp.\"jsoninput/input2.json\" limit 10");
  }

  @Test
  public void testContainingArray() throws Exception {
    test("select * from dfs.\"${WORKING_PATH}/src/test/resources/store/json/listdoc.json\"");
  }

  @Test
  public void testComplexMultipleTimes() throws Exception {
    for(int i =0 ; i < 5; i++) {
    test("select * from cp.\"join/merge_join.json\"");
    }
  }

  @Test
  public void trySimpleQueryWithLimit() throws Exception {
    test("select * from cp.\"limit/test1.json\" limit 10");
  }

  @Test// DRILL-1634 : retrieve an element in a nested array in a repeated map.  RepeatedMap (Repeated List (Repeated varchar))
  public void testNestedArrayInRepeatedMap() throws Exception {
    test("select a[0].b[0] from cp.\"jsoninput/nestedArray.json\"");
    test("select a[0].b[1] from cp.\"jsoninput/nestedArray.json\"");
    test("select a[1].b[1] from cp.\"jsoninput/nestedArray.json\"");  // index out of the range. Should return empty list.
  }

  @Test
  public void testEmptyMapDoesNotFailValueCapacityCheck() throws Exception {
    final String sql = "select * from cp.\"store/json/value-capacity.json\"";
    test(sql);
  }

  @Test
  @Ignore
  public void testEnableAllTextMode() throws Exception {
    testNoResult("alter session set \"store.json.all_text_mode\"= true");
    test("select * from cp.\"jsoninput/big_numeric.json\"");
    testNoResult("alter session set \"store.json.all_text_mode\"= false");
  }

  @Test
  @Ignore
  public void testExceptionHandling() throws Exception {
    try {
      test("select * from cp.\"jsoninput/DRILL-2350.json\"");
    } catch(UserException e) {
      Assert.assertEquals(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, e.getOrCreatePBError(false).getErrorType());
      String s = e.getMessage();
      assertEquals("Expected Unsupported Operation Exception.", true, s.contains("Dremio does not support lists of different types."));
    }

  }

  @Test //DRILL-1832
  @Ignore("update baseline")
  public void testJsonWithNulls1() throws Exception {
    final String query="select * from cp.\"jsoninput/twitter_43.json\"";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("jsoninput/drill-1832-1-result.json")
            .go();
  }

  @Test //DRILL-1832
  @Ignore
  public void testJsonWithNulls2() throws Exception {
    final String query="select SUM(1) as \"sum_Number_of_Records_ok\" from cp.\"/jsoninput/twitter_43.json\" having (COUNT(1) > 0)";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("jsoninput/drill-1832-2-result.json")
            .go();
  }

  @Test
  @Ignore("issue with resubmitting CTAS queries")
  public void drill_3353() throws Exception {
    try {
//      testNoResult("alter session set \"store.json.all_text_mode\" = true");
      test("create table dfs_test.tmp.drill_3353 as select a from dfs.\"${WORKING_PATH}/src/test/resources/jsoninput/drill_3353\" where e = true");
      String query = "select t.a.d cnt from dfs_test.tmp.drill_3353 t where t.a.d is not null";
      test(query);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(1l)
          .go();
    } finally {
      testNoResult("alter session set \"store.json.all_text_mode\" = false");
    }
  }

  @Test // See DRILL-3476
  public void testNestedFilter() throws Exception {
    String query = "select a from cp.\"jsoninput/nestedFilter.json\" t where t.a.b = 1";
    String baselineQuery = "select * from cp.\"jsoninput/nestedFilter.json\" t where t.a.b = 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery(baselineQuery)
        .go();
  }

  @Test
  public void testRefreshOnJsonFileNotFound() throws Exception {
    setEnableReAttempts(true);
    try {
      // create directory
      Path dir = new Path("/tmp/json_file_refresh");
      if (fs.exists(dir)) {
        fs.delete(dir, true);
      }
      fs.mkdirs(dir);

      // Create 2 json files in the directory.
      byte[] bytes = Resources.toByteArray(Resources.getResource("store/text/sample.json"));
      for (int i = 0; i < 2; ++i) {
        FSDataOutputStream os = fs.create(new Path(dir, i + "sample.json"));
        os.write(bytes);
        os.close();
      }

      // query on all 10 files.
      testBuilder()
        .sqlQuery(
          "select count(*) c from dfs.tmp.json_file_refresh where type = \'donut\'")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // delete the second file.
      fs.delete(new Path(dir, 1 + "sample.json"), false);

      // TODO(DX-15645): remove this sleep
      Thread.sleep(1000L); // fs modification times have second precision so read signature might be valid

      // re-run the query. Should trigger a metadata refresh and succeed.
      testBuilder()
        .sqlQuery(
          "select count(*) c from dfs.tmp.json_file_refresh where type = \'donut\'")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();

      // cleanup
      fs.delete(dir, true);
    } finally {
      setEnableReAttempts(false);
    }
  }
}
