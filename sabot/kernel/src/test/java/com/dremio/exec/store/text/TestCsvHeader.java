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
package com.dremio.exec.store.text;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.util.FileUtils;

public class TestCsvHeader extends BaseTestQuery{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCsvHeader.class);
  String root;

  @Before
  public void initialize() throws Exception {
    root = FileUtils.getResourceAsFile("/store/text/data/cars.csvh").toURI().toString();
    test("alter session set \"exec.errors.verbose\" = true ");
  }

  @Test //DRILL-951
  public void testCsvWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select * from dfs.\"%s\" LIMIT 2
    String query = String.format("select * from dfs.\"%s\" LIMIT 2", root);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Make", "Model", "Description", "Price")
            .baselineValues("1997", "Ford", "E350", "ac, abs, moon", "3000.00")
            .baselineValues("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00")
            .go();
  }

  @Test //DRILL-951
  public void testCsvWhereWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select * from dfs.\"%s\" where columns[1] = 'Chevy'
    String query = String.format("select * from dfs.\"%s\" where Make = 'Chevy'", root);

    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Make", "Model", "Description", "Price")
            .baselineValues("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00")
            .baselineValues("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", "", "5000.00")
            .go();
  }

  @Test //DRILL-951
  public void testCsvStarPlusWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select *,columns[1] from dfs.\"%s\" where columns[1] = 'Chevy'
    String query = String.format("select *, Make from dfs.\"%s\" where Make = 'Chevy'", root);

    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Make", "Model", "Description", "Price", "Make5")
            .baselineValues("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00", "Chevy")
            .baselineValues("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", "", "5000.00", "Chevy")
            .go();
  }

  @Test //DRILL-951
  public void testCsvWhereColumnsWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select columns[1] from dfs.\"%s\" where columns[1] = 'Chevy'
    String query = String.format("select Make from dfs.\"%s\" where Make = 'Chevy'", root);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Make")
            .baselineValues("Chevy")
            .baselineValues("Chevy")
            .go();
  }

  @Test //DRILL-951
  public void testCsvColumnsWithHeader() throws Exception {
    //Pre DRILL-951: Qry: select columns[0],columns[2],columns[4] from dfs.\"%s\" where columns[1] = 'Chevy'
    String query = String.format("select \"Year\", Model, Price from dfs.\"%s\" where Make = 'Chevy'", root);

    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Model", "Price")
            .baselineValues("1999", "Venture \"Extended Edition\"", "4900.00")
            .baselineValues("1999", "Venture \"Extended Edition, Very Large\"", "5000.00")
            .go();
  }

  @Test //DRILL-951
  public void testCsvHeaderShortCircuitReads() throws Exception {
    String query = String.format("select \"Year\", Model from dfs.\"%s\" where Make = 'Chevy'", root);

    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Model")
            .baselineValues("1999", "Venture \"Extended Edition\"")
            .baselineValues("1999", "Venture \"Extended Edition, Very Large\"")
            .go();
  }

  @Test //DRILL-4108
  @Ignore("not supported")
  public void testCsvHeaderNonExistingColumn() throws Exception {
    String query = String.format("select \"Year\", Model, Category from dfs.\"%s\" where Make = 'Chevy'", root);

    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Model", "Category")
            .baselineValues("1999", "Venture \"Extended Edition\"", "")
            .baselineValues("1999", "Venture \"Extended Edition, Very Large\"", "")
            .go();
  }

  @Test //DRILL-4108
  @Ignore("not supported currently")
  public void testCsvHeaderMismatch() throws Exception {
    String ddir = FileUtils.getResourceAsFile("/store/text/data/d2").toURI().toString();
    String query = String.format("select \"Year\", Model, Category from dfs.\"%s\" where Make = 'Chevy'", ddir);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Model", "Category")
            .baselineValues("1999", "", "Venture \"Extended Edition\"")
            .baselineValues("1999", "", "Venture \"Extended Edition, Very Large\"")
            .baselineValues("1999", "Venture \"Extended Edition\"", "")
            .baselineValues("1999", "Venture \"Extended Edition, Very Large\"", "")
            .go();
  }

  @Test //DRILL-4108
  public void testCsvHeaderSkipFirstLine() throws Exception {
    // test that header is not skipped when skipFirstLine is true
    // testing by defining new format plugin with skipFirstLine set to true and diff file extension
    String dfile = FileUtils.getResourceAsFile("/store/text/data/cars.csvh-test").getAbsolutePath();
    String query = String.format("select \"Year\", Model from TABLE(dfs_root.\"%s\"(type => 'TEXT', fieldDelimiter => ',', lineDelimiter => '\n', skipFirstLine => true, extractHeader => true)) where Make = 'Chevy'", dfile);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("Year", "Model")
            .baselineValues("1999", "Venture \"Extended Edition\"")
            .baselineValues("1999", "Venture \"Extended Edition, Very Large\"")
            .go();
  }

  @Test
  public void testEmptyFinalColumn() throws Exception {
    String dfs_temp = getDfsTestTmpSchemaLocation();
    File table_dir = new File(dfs_temp, "emptyFinalColumn");
    table_dir.mkdir();
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.csvh")));
    os.write("field1,field2\n".getBytes());
    for (int i = 0; i < 10000; i++) {
      os.write("a,\n".getBytes());
    }
    os.flush();
    os.close();
    String query = "select * from dfs_test.emptyFinalColumn";
      TestBuilder builder = testBuilder()
              .sqlQuery(query)
              .ordered()
              .baselineColumns("field1", "field2");
      for (int i = 0; i < 10000; i++) {
        builder.baselineValues("a", "");
      }
      builder.go();
  }

  @Test
  public void testCsvWithHeaderEmptyAndDuplicateColumnNames() throws Exception {
    String file = FileUtils.getResourceAsFile("/store/text/data/cars-badheaders.csvh").toURI().toString();
    String query = String.format("select * from dfs.\"%s\" LIMIT 2", file);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("Year", "Make", "Price", "D", "Price0")
      .baselineValues("1997", "Ford", "E350", "ac, abs, moon", "3000.00")
      .baselineValues("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00")
      .go();
  }
}
