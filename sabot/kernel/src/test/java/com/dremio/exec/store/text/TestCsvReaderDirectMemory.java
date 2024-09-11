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

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class TestCsvReaderDirectMemory extends BaseTestQuery {

  String root;

  @Before
  public void initialize() throws Exception {
    root = FileUtils.getResourceAsFile("/store/text/data/large.csvh").getAbsolutePath();
    test("alter session set \"exec.errors.verbose\" = true ");
  }

  @Test
  public void testReadLargeStrings() throws Exception {
    String query =
        String.format(
            "select count(*) as rc from TABLE(dfs_root.\"%s\"(type => 'TEXT', fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true))",
            root);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("rc").baselineValues(499L).go();
  }

  @Test
  public void testReadLargeStringsContent1() throws Exception {
    String cond = "position('Hello World 64' in col0) > 0";
    String query =
        String.format(
            "select count(*) as rc from TABLE(dfs_root.\"%s\"(type => 'TEXT', fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true)) where %s",
            root, cond);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("rc").baselineValues(483L).go();
  }

  @Test
  public void testReadLargeStringsContent2() throws Exception {
    String cond = "position('Hello World 94' in col0) > 0";
    String query =
        String.format(
            "select count(*) as rc from TABLE(dfs_root.\"%s\"(type => 'TEXT', fieldDelimiter => ',', lineDelimiter => '\n', extractHeader => true)) where %s",
            root, cond);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("rc").baselineValues(3L).go();
  }
}
