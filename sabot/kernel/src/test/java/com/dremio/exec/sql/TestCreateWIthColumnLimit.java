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
package com.dremio.exec.sql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserException;

public class TestCreateWIthColumnLimit extends PlanTestBase {

  @Before
  public void setUp() throws Exception {
    test("ALTER SYSTEM SET \"store.plugin.max_metadata_leaf_columns\" = 2");
  }

  @After
  public void cleanUp() throws Exception {
    test("ALTER SYSTEM RESET \"store.plugin.max_metadata_leaf_columns\"");
  }

  @Test
  public void columnLimitExceededSelectStar() throws Exception {
    String tableName = "columnLimit1";
    String ctasQuery = "create table " + TEMP_SCHEMA + "." + tableName + " AS select * from INFORMATION_SCHEMA.CATALOGS";
    try {
      test(ctasQuery);
      fail("query should have failed");
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("exceeded the maximum number of fields of 2"));
    }
  }

  @Test
  public void columnLimitExceededSelectCols() throws Exception {
    String tableName = "columnLimit2";
    String ctasQuery = "create table " + TEMP_SCHEMA + "." + tableName + " AS select CATALOG_NAME, CATALOG_DESCRIPTION, CATALOG_CONNECT from INFORMATION_SCHEMA.CATALOGS";
    try {
      test(ctasQuery);
      fail("query should have failed");
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("exceeded the maximum number of fields of 2"));
    }
  }

  @Test
  public void columnLimitExceededSelectLessColsShouldPass() throws Exception {
    String ctasQuery = "create table " + TEMP_SCHEMA + ".columnLimit3 AS select CATALOG_NAME, CATALOG_DESCRIPTION from INFORMATION_SCHEMA.CATALOGS";
    test(ctasQuery);
  }

  @Test
  public void columnLimitExceededCreateEmpty() throws Exception {
    final String tableName = "columnLimit4";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." + tableName + "(id int, id1 int, id2 int)";
      test(createCommandSql);
      fail("query should have failed");
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("exceeded the maximum number of fields of 2"));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
    }
  }

}
