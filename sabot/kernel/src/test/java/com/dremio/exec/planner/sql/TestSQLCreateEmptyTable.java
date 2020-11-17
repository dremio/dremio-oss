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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.google.common.collect.Sets;


public class TestSQLCreateEmptyTable {
  private ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Test
  public void testCreateTableNoColumns() {
    String sql = "CREATE TABLE newTbl";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Columns/Fields not specified for table.", ex.getMessage());
    }
  }

  @Test
  public void testCreateTableNoDatatypes() {
    String sql = "CREATE TABLE newTbl(id , name)";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Datatype not specified for some columns.", ex.getMessage());
    }

    sql = "CREATE TABLE newTbl(id int, name)";
    try {
      SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ex) {
      Assert.assertEquals("Datatype not specified for some columns.", ex.getMessage());
    }
  }

  @Test
  public void testCreateTableValidDatatypes() {
    String sql = "CREATE TABLE newTbl(id int, name varchar)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(new String[]{"`id` INTEGER", "`name` VARCHAR"},
      sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
  }

  @Test
  public void testCreateTableValidDatatypesWithPartitions() {
    String sql = "CREATE TABLE newTbl(id int, name varchar) PARTITION BY (name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(new String[]{"`id` INTEGER", "`name` VARCHAR"},
      sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
    Assert.assertArrayEquals(new String[]{"name"}, sqlCreateEmptyTable.getPartitionColumns(null, null).toArray(new String[0]));
  }

  @Test
  public void testCreateTableDecimalDatatype() {
    String sql = "CREATE TABLE newTbl(id DECIMAL(38, 2), name varchar) PARTITION BY (name)";
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER_DDL)));
    SqlCreateEmptyTable sqlCreateEmptyTable = (SqlCreateEmptyTable) sqlNode;
    Assert.assertArrayEquals(new String[]{"`id` DECIMAL(38, 2)", "`name` VARCHAR"},
      sqlCreateEmptyTable.getFieldNames().toArray(new String[0]));
    Assert.assertArrayEquals(new String[]{"name"}, sqlCreateEmptyTable.getPartitionColumns(null, null).toArray(new String[0]));
  }

}
