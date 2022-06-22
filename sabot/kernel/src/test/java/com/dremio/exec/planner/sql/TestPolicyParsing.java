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

import java.util.List;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.sql.parser.SqlAlterTableAddRowAccessPolicy;
import com.dremio.exec.planner.sql.parser.SqlAlterTableDropRowAccessPolicy;
import com.dremio.exec.planner.sql.parser.SqlAlterTableSetColumnMasking;
import com.dremio.exec.planner.sql.parser.SqlAlterTableUnsetColumnMasking;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.google.common.collect.ImmutableList;

/**
 * TestPolicyParsing
 */
public class TestPolicyParsing {

  @Test
  public void createTableWithColumnMasking() throws Exception {
    testCreateTable("CREATE TABLE ss.crm.dbo.employees (\n" +
        "     ssn_col VARCHAR MASKING POLICY protect_ssn (ssn_col, region)\n" +
        "    ,region VARCHAR\n" +
        "    ,state_col VARCHAR)\n",
      ImmutableList.of(
        "`ssn_col` VARCHAR `protect_ssn` (`ssn_col`, `region`)",
        "`region` VARCHAR",
        "`state_col` VARCHAR"),
      null
    );
  }

  @Test
  public void alterTableSetColumnMasking() throws Exception {
    testAlterTableSet("ALTER TABLE ss.crm.dbo.employees \n" +
        "   MODIFY COLUMN ssn_col\n" +
        "   SET MASKING POLICY protect_ssn (ssn_col, region)\n",
      "ssn_col",
      "protect_ssn (ssn_col, region)"
    );
  }

  @Test
  public void createTableWithRowPolicy() throws Exception {
    testCreateTable("CREATE TABLE ss.crm.dbo.employees (\n" +
        "     ssn_col VARCHAR\n" +
        "    ,region VARCHAR\n" +
        "    ,state_col VARCHAR)\n" +
        "     ROW ACCESS POLICY state_policy ( state_col )",
      null,
      "`state_policy` (`state_col`)"
    );
  }

  @Test
  public void alterTableAddRowAccessPolicy() throws Exception {
    testAlterTableAdd("ALTER TABLE ss.crm.dbo.sales_data \n" +
        "   ADD ROW ACCESS POLICY state_policy ( state_col )\n",
      "state_policy ( state_col )"
    );
  }

  @Test
  public void createTableWithColumnMaskingAndRowPolicy() throws Exception {
    testCreateTable("CREATE TABLE ss.crm.dbo.employees (\n" +
        "     ssn_col VARCHAR MASKING POLICY protect_ssn (ssn_col, region)\n" +
        "    ,region VARCHAR\n" +
        "    ,state_col VARCHAR)\n" +
        "     ROW ACCESS POLICY state_policy ( state_col )",
      ImmutableList.of(
        "`ssn_col` VARCHAR `protect_ssn` (`ssn_col`, `region`)",
        "`region` VARCHAR",
        "`state_col` VARCHAR"),
      "`state_policy` (`state_col`)"
    );
  }

  @Test
  public void createViewWithColumnMasking() throws Exception {
    testCreateView("CREATE VIEW ss.crm.dbo.employees (\n" +
        "     ssn_col MASKING POLICY protect_ssn (ssn_col, region)\n" +
        "    ,region\n" +
        "    ,state_col)\n" +
        "     as select a, b, c from some_table",
      ImmutableList.of(
        "`ssn_col` protect_ssn (ssn_col, region)",
        "region",
        "state_col"),
      null
    );
  }

  @Test
  public void createViewWithColumnMaskingAndRowPolicy() throws Exception {
    testCreateView("CREATE VIEW ss.crm.dbo.employees (\n" +
        "     ssn_col MASKING POLICY protect_ssn (ssn_col, region)\n" +
        "    ,region\n" +
        "    ,state_col)\n" +
        "     ROW ACCESS POLICY state_policy ( state_col ) as select a, b, c from some_table",
      ImmutableList.of(
         "`ssn_col` protect_ssn (ssn_col, region)",
        "region",
        "state_col"),
      "state_policy ( state_col )"
    );
  }

  @Test
  public void UnsetMasking() throws Exception {
    testAlterTableUnset("ALTER TABLE ss.crm.dbo.employees \n" +
        "   MODIFY COLUMN ssn_col\n" +
        "   UNSET MASKING POLICY protect_ssn\n",
      "ssn_col"
    );
  }

  @Test
  public void dropRowPolicy() throws Exception {
    testAlterTableDrop("ALTER TABLE ss.crm.dbo.employees \n" +
        "   DROP ROW ACCESS POLICY state_policy ( state_col )\n"
    );
  }

  private SqlNode parse(String command) throws Exception {
    ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 128, true);
    SqlParser parser = SqlParser.create(command, parserConfig);
    return parser.parseStmt();
  }

  private void testAlterTableUnset(String command, String columnName) throws Exception {
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlAlterTableUnsetColumnMasking);
    SqlAlterTableUnsetColumnMasking sqlResult = (SqlAlterTableUnsetColumnMasking) result;
    Assert.assertEquals(sqlResult.getColumnName().toString(), columnName);
  }

  private void testAlterTableDrop(String command) throws Exception {
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlAlterTableDropRowAccessPolicy);
  }

  private void testAlterTableSet(String command, String column, String masking) throws Exception {
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlAlterTableSetColumnMasking);
    SqlAlterTableSetColumnMasking sqlResult = (SqlAlterTableSetColumnMasking) result;
    Assert.assertEquals("Masking column doesn't match", column, sqlResult.getColumnName().toString());
    Assert.assertEquals("Masking policy doesn't match",
      masking.replace("\n","").replace(" ", "").replace("`", ""),
      sqlResult.getColumnMasking().toString().replace("\n","").replace(" ", "").replace("`", ""));
  }

  private void testAlterTableAdd(String command, String policy) throws Exception {
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlAlterTableAddRowAccessPolicy);
    SqlAlterTableAddRowAccessPolicy sqlResult = (SqlAlterTableAddRowAccessPolicy) result;
    Assert.assertEquals("Row policy doesn't match",
      policy.replace("\n","").replace(" ", "").replace("`", ""),
      sqlResult.getPolicy().toString().replace("\n","").replace(" ", "").replace("`", ""));
  }

  private void testCreateView(String command, List<String> expectedColumnSpec, String expectedRowPolicy) throws Exception{
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlCreateView);
    SqlCreateView sqlResult = (SqlCreateView) result;

    if (expectedColumnSpec != null) {
      List<SqlNode> fieldList = sqlResult.getFieldList().getList();
      Assert.assertEquals(expectedColumnSpec.size(), fieldList.size());
      for (int i = 0; i < fieldList.size(); i++) {
        Assert.assertEquals("Column spec doesn't match", expectedColumnSpec.get(i).replace("\n","").replace(" ", "").replace("`", ""), fieldList.get(i).toString().replace("\n","").replace(" ", "").replace("`", ""));
      }
    }

    if (expectedRowPolicy == null) {
      Assert.assertNull(sqlResult.getPolicy());
    } else {
      Assert.assertEquals("Row policy doesn't match",
        expectedRowPolicy.replace("\n","").replace(" ", "").replace("`", ""),
        sqlResult.getPolicy().toString().replace("\n","").replace(" ", "").replace("`", ""));
    }
  }

  private void testCreateTable(String command, List<String> expectedColumnSpec, String expectedRowPolicy) throws Exception{
    SqlNode result = parse(command);
    Assert.assertTrue(result instanceof SqlCreateEmptyTable);
    SqlCreateEmptyTable sqlResult = (SqlCreateEmptyTable) result;

    if (expectedColumnSpec != null) {
      List<SqlNode> fieldList = sqlResult.getFieldList().getList();
      Assert.assertEquals(expectedColumnSpec.size(), fieldList.size());
      for (int i = 0; i < fieldList.size(); i++) {
        Assert.assertEquals("Column spec doesn't match", expectedColumnSpec.get(i).toLowerCase(), fieldList.get(i).toString().toLowerCase());
      }
    }

    if (expectedRowPolicy == null) {
      Assert.assertNull(sqlResult.getPolicy());
    } else {
      Assert.assertEquals("Row policy doesn't match", expectedRowPolicy.toLowerCase(), sqlResult.getPolicy().toString().toLowerCase());
    }
  }
}
