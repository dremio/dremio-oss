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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.sql.parser.SqlCreateFunction;
import com.dremio.exec.planner.sql.parser.SqlDropFunction;

public class TestUDFParsing {

  @Test
  public void createFunctionWithSelect() throws Exception {
    testCreate("CREATE FUNCTION\n" +
                    "    protect_ssn (val VARCHAR)\n" +
                    "    RETURNS VARCHAR\n" +
                    "    RETURN\n" +
                    "        SELECT\n" +
                    "            CASE \n" +
                    "                WHEN query_user() IN ('dave','mike') \n" +
                    "                    OR is_member('Accounting') THEN val\n" +
                    "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
                    "            END\n",
            "protect_ssn",
            "SELECT CASE WHEN `query_user`() IN ('dave', 'mike') OR `is_member`('Accounting') THEN `val` ELSE `CONCAT`('XXX-XX-', `SUBSTR`(`val`, 8, 4)) END",
            "(`val`, VARCHAR)",
            "VARCHAR",
            false, null);
  }

  @Test
  public void createFunctionWithExpression() throws Exception {
    testCreate("CREATE FUNCTION\n" +
        "    protect_ssn (val VARCHAR)\n" +
        "    RETURNS VARCHAR\n" +
        "    RETURN\n" +
        "            CASE \n" +
        "                WHEN query_user() IN ('dave','mike') \n" +
        "                    OR is_member('Accounting') THEN val\n" +
        "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
        "            END\n",
      "protect_ssn",
      "CASE WHEN `query_user`() IN ('dave', 'mike') OR `is_member`('Accounting') THEN `val` ELSE `CONCAT`('XXX-XX-', `SUBSTR`(`val`, 8, 4)) END",
      "(`val`, VARCHAR)",
      "VARCHAR",
      false, null);
  }

  @Test
  public void createFunctionOrReplaceWithSelect() throws Exception {
    testCreate("CREATE OR REPLACE FUNCTION\n" +
            "    protect_ssn (val VARCHAR)\n" +
            "    RETURNS VARCHAR\n" +
            "    RETURN\n" +
            "        SELECT\n" +
            "            CASE \n" +
            "                WHEN query_user() IN ('dave','mike') \n" +
            "                    OR is_member('Accounting') THEN val\n" +
            "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
            "            END\n",
            "protect_ssn",
            "SELECT CASE WHEN `query_user`() IN ('dave', 'mike') OR `is_member`('Accounting') THEN `val` ELSE `CONCAT`('XXX-XX-', `SUBSTR`(`val`, 8, 4)) END",
            "(`val`, VARCHAR)",
            "VARCHAR",
            true, null);
  }

  @Test
  public void createFunctionWithDefault() throws Exception {
    testCreate("CREATE FUNCTION\n" +
        "    protect_ssn (val VARCHAR default (select 1 from foo), val2 VARCHAR default '1')\n" +
        "    RETURNS VARCHAR\n" +
        "    RETURN\n" +
        "        SELECT\n" +
        "            CASE \n" +
        "                WHEN query_user() IN ('dave','mike') \n" +
        "                    OR is_member('Accounting') THEN val\n" +
        "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
        "            END\n",
      "protect_ssn",
      "SELECT CASE WHEN `query_user`() IN ('dave', 'mike') OR `is_member`('Accounting') THEN `val` ELSE `CONCAT`('XXX-XX-', `SUBSTR`(`val`, 8, 4)) END",
      "(`val`, VARCHAR DEFAULT (SELECT 1\n" +
        "FROM `foo`)), (`val2`, VARCHAR DEFAULT '1')",
      "VARCHAR",
      false, null);
  }

  @Test
  public void createTabularFunction() throws Exception {
    testCreate("CREATE FUNCTION\n" +
        "    tabular_UDF (val VARCHAR)\n" +
        "    RETURNS TABLE (a VARCHAR)\n" +
        "    RETURN SELECT '1'",
      "tabular_UDF",
      "SELECT '1'",
      "(`val`, VARCHAR)",
      null,
      false,
      "(`a` VARCHAR)");
  }

  @Test
  public void createTabularFunctionWithDefault() throws Exception {
    testCreate("CREATE FUNCTION\n" +
        "    tabular_UDF (val VARCHAR default select 1 from foo)\n" +
        "    RETURNS TABLE (a VARCHAR, b BOOLEAN, c INT)\n" +
        "    RETURN SELECT '1'",
      "tabular_UDF",
      "SELECT '1'",
      "(`val`, VARCHAR DEFAULT (SELECT 1\n" +
        "FROM `foo`))",
      null,
      false,
      "(`a` VARCHAR, `b` BOOLEAN, `c` INT)");
  }

  @Test
  public void testDropFunction() throws Exception {
    SqlDropFunction sqlDropFunctionResult = parse(SqlDropFunction.class,
      "DROP FUNCTION protect_ssn");
    Assert.assertEquals("function name doesn't match", "protect_ssn", sqlDropFunctionResult.getName().toString());
    Assert.assertFalse("If exists is set", sqlDropFunctionResult.isIfExists());
  }

  @Test
  public void testDropFunctionIfExists() throws Exception {
    SqlDropFunction sqlDropFunctionResult = parse(SqlDropFunction.class,
        "DROP FUNCTION IF EXISTS protect_ssn");
    Assert.assertEquals("function name doesn't match", "protect_ssn", sqlDropFunctionResult.getName().toString());
    Assert.assertTrue("If exists is not set", sqlDropFunctionResult.isIfExists());

  }

  private void testCreate(String command, String fName, String exp, String fieldList, String returnType, boolean replacePolicy, String tabularReturnType) throws Exception {
    validateCreate(parse(SqlCreateFunction.class, command), fName, exp, fieldList, returnType, replacePolicy, tabularReturnType);
  }

  private <SQL_NODE extends SqlNode> SQL_NODE parse(Class<SQL_NODE> nodeType, String command) throws Exception {
    ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 128, true);
    SqlParser parser = SqlParser.create(command, parserConfig);
    SqlNode sqlNode = parser.parseStmt();
    Assert.assertTrue(nodeType.isInstance(sqlNode));
    return nodeType.cast(sqlNode);
  }

  private void validateCreate(SqlCreateFunction sqlCreateFunctionResult, String fName, String exp, String fieldList, String scalarReturnType, boolean replacePolicy, String tabularReturnType) {
    Assert.assertEquals("function name doesn't match", fName, sqlCreateFunctionResult.getName().toString());
    Assert.assertEquals("function body doesn't match", exp, sqlCreateFunctionResult.getExpression().toString());
    Assert.assertEquals("function arguments doesn't match", fieldList, sqlCreateFunctionResult.getFieldList().toString());
    if (scalarReturnType == null) {
      Assert.assertNull("function return type doesn't match", sqlCreateFunctionResult.getScalarReturnType());
    } else {
      Assert.assertEquals("function return type doesn't match", scalarReturnType, sqlCreateFunctionResult.getScalarReturnType().toString());
    }
    Assert.assertEquals("shouldReplace doesn't match", replacePolicy, sqlCreateFunctionResult.shouldReplace());
  }
}
