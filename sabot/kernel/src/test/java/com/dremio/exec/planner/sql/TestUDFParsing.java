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
            false);
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
      false);
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
            true);
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

  private void testCreate(String command, String fName, String exp, String fieldList, String returnType, boolean replacePolicy) throws Exception {
    validateCreate(parse(SqlCreateFunction.class, command), fName, exp, fieldList, returnType, replacePolicy);
  }

  private <SQL_NODE extends SqlNode> SQL_NODE parse(Class<SQL_NODE> nodeType, String command) throws Exception {
    ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 128, true);
    SqlParser parser = SqlParser.create(command, parserConfig);
    SqlNode sqlNode = parser.parseStmt();
    Assert.assertTrue(nodeType.isInstance(sqlNode));
    return nodeType.cast(sqlNode);
  }

  private void validateCreate(SqlCreateFunction sqlCreateFunctionResult, String fName, String exp, String fieldList, String returnType, boolean replacePolicy) {
    Assert.assertEquals("function name doesn't match", fName, sqlCreateFunctionResult.getName().toString());
    Assert.assertEquals("function body doesn't match", exp, sqlCreateFunctionResult.getExpression().toString());
    Assert.assertEquals("function arguments doesn't match", fieldList, sqlCreateFunctionResult.getFieldList().toString());
    Assert.assertEquals("function return type doesn't match", returnType, sqlCreateFunctionResult.getReturnType().toString());
    Assert.assertEquals("shouldReplace doesn't match", replacePolicy, sqlCreateFunctionResult.shouldReplace());
  }
}
