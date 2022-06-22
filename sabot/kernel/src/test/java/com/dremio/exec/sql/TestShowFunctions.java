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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestShowFunctions extends PlanTestBase {

  private static String dropFunction = "DROP FUNCTION %s";


  private static String createUdf1 = "CREATE FUNCTION\n" +
    "    protect_ssn (val VARCHAR)\n" +
    "    RETURNS VARCHAR\n" +
    "    RETURN\n" +
    "        SELECT\n" +
    "            CASE \n" +
    "                WHEN query_user() = 'dremio' \n" +
    "                   THEN val\n" +
    "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
    "            END\n";

  private static String createUdf2 = "CREATE FUNCTION doNothello%d (val VARCHAR)\n" +
    "RETURNS int\n" +
    "RETURN %d";


  @BeforeClass
  public static void before() throws Exception{
    test(createUdf1);
    for(int i=0;i<5;i++) {
      test(String.format(createUdf2, i, i));
    }
  }

  @Test
  public void testShowFunctions() throws Exception{
    testBuilder()
      .sqlQuery("SHOW FUNCTIONS")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("protect_ssn")
      .baselineValues("doNothello0")
      .baselineValues("doNothello1")
      .baselineValues("doNothello2")
      .baselineValues("doNothello3")
      .baselineValues("doNothello4")
      .go();

  }

  @Test
  public void testShowFunctionsLike() throws Exception{
    testBuilder()
      .sqlQuery("SHOW FUNCTIONS Like 'doNothello%'")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("doNothello0")
      .baselineValues("doNothello1")
      .baselineValues("doNothello2")
      .baselineValues("doNothello3")
      .baselineValues("doNothello4")
      .go();

    testBuilder()
      .sqlQuery("SHOW FUNCTIONS Like '  doNothello%  '")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("doNothello0")
      .baselineValues("doNothello1")
      .baselineValues("doNothello2")
      .baselineValues("doNothello3")
      .baselineValues("doNothello4")
      .go();

    testBuilder()
      .sqlQuery("SHOW FUNCTIONS Like '%do%'")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("doNothello0")
      .baselineValues("doNothello1")
      .baselineValues("doNothello2")
      .baselineValues("doNothello3")
      .baselineValues("doNothello4")
      .go();

    testBuilder()
      .sqlQuery("SHOW FUNCTIONS Like 'doNothello0'")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("doNothello0")
      .go();

    testBuilder()
      .sqlQuery("SHOW FUNCTIONS Like 'protect_ssn'")
      .unOrdered()
      .baselineColumns("FUNCTION_NAME")
      .baselineValues("protect_ssn")
      .go();

  }

  @AfterClass
  public static void after() throws Exception{
    test(String.format(dropFunction, "protect_ssn"));
    for(int i=0;i<5;i++) {
      test(String.format(dropFunction, String.format("doNothello%d", i)));
    }
  }





}
