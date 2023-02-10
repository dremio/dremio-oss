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
package com.dremio;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.work.ExecErrorConstants;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedDataTypeException;
import com.dremio.exec.work.foreman.UnsupportedFunctionException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;

public class TestDisabledFunctionality extends BaseTestQuery{

  @Test(expected = UserException.class)  // see DRILL-2054
  public void testBooleanORWhereClause() throws Exception {
    test("select * from cp.\"tpch/nation.parquet\" where (true || true) ");
  }

  @Test(expected = UserException.class)  // see DRILL-2054
  public void testBooleanAND() throws Exception {
    test("select true && true from cp.\"tpch/nation.parquet\" ");
  }

  private static void throwAsUnsupportedException(UserException ex) throws Exception {
    SqlUnsupportedException.errorClassNameToException(ex.getOrCreatePBError(false).getException().getExceptionClass());
    throw ex;
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledIntersectALL() throws Exception {
    try {
      test("(select n_name as name from cp.\"tpch/nation.parquet\") INTERSECT ALL (select r_name as name from cp.\"tpch/region.parquet\")");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledExceptALL() throws Exception {
    try {
      test("(select n_name as name from cp.\"tpch/nation.parquet\") EXCEPT ALL (select r_name as name from cp.\"tpch/region.parquet\")");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledNaturalJoin() throws Exception {
    try {
      test("select * from cp.\"tpch/nation.parquet\" NATURAL JOIN cp.\"tpch/region.parquet\"");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastTINYINT() throws Exception {
    try {
      test("select cast(n_name as tinyint) from cp.\"tpch/nation.parquet\";");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastSMALLINT() throws Exception {
    try {
      test("select cast(n_name as smallint) from cp.\"tpch/nation.parquet\";");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastREAL() throws Exception {
    try {
      test("select cast(n_name as real) from cp.\"tpch/nation.parquet\";");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UserRemoteException.class) // see DRILL-2115
  public void testDisabledCardinality() throws Exception {
    try {
      test("select cardinality(employee_id) from cp.\"employee.json\";");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // DRILL-2068
  @Ignore
  public void testImplicitCartesianJoin() throws Exception {
    try {
      test("select a.*, b.user_port " +
          "from cp.\"employee.json\" a, sys.nodes b;");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2068, DRILL-1325
  @Ignore
  public void testNonEqualJoin() throws Exception {
    try {
      test("select a.*, b.user_port " +
          "from cp.\"employee.json\" a, sys.nodes b " +
          "where a.position_id <> b.user_port;");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2068, DRILL-1325
  @Ignore
  public void testMultipleJoinsWithOneNonEqualJoin() throws Exception {
    try {
      test("select a.last_name, b.n_name, c.r_name " +
          "from cp.\"employee.json\" a, cp.\"tpch/nation.parquet\" b, cp.\"tpch/region.parquet\" c " +
          "where a.position_id > b.n_nationKey and b.n_nationKey = c.r_regionkey;");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see  DRILL-2068, DRILL-1325
  @Ignore
  public void testLeftOuterJoin() throws Exception {
    try {
      test("select a.last_name, b.n_name " +
          "from cp.\"employee.json\" a LEFT JOIN cp.\"tpch/nation.parquet\" b " +
          "ON a.position_id > b.n_nationKey;");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2068, DRILL-1325
  @Ignore
  public void testInnerJoin() throws Exception {
    try {
      test("select a.last_name, b.n_name " +
          "from cp.\"employee.json\" a INNER JOIN cp.\"tpch/nation.parquet\" b " +
          "ON a.position_id > b.n_nationKey;");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2068, DRILL-1325
  @Ignore
  public void testExplainPlanForCartesianJoin() throws Exception {
    try {
      test("explain plan for (select a.last_name, b.n_name " +
          "from cp.\"employee.json\" a INNER JOIN cp.\"tpch/nation.parquet\" b " +
          "ON a.position_id > b.n_nationKey);");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2441
  @Ignore
  public void testExplainPlanOuterJoinWithInequality() throws Exception {
    try {
      test("explain plan for (select a.last_name, b.n_name " +
          "from cp.\"employee.json\" a LEFT OUTER JOIN cp.\"tpch/nation.parquet\" b " +
          "ON (a.position_id > b.n_nationKey AND a.employee_id = b.n_regionkey));");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-2441
  @Ignore
  public void testOuterJoinWithInequality() throws Exception {
    try {
      test("select a.last_name, b.n_name " +
          "from cp.\"employee.json\" a RIGHT OUTER JOIN cp.\"tpch/nation.parquet\" b " +
          "ON (a.position_id > b.n_nationKey AND a.employee_id = b.n_regionkey);");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // see DRILL-2181
  public void testFlattenWithinGroupBy() throws Exception {
    try {
      String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
      String query = String.format("select flatten(j.topping) tt " +
          "from dfs.\"%s\" j " +
          "group by flatten(j.topping)", root);

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // see DRILL-2181
  public void testFlattenWithinOrderBy() throws Exception {
    try {
      String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
      String query = String.format("select flatten(j.topping) tt " +
          "from dfs.\"%s\" j " +
          "order by flatten(j.topping)", root);

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // see DRILL-2181
  public void testFlattenWithinAggFunction() throws Exception {
    try {
      String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
      String query = String.format("select count(flatten(j.topping)) tt " +
          "from dfs.\"%s\" j", root);

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // see DRILL-2181
  public void testFlattenWithinDistinct() throws Exception {
    try {
      String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
      String query = String.format("select Distinct (flatten(j.topping)) tt " +
          "from dfs.\"%s\" j", root);

      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test // DRILL-2848
  @Ignore("decimal")
  public void testDisableDecimalCasts() throws Exception {
    final String query = "select cast('1.2' as decimal(9, 2)) from cp.\"employee.json\" limit 1";
    errorMsgTestHelper(query, ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG);
  }

  @Test // DRILL-2848
  @Ignore("decimal")
  public void testDisableDecimalFromParquet() throws Exception {
    final String query = "select * from cp.\"parquet/decimal_dictionary.parquet\"";
    errorMsgTestHelper(query, ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG);
  }

  @Test (expected = UnsupportedFunctionException.class) //DRILL-3802
  public void testDisableGroup_ID() throws Exception{
    try {
      final String query = "select n_regionkey, count(*), GROUP_ID() from cp.\"tpch/nation.parquet\" group by n_regionkey;";
      test(query);
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }
}
