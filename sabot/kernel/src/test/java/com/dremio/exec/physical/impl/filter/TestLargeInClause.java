/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.physical.impl.filter;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

public class TestLargeInClause extends BaseTestQuery {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private static String getInIntList(int size){
    StringBuffer sb = new StringBuffer();
    for(int i =0; i < size; i++){
      if(i != 0){
        sb.append(", ");
      }
      sb.append(i);
    }
    return sb.toString();
  }

  private static String getInDateList(int size){
    StringBuffer sb = new StringBuffer();
    for(int i =0; i < size; i++){
      if(i != 0){
        sb.append(", ");
      }
      sb.append("DATE '1961-08-26'");
    }
    return sb.toString();
  }

  @Test
  public void queryWith300InConditions() throws Exception {
    test("select * from cp.\"employee.json\" where employee_id in (" + getInIntList(300) + ")");
  }

  @Test
  public void queryWith50000InConditions() throws Exception {
    test("select * from cp.\"employee.json\" where employee_id in (" + getInIntList(50000) + ")");
  }

  @Test
  public void queryWith50000DateInConditions() throws Exception {
    test("select * from cp.\"employee.json\" where cast(birth_date as date) in (" + getInDateList(500) + ")");
  }

  @Test // DRILL-3062
  public void testStringLiterals() throws Exception {
    String query = "select count(*) as cnt from (select n_name from cp.\"tpch/nation.parquet\" "
        + " where n_name in ('ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', "
        + "'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', "
        + "'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM'))";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(22l)
      .go();

  }

  @Test // DRILL-3019
  @Ignore("schema change bug")
  public void testExprsInInList() throws Exception{
    String query = "select r_regionkey \n" +
        "from cp.\"tpch/region.parquet\" \n" +
        "where r_regionkey in \n" +
        "(1, 1 + 1, 1, 1, 1, \n" +
        "1, 1 , 1, 1 , 1, \n" +
        "1, 1 , 1, 1 , 1, \n" +
        "1, 1 , 1, 1 , 1)";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("r_regionkey")
        .baselineValues(1)
        .baselineValues(2)
        .build()
        .run();
  }

  @Test
  public void testExtractedColInList() throws Exception {
    String query = "SELECT  str_list_list_col[1] AS str_list_list_col_1, int_text_col\n" +
      "FROM (\n" +
      "  SELECT flatten(str_list_list_col) AS str_list_list_col, flatten(order_list) AS order_list, int_text_col\n" +
      "  FROM cp.\"flatten/all_types_dremio.json\"\n" +
      ") nested_0\n" +
      "WHERE str_list_list_col[1] IN ('B','two','','B','B','B','B','B','B','B','B','B','B','B','B','B','B','B','B' ,'two')";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("str_list_list_col_1", "int_text_col")
      .baselineValues("B", "1")
      .baselineValues("B", "1")
      .baselineValues("two", "2147483648")
      .build()
      .run();
  }

  @Test
  public void testExtractedColNumericInList() throws Exception {
    String query = "SELECT int_list_col[1] AS int_list_col_1, bool_col\n" +
      "FROM cp.\"flatten/all_types_dremio.json\"\n" +
      "WHERE int_list_col[1] IN (-1,2,-3,4,-5,6,-7,8,-9,10,-11,12,-13,14,-15,16," +
      "-9223372036854775808,9223372036854775807,2147483648,-2147483649,2147483647,-2147483648)";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("int_list_col_1", "bool_col")
      .baselineValues(2L, null)
      .baselineValues(4L, false)
      .baselineValues(2L, false)
      .build()
      .run();
  }

  @Test
  public void testExtractedColIncompatibleTypesInList() throws Exception {
    exception.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
      "VALIDATION ERROR: Values in expression list must have compatible types"));

    String query = "SELECT int_list_col[1] AS int_list_col_1, bool_col\n" +
        "FROM cp.\"flatten/all_types_dremio.json\"\n" +
        "WHERE int_list_col[1] IN ('a',2,'hello','',4,-5,'world',-7,8,-9,10,-11,12,-13,14,-15.3,16.1," +
        "-9223372036854775808,9223372036854775807,2147483648,-2147483649,2147483647,-2147483648)";

    test(query);
  }

  @Ignore("DX-6574 - Decimal not supported")
  @Test
  public void testExtractedColDecimalInList() throws Exception {
    String query = "SELECT int_list_col[1] AS int_list_col_1, bool_col\n" +
      "FROM cp.\"flatten/all_types_dremio.json\"\n" +
      "WHERE int_list_col[1] IN (-1,2,-3,4,-5,6,-7,8,-9,10,-11,12,-13,14,-15.3,16.1," +
      "-9223372036854775808,9223372036854775807,2147483648,-2147483649,2147483647,-2147483648)";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("int_list_col_1", "bool_col")
      .baselineValues(2L, null)
      .baselineValues(4L, false)
      .baselineValues(2L, false)
      .build()
      .run();
  }
}
