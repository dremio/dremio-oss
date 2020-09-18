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

import static com.dremio.common.util.JodaDateUtility.formatDate;
import static com.dremio.common.util.JodaDateUtility.formatTimeStampMilli;
import static org.joda.time.DateTimeZone.UTC;

import java.io.File;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.util.TSI;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestFunctionsQuery extends BaseTestQuery {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set \"%s\" = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test
  public void testContains() throws Exception {
    String query = "select * from cp.\"employee.json\" where contains(first_name:ABCDE)";
    errorMsgWithTypeTestHelper(query, ErrorType.UNSUPPORTED_OPERATION, "Contains operator is not supported for the table");
  }

  @Test
  public void testDecimalPartitionColumns() throws Exception{
    String query = "select region_plan_profile_id, plan_profile_id from cp" +
      ".\"parquet/decimalPartitionColumns.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("region_plan_profile_id", "plan_profile_id")
      .baselineValues(BigDecimal.valueOf(987654321), BigDecimal.valueOf(987654321))
      .go();
  }

  @Test
  public void testAbsDecimalFunction() throws Exception{
    String[] values = new String[] {
            "1234.4567",
            "-1234.4567",
            "99999912399.4567",
            "-99999912399.4567",
            "12345678912345678912.4567",
            "-12345678912345678912.4567",
            "1234567891234567891234567891234567891.4",
            "-1234567891234567891234567891234567891.4" };
    String query = "SELECT " +
        String.format("abs(cast('%s' as decimal(9, 5))) as DEC9_ABS_1, ", values[0]) +
        String.format("abs(cast('%s' as decimal(9, 5))) DEC9_ABS_2, ", values[1]) +
        String.format("abs(cast('%s' as decimal(18, 5))) DEC18_ABS_1, ", values[2]) +
        String.format("abs(cast('%s' as decimal(18, 5))) DEC18_ABS_2, ", values[3]) +
        String.format("abs(cast('%s' as decimal(28, 5))) DEC28_ABS_1, ", values[4]) +
        String.format("abs(cast('%s' as decimal(28, 5))) DEC28_ABS_2, ", values[5]) +
        String.format("abs(cast('%s' as decimal(38, 1))) DEC38_ABS_1, ", values[6]) +
        String.format("abs(cast('%s' as decimal(38, 1))) DEC38_ABS_2 ", values[7]) +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    Object[] expected = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      expected[i] = new BigDecimal(values[i]).abs();
    }

    test(query);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("DEC9_ABS_1", "DEC9_ABS_2", "DEC18_ABS_1", "DEC18_ABS_2", "DEC28_ABS_1", "DEC28_ABS_2", "DEC38_ABS_1", "DEC38_ABS_2")
        .baselineValues(expected)
            // Fix this once we have full decimal support again
//        .baselineValues(new BigDecimal("1234.45670"), new BigDecimal("1234.45670"), new BigDecimal("99999912399.45670"), new BigDecimal("99999912399.45670"),
//            new BigDecimal("12345678912345678912.45670"), new BigDecimal("12345678912345678912.45670"), new BigDecimal("1234567891234567891234567891234567891.4"),
//            new BigDecimal("1234567891234567891234567891234567891.4"))
        .go();
  }


  @Test
  public void testCeilDecimalFunction() throws Exception {
    String query = "SELECT " +
        "ceil(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "ceil(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "ceil(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "ceil(cast('-1234.000' as decimal(9, 5))) as DEC9_4, " +
        "ceil(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "ceil(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "ceil(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "ceil(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "ceil(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "ceil(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "ceil(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "ceil(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "ceil(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "ceil(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "ceil(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "ceil(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "ceil(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "ceil(cast('-1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_5 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5")
        .baselineValues(new BigDecimal("1235"), new BigDecimal("1234"), new BigDecimal("-1234"), new BigDecimal("-1234"),
            new BigDecimal("99999912400"), new BigDecimal("99999912399"), new BigDecimal("-99999912399"), new BigDecimal("-99999912399"),
            new BigDecimal("12345678912345678913"), new BigDecimal("1000000000000000000"), new BigDecimal("12345678912345678912"), new BigDecimal("-12345678912345678912"),
            new BigDecimal("-12345678912345678912"), new BigDecimal("1234567891234567891234567891234567892"), new BigDecimal("1000000000000000000000000000000000000"),
            new BigDecimal("1234567891234567891234567891234567891"), new BigDecimal("-1234567891234567891234567891234567891"),
            new BigDecimal("-1234567891234567891234567891234567891"))
        .go();
  }

  @Test
  public void testFloorDecimalFunction() throws Exception {
    String query = "SELECT " +
        "floor(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "floor(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "floor(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "floor(cast('-1234.000' as decimal(9, 5))) as DEC9_4, " +
        "floor(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "floor(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "floor(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "floor(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "floor(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "floor(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "floor(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "floor(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "floor(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "floor(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "floor(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "floor(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "floor(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "floor(cast('-999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_5 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5")
        .baselineValues(new BigDecimal("1234"), new BigDecimal("1234"), new BigDecimal("-1235"), new BigDecimal("-1234"),
            new BigDecimal("99999912399"), new BigDecimal("99999912399"), new BigDecimal("-99999912400"), new BigDecimal("-99999912399"),
            new BigDecimal("12345678912345678912"), new BigDecimal("999999999999999999"), new BigDecimal("12345678912345678912"),
            new BigDecimal("-12345678912345678913"), new BigDecimal("-12345678912345678912"), new BigDecimal("1234567891234567891234567891234567891"),
            new BigDecimal("999999999999999999999999999999999999"), new BigDecimal("1234567891234567891234567891234567891"),
            new BigDecimal("-1234567891234567891234567891234567892"), new BigDecimal("-1000000000000000000000000000000000000"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testTruncateDecimalFunction() throws Exception {
    String query = "SELECT " +
        "trunc(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "trunc(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "trunc(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "trunc(cast('0.111' as decimal(9, 5))) as DEC9_4, " +
        "trunc(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "trunc(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "trunc(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "trunc(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "trunc(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "trunc(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "trunc(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "trunc(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "trunc(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "trunc(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "trunc(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "trunc(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "trunc(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "trunc(cast('-999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_5 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5")
        .baselineValues(new BigDecimal("1234"), new BigDecimal("1234"), new BigDecimal("-1234"), new BigDecimal("0"),
            new BigDecimal("99999912399"), new BigDecimal("99999912399"), new BigDecimal("-99999912399"),
            new BigDecimal("-99999912399"), new BigDecimal("12345678912345678912"), new BigDecimal("999999999999999999"),
            new BigDecimal("12345678912345678912"), new BigDecimal("-12345678912345678912"), new BigDecimal("-12345678912345678912"),
            new BigDecimal("1234567891234567891234567891234567891"), new BigDecimal("999999999999999999999999999999999999"),
            new BigDecimal("1234567891234567891234567891234567891"), new BigDecimal("-1234567891234567891234567891234567891"),
            new BigDecimal("-999999999999999999999999999999999999"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testTruncateWithParamDecimalFunction() throws Exception {
    String query = "SELECT " +
        "trunc(cast('1234.4567' as decimal(9, 5)), 2) as DEC9_1, " +
        "trunc(cast('1234.45' as decimal(9, 2)), 4) as DEC9_2, " +
        "trunc(cast('-1234.4567' as decimal(9, 5)), 0) as DEC9_3, " +
        "trunc(cast('0.111' as decimal(9, 5)), 2) as DEC9_4, " +
        "trunc(cast('99999912399.4567' as decimal(18, 5)), 2) DEC18_1, " +
        "trunc(cast('99999912399.0000' as decimal(18, 5)), 2) DEC18_2, " +
        "trunc(cast('-99999912399.45' as decimal(18, 2)), 6) DEC18_3, " +
        "trunc(cast('-99999912399.0000' as decimal(18, 5)), 4) DEC18_4, " +
        "trunc(cast('12345678912345678912.4567' as decimal(28, 5)), 1) DEC28_1, " +
        "trunc(cast('999999999999999999.456' as decimal(28, 3)), 6) DEC28_2, " +
        "trunc(cast('12345678912345678912.0000' as decimal(28, 5)), 2) DEC28_3, " +
        "trunc(cast('-12345678912345678912.45' as decimal(28, 2)), 0) DEC28_4, " +
        "trunc(cast('-12345678912345678912.0000' as decimal(28, 5)), 1) DEC28_5, " +
        "trunc(cast('999999999.123456789' as decimal(38, 9)), 7) DEC38_1, " +
        "trunc(cast('999999999.4' as decimal(38, 1)), 8) DEC38_2, " +
        "trunc(cast('999999999.1234' as decimal(38, 4)), 12) DEC38_3, " +
        "trunc(cast('-123456789123456789.4' as decimal(38, 1)), 10) DEC38_4, " +
        "trunc(cast('-999999999999999999999999999999999999.4' as decimal(38, 1)), 1) DEC38_5 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5")
        .baselineValues(new BigDecimal("1234.45"), new BigDecimal("1234.4500"), new BigDecimal("-1234"), new BigDecimal("0.11"),
            new BigDecimal("99999912399.45"), new BigDecimal("99999912399.00"), new BigDecimal("-99999912399.450000"),
            new BigDecimal("-99999912399.0000"), new BigDecimal("12345678912345678912.4"), new BigDecimal("999999999999999999.456000"),
            new BigDecimal("12345678912345678912.00"), new BigDecimal("-12345678912345678912"), new BigDecimal("-12345678912345678912.0"),
            new BigDecimal("999999999.1234567"), new BigDecimal("999999999.40000000"), new BigDecimal("999999999.123400000000"),
            new BigDecimal("-123456789123456789.4000000000"), new BigDecimal("-999999999999999999999999999999999999.4"))
        .go();
  }

  @Test
  public void testRoundDecimalFunction() throws Exception {
    String query = "SELECT " +
        "round(cast('1234.5567' as decimal(9, 5))) as DEC9_1, " +
        "round(cast('1234.1000' as decimal(9, 5))) as DEC9_2, " +
        "round(cast('-1234.5567' as decimal(9, 5))) as DEC9_3, " +
        "round(cast('-1234.1234' as decimal(9, 5))) as DEC9_4, " +
        "round(cast('99999912399.9567' as decimal(18, 5))) DEC18_1, " +
        "round(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "round(cast('-99999912399.5567' as decimal(18, 5))) DEC18_3, " +
        "round(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "round(cast('12345678912345678912.5567' as decimal(28, 5))) DEC28_1, " +
        "round(cast('999999999999999999.5567' as decimal(28, 5))) DEC28_2, " +
        "round(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "round(cast('-12345678912345678912.5567' as decimal(28, 5))) DEC28_4, " +
        "round(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "round(cast('999999999999999999999999999.5' as decimal(38, 1))) DEC38_1, " +
        "round(cast('99999999.512345678123456789' as decimal(38, 18))) DEC38_2, " +
        "round(cast('999999999999999999999999999999999999.5' as decimal(38, 1))) DEC38_3, " +
        "round(cast('1234567891234567891234567891234567891.2' as decimal(38, 1))) DEC38_4, " +
        "round(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_5, " +
        "round(cast('-999999999999999999999999999999999999.9' as decimal(38, 1))) DEC38_6 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5", "DEC38_6")
        .baselineValues(new BigDecimal("1235"), new BigDecimal("1234"), new BigDecimal("-1235"), new BigDecimal("-1234"),
            new BigDecimal("99999912400"), new BigDecimal("99999912399"), new BigDecimal("-99999912400"), new BigDecimal("-99999912399"),
            new BigDecimal("12345678912345678913"), new BigDecimal("1000000000000000000"), new BigDecimal("12345678912345678912"),
            new BigDecimal("-12345678912345678913"), new BigDecimal("-12345678912345678912"), new BigDecimal("1000000000000000000000000000"),
            new BigDecimal("100000000"), new BigDecimal("1000000000000000000000000000000000000"), new BigDecimal("1234567891234567891234567891234567891"),
            new BigDecimal("-1234567891234567891234567891234567891"), new BigDecimal("-1000000000000000000000000000000000000"))
        .go();
  }

  @Ignore("DRILL-3909")
  @Test
  public void testRoundWithScaleDecimalFunction() throws Exception {
    String query = "SELECT " +
        "round(cast('1234.5567' as decimal(9, 5)), 3) as DEC9_1, " +
        "round(cast('1234.1000' as decimal(9, 5)), 2) as DEC9_2, " +
        "round(cast('-1234.5567' as decimal(9, 5)), 4) as DEC9_3, " +
        "round(cast('-1234.1234' as decimal(9, 5)), 3) as DEC9_4, " +
        "round(cast('-1234.1234' as decimal(9, 2)), 4) as DEC9_5, " +
        "round(cast('99999912399.9567' as decimal(18, 5)), 3) DEC18_1, " +
        "round(cast('99999912399.0000' as decimal(18, 5)), 2) DEC18_2, " +
        "round(cast('-99999912399.5567' as decimal(18, 5)), 2) DEC18_3, " +
        "round(cast('-99999912399.0000' as decimal(18, 5)), 0) DEC18_4, " +
        "round(cast('12345678912345678912.5567' as decimal(28, 5)), 2) DEC28_1, " +
        "round(cast('999999999999999999.5567' as decimal(28, 5)), 1) DEC28_2, " +
        "round(cast('12345678912345678912.0000' as decimal(28, 5)), 8) DEC28_3, " +
        "round(cast('-12345678912345678912.5567' as decimal(28, 5)), 3) DEC28_4, " +
        "round(cast('-12345678912345678912.0000' as decimal(28, 5)), 0) DEC28_5, " +
        "round(cast('999999999999999999999999999.5' as decimal(38, 1)), 1) DEC38_1, " +
        "round(cast('99999999.512345678923456789' as decimal(38, 18)), 9) DEC38_2, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 9) DEC38_3, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 11) DEC38_4, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 21) DEC38_5, " +
        "round(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1)), 1) DEC38_6, " +
        "round(cast('-999999999999999999999999999999999999.9' as decimal(38, 1)), 0) DEC38_7 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_1", "DEC9_2", "DEC9_3", "DEC9_4", "DEC9_5", "DEC18_1", "DEC18_2", "DEC18_3", "DEC18_4", "DEC28_1",
            "DEC28_2", "DEC28_3", "DEC28_4", "DEC28_5", "DEC38_1", "DEC38_2", "DEC38_3", "DEC38_4", "DEC38_5", "DEC38_6", "DEC38_7")
        .baselineValues(new BigDecimal("1234.557"), new BigDecimal("1234.10"), new BigDecimal("-1234.5567"), new BigDecimal("-1234.123"),
            new BigDecimal("-1234.1200"), new BigDecimal("99999912399.957"), new BigDecimal("99999912399.00"), new BigDecimal("-99999912399.56"),
            new BigDecimal("-99999912399"), new BigDecimal("12345678912345678912.56"), new BigDecimal("999999999999999999.6"),
            new BigDecimal("12345678912345678912.00000000"), new BigDecimal("-12345678912345678912.557"), new BigDecimal("-12345678912345678912"),
            new BigDecimal("999999999999999999999999999.5"), new BigDecimal("99999999.512345679"), new BigDecimal("1000000000.000000000"),
            new BigDecimal("999999999.99999999957"), new BigDecimal("999999999.999999999567800000000"), new BigDecimal("-1234567891234567891234567891234567891.4"),
            new BigDecimal("-1000000000000000000000000000000000000"))
        .go();
  }

  @Ignore("we don't have decimal division")
  @Test
  public void testCastDecimalDivide() throws Exception {
    String query = "select  (cast('9' as decimal(9, 1)) / cast('2' as decimal(4, 1))) as DEC9_DIV, " +
        "cast('999999999' as decimal(9,0)) / cast('0.000000000000000000000000001' as decimal(28,28)) as DEC38_DIV, " +
        "cast('123456789.123456789' as decimal(18, 9)) * cast('123456789.123456789' as decimal(18, 9)) as DEC18_MUL " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC9_DIV", "DEC38_DIV", "DEC18_MUL")
        .baselineValues(new BigDecimal("4.500000000"), new BigDecimal("999999999000000000000000000000000000.0"),
            new BigDecimal("15241578780673678.515622620750190521"))
        .go();
  }


  // From DRILL-2668:  "CAST ( 1.1 AS FLOAT )" yielded TYPE DOUBLE:

  /**
   * Test for DRILL-2668, that "CAST ( 1.5 AS FLOAT )" really yields type FLOAT
   * (rather than type DOUBLE).
   */
  @Test
  public void testLiteralCastToFLOATYieldsFLOAT() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 1.5 AS FLOAT ) AS ShouldBeFLOAT "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeFLOAT")
    .baselineValues(new Float(1.5f))
    .go();
  }

  @Test
  public void testLiteralCastToDecimal11() throws Exception { // suffix 11 denotes precision=1 and scale=1
    testBuilder()
      .sqlQuery( "SELECT CAST( 123456.799 AS DECIMAL(1,1 )) AS ShouldBeZERO ")
      .unOrdered()
      .baselineColumns("ShouldBeZERO")
      .baselineValues(new BigDecimal("0.0"))
      .go();
  }

  @Test
  public void testCastToDecimal11() throws Exception { // suffix 11 denotes precision=1 and scale=1
    testBuilder()
      .sqlQuery( "SELECT cast(columns[0] as Decimal(1,1)) as ShouldBeZERO "
                 + "FROM cp.\"decimal/cast_decimal_11.csv\"")
      .unOrdered()
      .baselineColumns("ShouldBeZERO")
      .baselineValues(new BigDecimal("0.0"))
      .go();
  }

  @Test
  public void testLiteralCastToDOUBLEYieldsDOUBLE() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 1.25 AS DOUBLE PRECISION ) AS ShouldBeDOUBLE "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeDOUBLE")
    .baselineValues(new Double(1.25))
    .go();
  }

  @Test
  public void testLiteralCastToBIGINTYieldsBIGINT() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 64 AS BIGINT ) AS ShouldBeBIGINT "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeBIGINT")
    .baselineValues(new Long(64))
    .go();
  }

  @Test
  public void testLiteralCastToINTEGERYieldsINTEGER() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 32 AS INTEGER ) AS ShouldBeINTEGER "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeINTEGER")
    .baselineValues(new Integer(32))
    .go();
  }

  @Ignore( "until SMALLINT is supported (DRILL-2470)" )
  @Test
  public void testLiteralCastToSMALLINTYieldsSMALLINT() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 16 AS SMALLINT ) AS ShouldBeSMALLINT "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeSMALLINT")
    .baselineValues(new Short((short) 16))
    .go();
  }

  @Ignore( "until TINYINT is supported (~DRILL-2470)" )
  @Test
  public void testLiteralCastToTINYINTYieldsTINYINT() throws Exception {
    testBuilder()
    .sqlQuery( "SELECT CAST( 8 AS TINYINT ) AS ShouldBeTINYINT "
               + "FROM cp.\"employee.json\" LIMIT 1" )
    .unOrdered()
    .baselineColumns("ShouldBeTINYINT")
    .baselineValues(new Byte((byte) 8))
    .go();
  }


  @Test
  @Ignore("decimal")
  public void testDecimalMultiplicationOverflowHandling() throws Exception {
    String query = "select cast('1' as decimal(9, 5)) * cast ('999999999999999999999999999.999999999' as decimal(38, 9)) as DEC38_1, " +
        "cast('1000000000000000001.000000000000000000' as decimal(38, 18)) * cast('0.999999999999999999' as decimal(38, 18)) as DEC38_2, " +
        "cast('3' as decimal(9, 8)) * cast ('333333333.3333333333333333333' as decimal(38, 19)) as DEC38_3 " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC38_1", "DEC38_2", "DEC38_3")
        .baselineValues(new BigDecimal("1000000000000000000000000000.00000"), new BigDecimal("1000000000000000000"), new BigDecimal("1000000000.000000000000000000"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testDecimalRoundUp() throws Exception {
    String query = "select cast('999999999999999999.9999999999999999995' as decimal(38, 18)) as DEC38_1, " +
        "cast('999999999999999999.9999999999999999994' as decimal(38, 18)) as DEC38_2, " +
        "cast('999999999999999999.1234567895' as decimal(38, 9)) as DEC38_3, " +
        "cast('99999.12345' as decimal(18, 4)) as DEC18_1, " +
        "cast('99999.99995' as decimal(18, 4)) as DEC18_2 " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC38_1", "DEC38_2", "DEC38_3", "DEC18_1", "DEC18_2")
        .baselineValues(new BigDecimal("1000000000000000000.000000000000000000"), new BigDecimal("999999999999999999.999999999999999999"),
            new BigDecimal("999999999999999999.123456790"), new BigDecimal("99999.1235"), new BigDecimal("100000.0000"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testDecimalDownwardCast() throws Exception {
    String query = "select cast((cast('12345.6789' as decimal(18, 4))) as decimal(9, 4)) as DEC18_DEC9_1, " +
        "cast((cast('12345.6789' as decimal(18, 4))) as decimal(9, 2)) as DEC18_DEC9_2, " +
        "cast((cast('-12345.6789' as decimal(18, 4))) as decimal(9, 0)) as DEC18_DEC9_3, " +
        "cast((cast('999999999.6789' as decimal(38, 4))) as decimal(9, 0)) as DEC38_DEC19_1, " +
        "cast((cast('-999999999999999.6789' as decimal(38, 4))) as decimal(18, 2)) as DEC38_DEC18_1, " +
        "cast((cast('-999999999999999.6789' as decimal(38, 4))) as decimal(18, 0)) as DEC38_DEC18_2, " +
        "cast((cast('100000000999999999.6789' as decimal(38, 4))) as decimal(28, 0)) as DEC38_DEC28_1 " +
        "from cp.\"employee.json\" where employee_id = 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC18_DEC9_1", "DEC18_DEC9_2", "DEC18_DEC9_3", "DEC38_DEC19_1", "DEC38_DEC18_1", "DEC38_DEC18_2", "DEC38_DEC28_1")
        .baselineValues(new BigDecimal("12345.6789"), new BigDecimal("12345.68"), new BigDecimal("-12346"), new BigDecimal("1000000000"),
            new BigDecimal("-999999999999999.68"), new BigDecimal("-1000000000000000"), new BigDecimal("100000001000000000"))
        .go();
  }

  @Test
  public void testTruncateWithParamFunction() throws Exception {
    String query = "SELECT " +
        "truncate(1234.4567, 2) as T_1, " +
        "truncate(-1234.4567, 2) as T_2, " +
        "truncate(1234.4567, -2) as T_3, " +
        "truncate(-1234.4567, -2) as T_4, " +
        "truncate(1234, 4) as T_5, " +
        "truncate(-1234, 4) as T_6, " +
        "truncate(1234, -4) as T_7, " +
        "truncate(-1234, -4) as T_8, " +
        "truncate(8124674407369523212, 0) as T_9, " +
        "truncate(81246744073695.395, 1) as T_10 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("T_1", "T_2", "T_3", "T_4", "T_5", "T_6", "T_7", "T_8", "T_9", "T_10")
        .baselineValues(BigDecimal.valueOf(1234.45D), BigDecimal.valueOf(-1234.45D),
          new BigDecimal("1200"), new BigDecimal("-1200"), 1234, -1234,
            0, 0, 8124674407369523212L, BigDecimal.valueOf(81246744073695.3D))
        .go();
  }

  @Test
  public void testRoundWithParamFunction() throws Exception {
    // this tests constant reduction
    String query = "SELECT " +
        "round(1234.4567, 2) as T_1, " +
        "round(-1234.4567, 2) as T_2, " +
        "round(1234.4567, -2) as T_3, " +
        "round(-1234.4567, -2) as T_4, " +
        "round(1234, 4) as T_5, " +
        "round(-1234, 4) as T_6, " +
        "round(1234, -4) as T_7, " +
        "round(-1234, -4) as T_8, " +
        "round(8124674407369523212, -4) as T_9, " +
        "round(81246744073695.395, 1) as T_10, " +
        "round(6.05, 1) as T_11, " +
        "round(-6.05, 1) as T_12 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("T_1", "T_2", "T_3", "T_4", "T_5", "T_6", "T_7", "T_8", "T_9", "T_10", "T_11", "T_12")
        .baselineValues(BigDecimal.valueOf(1234.46D), BigDecimal.valueOf(-1234.46D),
          new BigDecimal("1200"), new BigDecimal("-1200"), 1234, -1234,
            0, 0, 8124674407369520000L, new BigDecimal("81246744073695.4"), BigDecimal.valueOf
            (6.1D), BigDecimal.valueOf(-6.1D))
        .go();
  }

  @Test
  public void testRoundWithParamFunction2() throws Exception {
    String query = "SELECT round(float4_col, 0) as t1, round(float8_col, 0) as t2 " +
        "FROM cp.\"parquet/all_scalar_types.parquet\"";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("t1", "t2")
        .baselineValues(1.0F, 1.0D)
        .go();
  }

  @Test
  public void testRoundWithOneParam() throws Exception {
    String query = "select " +
        "round(8124674407369523212) round_bigint," +
        "round(9999999) round_int, " +
        "round(cast('23.45' as float)) round_float_1, " +
        "round(cast('23.55' as float)) round_float_2, " +
        "round(8124674407369.2345) round_double_1, " +
        "round(8124674407369.589) round_double_2 " +
        " from cp.\"tpch/region.parquet\" limit 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("round_bigint", "round_int", "round_float_1", "round_float_2", "round_double_1", "round_double_2")
        .baselineValues(8124674407369523212l, 9999999, 23.0f, 24.0f, new BigDecimal
            ("8124674407369"), new BigDecimal("8124674407370"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testToCharFunction() throws Exception {
    String query = "SELECT " +
        "to_char(1234.5567, '#,###.##') as FLOAT8_1, " +
        "to_char(1234.5, '$#,###.00') as FLOAT8_2, " +
        "to_char(cast('1234.5567' as decimal(9, 5)), '#,###.##') as DEC9_1, " +
        "to_char(cast('99999912399.9567' as decimal(18, 5)), '#.#####') DEC18_1, " +
        "to_char(cast('12345678912345678912.5567' as decimal(28, 5)), '#,###.#####') DEC28_1, " +
        "to_char(cast('999999999999999999999999999.5' as decimal(38, 1)), '#.#') DEC38_1 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("FLOAT8_1", "FLOAT8_2", "DEC9_1", "DEC18_1", "DEC28_1", "DEC38_1")
        .baselineValues("1,234.56", "$1,234.50", "1,234.56", "99999912399.9567", "12,345,678,912,345,678,912.5567", "999999999999999999999999999.5")
        .go();
  }

  @Test
  public void testConcatFunction() throws Exception {
    String query = "SELECT " +
        "concat('1234', ' COL_VALUE ', R_REGIONKEY, ' - STRING') as STR_1 " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("STR_1")
        .baselineValues("1234 COL_VALUE 0 - STRING")
        .go();
  }

  @Test
  public void testTimeStampConstant() throws Exception {
    String query = "SELECT " +
        "timestamp '2008-2-23 12:23:23' as TS " +
        "FROM cp.\"tpch/region.parquet\" limit 1";

    LocalDateTime date = formatTimeStampMilli.parseLocalDateTime("2008-02-23 12:23:23.0");
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("TS")
        .baselineValues(date)
        .go();
  }

  @Test
  public void testNullConstantsTimeTimeStampAndDate() throws Exception {
    String query = "SELECT " +
        "CAST(NULL AS TIME) AS t, " +
        "CAST(NULL AS TIMESTAMP) AS ts, " +
        "CAST(NULL AS DATE) AS d " +
        "FROM cp.\"region.json\" LIMIT 1";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("t", "ts", "d")
        .baselineValues(null, null, null)
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testIntMinToDecimal() throws Exception {
    String query = "select cast((employee_id - employee_id + -2147483648) as decimal(28, 2)) as DEC_28," +
        "cast((employee_id - employee_id + -2147483648) as decimal(18, 2)) as DEC_18 from " +
        "cp.\"employee.json\" limit 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC_28", "DEC_18")
        .baselineValues(new BigDecimal("-2147483648.00"), new BigDecimal("-2147483648.00"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testDecimalAddConstant() throws Exception {
    String query = "select (cast('-1' as decimal(37, 3)) + cast (employee_id as decimal(37, 3))) as CNT " +
        "from cp.\"employee.json\" where employee_id <= 4";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("CNT")
        .baselineValues(new BigDecimal("0.000"))
        .baselineValues(new BigDecimal("1.000"))
        .baselineValues(new BigDecimal("3.000"))
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testDecimalAddIntConstant() throws Exception {
    String query = "select 1 + cast(employee_id as decimal(9, 3)) as DEC_9 , 1 + cast(employee_id as decimal(37, 5)) as DEC_38 " +
        "from cp.\"employee.json\" where employee_id <= 2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC_9", "DEC_38")
        .baselineValues(new BigDecimal("2.000"), new BigDecimal("2.00000"))
        .baselineValues(new BigDecimal("3.000"), new BigDecimal("3.00000"))
        .go();
  }

  @Test
  public void testSignFunction() throws Exception {
    String query = "select sign(cast('1.23' as float)) as SIGN_FLOAT, sign(-1234.4567) as SIGN_DOUBLE, sign(23) as SIGN_INT " +
        "from cp.\"employee.json\" where employee_id < 2";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("SIGN_FLOAT", "SIGN_DOUBLE", "SIGN_INT")
        .baselineValues(1F, BigDecimal.valueOf(-1D), 1)
        .go();
  }


  @Test
  public void testPadFunctions() throws Exception {
    String query = "select rpad(first_name, 10) as RPAD_DEF, rpad(first_name, 10, '*') as RPAD_STAR, lpad(first_name, 10) as LPAD_DEF, lpad(first_name, 10, '*') as LPAD_STAR, " +
        "lpad(first_name, 2) as LPAD_TRUNC, rpad(first_name, 2) as RPAD_TRUNC " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("RPAD_DEF", "RPAD_STAR", "LPAD_DEF", "LPAD_STAR", "LPAD_TRUNC", "RPAD_TRUNC")
        .baselineValues("Sheri     ", "Sheri*****", "     Sheri", "*****Sheri", "Sh", "Sh")
        .go();

  }

  @Test
  public void testExtractSecond() throws Exception {
    String query = "select extract(second from date '2008-2-23') as DATE_EXT, extract(second from timestamp '2008-2-23 10:00:20.123') as TS_EXT, " +
        "extract(second from time '10:20:30.303') as TM_EXT " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DATE_EXT", "TS_EXT", "TM_EXT")
        .baselineValues(0L, 20L, 30L)
        .go();
  }

  @Test
  public void testCastIntervalDayToInt() throws Exception {
    final String query = "select " +
      "cast(interval '1' second as integer) as sec," +
      "cast((interval '1' second - interval '2' second) as integer) as sec_calc," +
      "cast(interval '1' minute as integer) as mins," +
      "cast(interval '1' hour as integer) as hours," +
      "cast(interval '1' day as integer) as days ";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("sec", "sec_calc", "mins", "hours", "days")
      .baselineValues(1, -1, 1, 1, 1)
      .go();
  }

  @Test
  public void testCastIntervalYearToInt() throws Exception {
    final String query = "select " +
      "cast(interval '1' month as integer) as months," +
      "cast(interval '1' year as integer) as years, " +
      "cast((interval '1' year - interval '2' year) as integer) as year_calc ";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("months", "years", "year_calc")
      .baselineValues(1, 1, -1)
      .go();
  }

  @Test
  public void testTsAddCastIntervalDayToInt() throws Exception {
    final String query = "select " +
      "timestampadd(second, cast((interval '2' second - interval '4' second) as integer), {ts '1990-12-12 12:12:12'}) as ts1, \n" +
      "timestampadd(second, cast((interval '2' second - interval '4' second) as integer) , a) as ts2 from (values({ts '1990-12-12 12:12:12'})) tbl(a)";

    final LocalDateTime result = formatTimeStampMilli.parseLocalDateTime("1990-12-12 12:12:10.000");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ts1", "ts2")
      .baselineValues(result, result)
      .go();
  }

  @Test
  public void testTsAddCastIntervalYearToInt() throws Exception {
    final String query = "select " +
      "timestampadd(year, cast((interval '2' year - interval '4' year) as integer) , {ts '1990-12-12 12:12:12'}) as ts1, " +
      "timestampadd(year, cast((interval '2' year - interval '4' year) as integer) , a) as ts2 from (values({ts '1990-12-12 12:12:12'})) tbl(a)";

    final LocalDateTime result = formatTimeStampMilli.parseLocalDateTime("1988-12-12 12:12:12.000");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ts1", "ts2")
      .baselineValues(result, result)
      .go();
  }

  @Test
  @Ignore("decimal")
  public void testCastDecimalDouble() throws Exception {
    String query = "select cast((cast('1.0001' as decimal(18, 9))) as double) DECIMAL_DOUBLE_CAST " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DECIMAL_DOUBLE_CAST")
        .baselineValues(1.0001d)
        .go();
  }

  @Test
  public void testExtractSecondFromInterval() throws Exception {
    String query = "select extract (second from interval '1 2:30:45.100' day to second) as EXT_INTDAY " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXT_INTDAY")
        .baselineValues(45L)
        .go();
  }

  @Test
  public void testFunctionCaseInsensitiveNames() throws Exception {
    String query = "SELECT to_date('2003/07/09', 'YYYY/MM/DD') as col1, " +
        "TO_DATE('2003/07/09', 'YYYY/MM/DD') as col2, " +
        "To_DaTe('2003/07/09', 'YYYY/MM/DD') as col3 " +
        "from cp.\"employee.json\" LIMIT 1";

    LocalDateTime date = formatDate.parseLocalDateTime("2003-07-09");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3")
        .baselineValues(date, date, date)
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testDecimal18Decimal38Comparison() throws Exception {
    String query = "select cast('-999999999.999999999' as decimal(18, 9)) = cast('-999999999.999999999' as decimal(38, 18)) as CMP " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("CMP")
        .baselineValues(true)
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testOptiqDecimalCapping() throws Exception {
    String query = "select  cast('12345.678900000' as decimal(18, 9))=cast('12345.678900000' as decimal(38, 9)) as CMP " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("CMP")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testNegative() throws Exception {
    String query = "select  negative(2) as NEG " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("NEG")
        .baselineValues(-2l)
        .go();
  }

  @Test
  public void testOptiqValidationFunctions() throws Exception {
    String query = "select trim(first_name) as TRIM_STR, substring(first_name, 2) as SUB_STR " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("TRIM_STR", "SUB_STR")
        .baselineValues("Sheri", "heri")
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testToTimeStamp() throws Exception {
    String query = "select to_timestamp(cast('800120400.12312' as decimal(38, 5))) as DEC38_TS, to_timestamp(200120400) as INT_TS " +
        "from cp.\"employee.json\" where employee_id < 2";

    LocalDateTime result1 = new LocalDateTime(800120400123l, UTC);
    LocalDateTime result2 = new LocalDateTime(200120400000l, UTC);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("DEC38_TS", "INT_TS")
        .baselineValues(result1, result2)
        .go();
  }

  @Test
  @Ignore("decimal")
  public void testCaseWithDecimalExpressions() throws Exception {
    String query = "select " +
        "case when true then cast(employee_id as decimal(15, 5)) else cast('0.0' as decimal(2, 1)) end as col1 " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(new BigDecimal("1.00000"))
        .go();
  }


  /*
   * We may apply implicit casts in Hash Join while dealing with different numeric data types
   * For this to work we need to distribute the data based on a common key, below method
   * makes sure the hash value for different numeric types is the same for the same key
   */
  @Test
  @Ignore("decimal")
  public void testHash64() throws Exception {
    String query = "select " +
        "hash64AsDouble(cast(employee_id as int)) = hash64AsDouble(cast(employee_id as bigint)) col1, " +
        "hash64AsDouble(cast(employee_id as bigint)) = hash64AsDouble(cast(employee_id as float)) col2, " +
        "hash64AsDouble(cast(employee_id as float)) = hash64AsDouble(cast(employee_id as double)) col3, " +
        "hash64AsDouble(cast(employee_id as double)) = hash64AsDouble(cast(employee_id as decimal(9, 0))) col4, " +
        "hash64AsDouble(cast(employee_id as decimal(9, 0))) = hash64AsDouble(cast(employee_id as decimal(18, 0))) col5, " +
        "hash64AsDouble(cast(employee_id as decimal(18, 0))) = hash64AsDouble(cast(employee_id as decimal(28, 0))) col6, " +
        "hash64AsDouble(cast(employee_id as decimal(28, 0))) = hash64AsDouble(cast(employee_id as decimal(38, 0))) col7 " +
        "from cp.\"employee.json\"  where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7")
        .baselineValues(true, true, true, true, true, true, true)
        .go();

    java.util.Random seedGen = new java.util.Random();
    seedGen.setSeed(System.currentTimeMillis());
    int seed = seedGen.nextInt();

    String querytemplate = "select " +
            "hash64AsDouble(cast(employee_id as int), #RAND_SEED#) = hash64AsDouble(cast(employee_id as bigint), #RAND_SEED#) col1, " +
            "hash64AsDouble(cast(employee_id as bigint), #RAND_SEED#) = hash64AsDouble(cast(employee_id as float), #RAND_SEED#) col2, " +
            "hash64AsDouble(cast(employee_id as float), #RAND_SEED#) = hash64AsDouble(cast(employee_id as double), #RAND_SEED#) col3, " +
            "hash64AsDouble(cast(employee_id as double), #RAND_SEED#) = hash64AsDouble(cast(employee_id as decimal(9, 0)), #RAND_SEED#) col4, " +
            "hash64AsDouble(cast(employee_id as decimal(9, 0)), #RAND_SEED#) = hash64AsDouble(cast(employee_id as decimal(18, 0)), #RAND_SEED#) col5, " +
            "hash64AsDouble(cast(employee_id as decimal(18, 0)), #RAND_SEED#) = hash64AsDouble(cast(employee_id as decimal(28, 0)), #RAND_SEED#) col6, " +
            "hash64AsDouble(cast(employee_id as decimal(28, 0)), #RAND_SEED#) = hash64AsDouble(cast(employee_id as decimal(38, 0)), #RAND_SEED#) col7 " +
            "from cp.\"employee.json\" where employee_id = 1";

    String queryWithSeed = querytemplate.replaceAll("#RAND_SEED#", String.format("%d",seed));
    testBuilder()
            .sqlQuery(queryWithSeed)
            .unOrdered()
            .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7")
            .baselineValues(true, true, true, true, true, true, true)
            .go();

  }


  @Test
  @Ignore("decimal")
  public void testHash32() throws Exception {
    String query = "select " +
        "hash32AsDouble(cast(employee_id as int)) = hash32AsDouble(cast(employee_id as bigint)) col1, " +
        "hash32AsDouble(cast(employee_id as bigint)) = hash32AsDouble(cast(employee_id as float)) col2, " +
        "hash32AsDouble(cast(employee_id as float)) = hash32AsDouble(cast(employee_id as double)) col3, " +
        "hash32AsDouble(cast(employee_id as double)) = hash32AsDouble(cast(employee_id as decimal(9, 0))) col4, " +
        "hash32AsDouble(cast(employee_id as decimal(9, 0))) = hash32AsDouble(cast(employee_id as decimal(18, 0))) col5, " +
        "hash32AsDouble(cast(employee_id as decimal(18, 0))) = hash32AsDouble(cast(employee_id as decimal(28, 0))) col6, " +
        "hash32AsDouble(cast(employee_id as decimal(28, 0))) = hash32AsDouble(cast(employee_id as decimal(38, 0))) col7 " +
        "from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7")
        .baselineValues(true, true, true, true, true, true, true)
        .go();

    java.util.Random seedGen = new java.util.Random();
    seedGen.setSeed(System.currentTimeMillis());
    int seed = seedGen.nextInt();

    String querytemplate = "select " +
            "hash32AsDouble(cast(employee_id as int), #RAND_SEED#) = hash32AsDouble(cast(employee_id as bigint), #RAND_SEED#) col1, " +
            "hash32AsDouble(cast(employee_id as bigint), #RAND_SEED#) = hash32AsDouble(cast(employee_id as float), #RAND_SEED#) col2, " +
            "hash32AsDouble(cast(employee_id as float),  #RAND_SEED#) = hash32AsDouble(cast(employee_id as double), #RAND_SEED#) col3, " +
            "hash32AsDouble(cast(employee_id as double), #RAND_SEED#) = hash32AsDouble(cast(employee_id as decimal(9, 0)), #RAND_SEED#) col4, " +
            "hash32AsDouble(cast(employee_id as decimal(9, 0)), #RAND_SEED#) = hash32AsDouble(cast(employee_id as decimal(18, 0)), #RAND_SEED#) col5, " +
            "hash32AsDouble(cast(employee_id as decimal(18, 0)), #RAND_SEED#) = hash32AsDouble(cast(employee_id as decimal(28, 0)), #RAND_SEED#) col6, " +
            "hash32AsDouble(cast(employee_id as decimal(28, 0)), #RAND_SEED#) = hash32AsDouble(cast(employee_id as decimal(38, 0)), #RAND_SEED#) col7 " +
            "from cp.\"employee.json\" where employee_id = 1";

    String queryWithSeed = querytemplate.replaceAll("#RAND_SEED#", String.format("%d",seed));
    testBuilder()
            .sqlQuery(queryWithSeed)
            .unOrdered()
            .baselineColumns("col1", "col2", "col3", "col4", "col5", "col6", "col7")
            .baselineValues(true, true, true, true, true, true, true)
            .go();

  }

  @Test
  public void testImplicitCastVarcharToDouble() throws Exception {
    // tests implicit cast from varchar to double
    testBuilder()
        .sqlQuery("select \"integer\" i, \"float\" f from cp.\"jsoninput/input1.json\" where \"float\" = '1.2'")
        .unOrdered()
        .baselineColumns("i", "f")
        .baselineValues(2001l, 1.2d)
        .go();
  }

  @Test
  public void testConcatSingleInput() throws Exception {
    String query = "select concat(employee_id) as col1 from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("1")
        .go();

    query = "select concat('foo') as col1 from cp.\"employee.json\" where employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("foo")
        .go();
  }

  @Test // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
  public void timestampAddMicroSecond() throws Exception {
    exception.expect(new UserExceptionMatcher(ErrorType.UNSUPPORTED_OPERATION,
        "TIMESTAMPADD function supports the following time units: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND"));

    test("select timestampadd(MICROSECOND, 2, timestamp '2015-03-30 20:49:59.000') as ts from (values(1))");
  }

  @Test // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
  public void timestampAddNanoSecond() throws Exception {
      exception.expect(new UserExceptionMatcher(ErrorType.PARSE, "Failure parsing the query."));

    test("select timestampadd(NANOSECOND, 2, timestamp '2015-03-30 20:49:59.000') as ts from (values(1))");
  }

  @Test
  public void testTimestampAddTimestamp() throws Exception {
    String queryTemplate = "select timestampadd(%s, 2, timestamp '2015-03-30 20:49:59.000') as ts from (values(1))";
    Map<TSI, LocalDateTime> results = ImmutableMap.<TSI, LocalDateTime>builder()
        // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
        // .put(TSI.MICROSECOND, formatTimeStampMilli.parseLocalDateTime("2015-03-30 20:49:59.002"))
        .put(TSI.SECOND, formatTimeStampMilli.parseLocalDateTime("2015-03-30 20:50:01.000"))
        .put(TSI.MINUTE, formatTimeStampMilli.parseLocalDateTime("2015-03-30 20:51:59.000"))
        .put(TSI.HOUR, formatTimeStampMilli.parseLocalDateTime("2015-03-30 22:49:59.000"))
        .put(TSI.DAY, formatTimeStampMilli.parseLocalDateTime("2015-04-01 20:49:59.000"))
        .put(TSI.WEEK, formatTimeStampMilli.parseLocalDateTime("2015-04-13 20:49:59.000"))
        .put(TSI.MONTH, formatTimeStampMilli.parseLocalDateTime("2015-05-30 20:49:59.000"))
        .put(TSI.QUARTER, formatTimeStampMilli.parseLocalDateTime("2015-09-30 20:49:59.000"))
        .put(TSI.YEAR, formatTimeStampMilli.parseLocalDateTime("2017-03-30 20:49:59.000"))
        .build();

    final MajorType timeStampType = Types.optional(MinorType.TIMESTAMP);
    final List<Pair<SchemaPath,MajorType>> expectedSchema = Collections.singletonList(
        Pair.of(SchemaPath.getSimplePath("ts"), timeStampType));

    for (Map.Entry<TSI, LocalDateTime> entry : results.entrySet()) {
      for (String name : entry.getKey().getNames()) {
        final String query = String.format(queryTemplate, name);
        // verify schema in LIMIT 0 query
        testBuilder()
            .sqlQuery(query + " LIMIT 0")
            .schemaBaseLine(expectedSchema)
            .go();

        // verify schema in query without limit
        testBuilder()
            .sqlQuery(query)
            .schemaBaseLine(expectedSchema)
            .go();

        // very output
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("ts")
            .baselineValues(entry.getValue())
            .go();
      }
    }
  }

  @Test
  public void testTimestampAddDate() throws Exception {
    final String queryTemplate = "select timestampadd(%s, 2, date '2015-03-30') as dt from (values(1))";

    final MajorType timeStampType = Types.required(MinorType.TIMESTAMP);

    final MajorType dateType = Types.required(MinorType.DATE);

    Map<TSI, List<Object>> results = ImmutableMap.<TSI, List<Object>>builder()
        // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
        // .put(TSI.MICROSECOND, ImmutableList.<Object>of(formatTimeStampMilli.parseLocalDateTime("2015-03-30 00:00:00.002"), timeStampType))
        .put(TSI.SECOND, ImmutableList.<Object>of(formatTimeStampMilli.parseLocalDateTime("2015-03-30 00:00:02.000"), timeStampType))
        .put(TSI.MINUTE, ImmutableList.<Object>of(formatTimeStampMilli.parseLocalDateTime("2015-03-30 00:02:00.000"), timeStampType))
        .put(TSI.HOUR, ImmutableList.<Object>of(formatTimeStampMilli.parseLocalDateTime("2015-03-30 02:00:00.000"), timeStampType))
        .put(TSI.DAY, ImmutableList.<Object>of(formatDate.parseLocalDateTime("2015-04-01"), dateType))
        .put(TSI.WEEK, ImmutableList.<Object>of(formatDate.parseLocalDateTime("2015-04-13"), dateType))
        .put(TSI.MONTH, ImmutableList.<Object>of(formatDate.parseLocalDateTime("2015-05-30"), dateType))
        .put(TSI.QUARTER, ImmutableList.<Object>of(formatDate.parseLocalDateTime("2015-09-30"), dateType))
        .put(TSI.YEAR, ImmutableList.<Object>of(formatDate.parseLocalDateTime("2017-03-30"), dateType))
        .build();

    for (Map.Entry<TSI, List<Object>> entry : results.entrySet()) {
      final List<Pair<SchemaPath, MajorType>> expectedSchema = Collections.singletonList(
          Pair.of(SchemaPath.getSimplePath("dt"), (MajorType) entry.getValue().get(1)));
      final Object expectedValue = entry.getValue().get(0);
      for (String name : entry.getKey().getNames()) {
        final String query = String.format(queryTemplate, name);

        // verify schema in LIMIT 0 query
        testBuilder()
            .sqlQuery(query + " LIMIT 0")
            .schemaBaseLine(expectedSchema)
            .go();

        // verify schema in query without limit
        testBuilder()
            .sqlQuery(query)
            .schemaBaseLine(expectedSchema)
            .go();

        // very output
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("dt")
            .baselineValues(expectedValue)
            .go();
      }
    }
  }

  @Test
  public void testTimestampDiff() throws Exception {
    String query = "select " +
        "timestampdiff(%s, timestamp '2015-09-10 20:49:42.000', timestamp '2017-03-30 22:50:59.050') as diff " +
        "from (values(1))";
    Map<TSI, Integer> results = ImmutableMap.<TSI, Integer>builder()
        //.put(TSI.MICROSECOND, 48996077050L)
        .put(TSI.SECOND, 48996077)
        .put(TSI.MINUTE, 816601)
        .put(TSI.HOUR, 13610)
        .put(TSI.DAY, 567)
        .put(TSI.WEEK, 81)
        .put(TSI.MONTH, 18)
        .put(TSI.QUARTER, 6)
        .put(TSI.YEAR, 1)
        .build();

    for (Map.Entry<TSI, Integer> entry : results.entrySet()) {
      for (String name : entry.getKey().getNames()) {
        testBuilder()
            .sqlQuery(String.format(query, name))
            .unOrdered()
            .baselineColumns("diff")
            .baselineValues(entry.getValue())
            .go();
      }
    }
  }

  @Test
  public void testTimestampDiffReverse() throws Exception {
    String query = "select " +
      "timestampdiff(%s, timestamp '2017-03-30 22:50:59.050', timestamp '2015-09-10 20:49:42.000') as diff " +
      "from (values(1))";
    Map<TSI, Integer> results = ImmutableMap.<TSI, Integer>builder()
      .put(TSI.SECOND, -48996077)
      .put(TSI.MINUTE, -816601)
      .put(TSI.HOUR, -13610)
      .put(TSI.DAY, -567)
      .put(TSI.WEEK, -81)
      .put(TSI.MONTH, -18)
      .put(TSI.QUARTER, -6)
      .put(TSI.YEAR, -1)
      .build();

    for (Map.Entry<TSI, Integer> entry : results.entrySet()) {
      for (String name : entry.getKey().getNames()) {
        testBuilder()
          .sqlQuery(String.format(query, name))
          .unOrdered()
          .baselineColumns("diff")
          .baselineValues(entry.getValue())
          .go();
      }
    }
  }

  @Test
  public void testTimestampAddTableauQuery() throws Exception {
    final String query = "SELECT * FROM (SELECT SUM(1) AS \"sum_Number_of_Records_ok\", " +
        "{fn TIMESTAMPADD(SQL_TSI_MONTH,CAST((-1 * (EXTRACT(MONTH FROM CAST(\"userswithdate\".\"yelping_since\" as DATE)) - 1)) AS INTEGER)," +
        "{fn TIMESTAMPADD(SQL_TSI_DAY,CAST((-1 * (EXTRACT(DAY FROM CAST(\"userswithdate\".\"yelping_since\" as DATE)) - 1)) AS INTEGER)," +
        "CAST(\"userswithdate\".\"yelping_since\" AS DATE))})} AS \"tyr_yelping_since_ok\"" +
        "FROM cp.\"yelp_user_data.json\" \"userswithdate\"" +
        "GROUP BY {fn TIMESTAMPADD(SQL_TSI_MONTH,CAST((-1 * (EXTRACT(MONTH FROM CAST(\"userswithdate\".\"yelping_since\" as DATE)) - 1)) AS INTEGER)," +
        "{fn TIMESTAMPADD(SQL_TSI_DAY,CAST((-1 * (EXTRACT(DAY FROM CAST(\"userswithdate\".\"yelping_since\" as DATE)) - 1)) AS INTEGER)," +
        "CAST(\"userswithdate\".\"yelping_since\" AS DATE))})}) T";

    final List<Pair<SchemaPath, MajorType>> expectedSchema = ImmutableList.of(
        Pair.of(SchemaPath.getSimplePath("sum_Number_of_Records_ok"),
            Types.optional(MinorType.BIGINT)),
        Pair.of(SchemaPath.getSimplePath("tyr_yelping_since_ok"),
            Types.optional(MinorType.DATE)));


    // verify schema in LIMIT 0 query
    testBuilder()
        .sqlQuery(query + " LIMIT 0")
        .schemaBaseLine(expectedSchema)
        .go();

    // verify schema in query without limit
    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .go();

    // very output
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sum_Number_of_Records_ok", "tyr_yelping_since_ok")
        .baselineValues(1L, formatDate.parseLocalDateTime("2008-01-01"))
        .baselineValues(1L, formatDate.parseLocalDateTime("2013-01-01"))
        .go();
  }

  @Test
  public void testVarianceWithDecimal() throws Exception {
    final String query = "select variance(cast(employee_id as decimal(9, 5))) from cp.\"employee.json\"";
    testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(111266.99999699896D)
        .go();
  }

  @Test
  public void testTimestampDateMinus() throws Exception {
    final String query = "select to_timestamp('2016-11-02 10:01:01','YYYY-MM-DD HH:MI:SS') - to_timestamp('2016-01-01 10:00:00','YYYY-MM-DD HH:MI:SS'), \n" +
        "date '2015-03-30' - date '2015-04-30', date '2015-04-30' - date '2015-03-30'\n" +
        "from (values(1))";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1",  "EXPR$2")
        .baselineValues(Period.days(306).plusMillis(61000), Period.days(-31), Period.days(31))
        .build()
        .run();

    final String query2 = "select to_timestamp('2016-11-02 10:01:01','YYYY-MM-DD HH:MI:SS') - CAST(NULL AS TIMESTAMP), \n" +
        "date '2015-03-30' - CAST(NULL AS DATE)\n" +
        "from (values(1))";

    testBuilder()
        .sqlQuery(query2)
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1")
        .baselineValues(null, null)
        .build()
        .run();
  }


  @Test
  public void testRandom() throws Exception {
    String query = "select 2*random()=2*random() as col1 from (values (1))";
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("col1")
            .baselineValues(false)
            .go();
  }

  @Test
  public void testRandomWithSeed() throws Exception {
    final String query = "select random(12), rand(12), random(11) from cp" +
      ".\"parquet/decimals/castFloatDecimal.parquet\" limit 3";
    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2")
      .baselineValues(0.1597116001879662, 0.1597116001879662, 0.8405179541090823)
      .baselineValues(0.7347813877263527, 0.7347813877263527, 0.5565188684135179)
      .baselineValues(0.6069965050584282, 0.6069965050584282, 0.6293125967019425)
      .go();
  }

  @Test
  public void testBitwiseOperators() throws Exception {
    // bitwise AND
    final String andQuery = "select bitwise_and(2343, 6332), bitwise_and(34531, -324234)," +
      "bitwise_and(134134531, 453324234523), bitwise_and(87453, 0), bitwise_and(-61243113, -342536472134)";
    testBuilder()
      .sqlQuery(andQuery)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4")
      .baselineValues(2084, 1122, 70949635L, 0, -342589177582L)
      .go();

    // bitwise OR
    final String orQuery = "select bitwise_or(2343, 6332), bitwise_or(34531, -324234)," +
      "bitwise_or(134134531, 453324234523), bitwise_or(87453, 0), bitwise_or(-61243113, -342536472134)";
    testBuilder()
      .sqlQuery(orQuery)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4")
      .baselineValues(6591, -290825, 453387419419L, 87453, -8537665L)
      .go();

    // bitwise NOT
    final String notQuery = "select bitwise_not(13524231), bitwise_not(-61243113), bitwise_not(0), bitwise_not(453387419419)";
    testBuilder()
      .sqlQuery(notQuery)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3")
      .baselineValues(-13524232, 61243112, -1, -453387419420L)
      .go();
  }

  @Test
  public void testKvGen() throws Exception {
    String query = "SELECT KVGEN(CONVERT_FROM('{\"1\": 0.123, \"2\": 0.456, \"3\": 0.789}', 'JSON')) FROM (values (1))";
    test(query);
  }

  @Test
  public void testDX16973() throws Exception {
    final String query = "select cast(c_float8 as decimal(18,3)) from cp" +
      ".\"parquet/decimals/castFloatDecimal.parquet\" limit 6";
    // Test with decimal v2 implementation.
    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("EXPR$0")
      .baselineValues(BigDecimal.valueOf(10.500))
      .baselineValues(BigDecimal.valueOf(10.500))
      .baselineValues(BigDecimal.valueOf(-1.000))
      .baselineValues(new BigDecimal("123456789012344.992"))
      .baselineValues(new BigDecimal("999999999999998.976"))
      .baselineValues(new BigDecimal("-999999999999998.976"))
      .go();
  }

  @Test
  public void testDecimalLiterals() throws Exception {
    final String query = "select cast('999999999999999999' as decimal(18,0)) + cast('-0.0000000000000000000000000000000000001' as decimal(38,38)) ";
    // Test with decimal v1 implementation.
    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("999999999999999999.0000000000000000000"))
      .go();

  }

  @Test
  @Ignore("DX-11334")
  public void testDecimalLiterals11() throws Exception {
    final String query = "select cast('-999999999' as decimal(9,0)) / cast('0.000000000000000000000000001' as decimal(28,28))";

    testBuilder()
      .unOrdered()
      .sqlQuery(query)
      .baselineColumns("EXPR$0")
      .baselineValues(new BigDecimal("-999999999000000000000000000000000000"))
      .go();

  }

  @Test
  public void testFlattenOnEmpty() throws Exception {
    final String query = "select flatten(col2) from cp.\"parquet/listofemptystruct.parquet\"";
    testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .schemaBaseLine(ImmutableList.of(Pair.of(SchemaPath.getSimplePath("EXPR$0"), Types.optional(MinorType.STRUCT))))
        .go();
  }

  @Test
  public void testTimeStampConvertTimezone() throws Exception {
    String query = "SELECT " +
            "convert_timezone('+01:00', '+02:00', timestamp '2008-2-23 12:23:23') as NEW_TZ1, " +
            "convert_timezone('+02:00', '+01:00', timestamp '2008-2-23 12:23:23') as NEW_TZ2, " +
            "convert_timezone('UTC', 'Asia/Kolkata', timestamp '2008-2-23 12:23:23') as NEW_TZ3, " +
            "convert_timezone('UTC', 'US/Hawaii', timestamp '2008-2-23 12:23:23') as NEW_TZ4 " +
            "FROM cp.\"tpch/region.parquet\" limit 1";

    LocalDateTime dateNewTz1 = formatTimeStampMilli.parseLocalDateTime("2008-02-23 13:23:23.0");
    LocalDateTime dateNewTz2 = formatTimeStampMilli.parseLocalDateTime("2008-02-23 11:23:23.0");
    LocalDateTime dateNewTz3 = formatTimeStampMilli.parseLocalDateTime("2008-02-23 17:53:23.0");
    LocalDateTime dateNewTz4 = formatTimeStampMilli.parseLocalDateTime("2008-02-23 02:23:23.0");
    testBuilder().sqlQuery(query)
            .unOrdered()
            .baselineColumns("NEW_TZ1", "NEW_TZ2", "NEW_TZ3", "NEW_TZ4")
            .baselineValues(dateNewTz1, dateNewTz2, dateNewTz3, dateNewTz4)
            .go();
  }

  @Test
  public void toTimstampMultipleTZAbbrevs() throws Exception {
    final String queryTable = "to_ts_multiple_abbrevs";
    final String resultTable = "to_ts_multiple_abbrevs_results";
    try (AutoCloseable c = enableIcebergTables()) {
      String table = String.format("create table %s.%s(timest varchar)", TEMP_SCHEMA, queryTable);
      String table2 = String.format("create table %s.%s(ts timestamp)", TEMP_SCHEMA, resultTable);
      test(table);
      test(table2);
      Thread.sleep(1001);

      // PST is UTC-8
      String insertTable1 = String.format("insert into %s.%s values('2018-11-23T15:36:00.000PST')", TEMP_SCHEMA, queryTable);
      String insertTable2 = String.format("insert into %s.%s values(timestamp '2018-11-23 23:36:00')", TEMP_SCHEMA, resultTable);
      test(insertTable1);
      test(insertTable2);
      Thread.sleep(1001);
      insertTable1 = String.format("insert into %s.%s values('2018-11-23T10:36:00.000PST')", TEMP_SCHEMA, queryTable);
      insertTable2 = String.format("insert into %s.%s values(timestamp '2018-11-23 18:36:00')", TEMP_SCHEMA, resultTable);
      test(insertTable1);
      test(insertTable2);
      Thread.sleep(1001);
      // PDT is UTC-7
      insertTable1 = String.format("insert into %s.%s values('2018-11-23T15:36:00.000PDT')", TEMP_SCHEMA, queryTable);
      insertTable2 = String.format("insert into %s.%s values(timestamp '2018-11-23 22:36:00')", TEMP_SCHEMA, resultTable);
      test(insertTable1);
      test(insertTable2);
      Thread.sleep(1001);
      // IST is UTC+2
      insertTable1 = String.format("insert into %s.%s values('2018-11-23T15:36:00.000IST')", TEMP_SCHEMA, queryTable);
      insertTable2 = String.format("insert into %s.%s values(timestamp '2018-11-23 13:36:00')", TEMP_SCHEMA, resultTable);
      test(insertTable1);
      test(insertTable2);
      Thread.sleep(1001);
      insertTable1 = String.format("insert into %s.%s values('2018-11-23T15:36:00.000PST')", TEMP_SCHEMA, queryTable);
      insertTable2 = String.format("insert into %s.%s values(timestamp '2018-11-23 23:36:00')", TEMP_SCHEMA, resultTable);
      test(insertTable1);
      test(insertTable2);
      Thread.sleep(1001);

      testBuilder()
        .unOrdered()
        .sqlQuery("select to_timestamp(timest, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFFTZD') ts from " + TEMP_SCHEMA + "." + queryTable)
        .sqlBaselineQuery("select * from " + TEMP_SCHEMA + "." + resultTable)
        .go();
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), queryTable));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), resultTable));
    }
  }

}
