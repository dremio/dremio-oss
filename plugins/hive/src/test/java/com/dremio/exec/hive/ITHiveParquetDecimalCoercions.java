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
package com.dremio.exec.hive;

import static com.dremio.exec.hive.HiveTestUtilities.executeQuery;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.Driver;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITHiveParquetDecimalCoercions extends LazyDataGeneratingHiveTestBase {
  private static final String TABLE_TO_TEST_CONTRACTION = "contracting_parquet";
  private static final String[] VALUES_FOR_CONTRACTION = new String[]{
    // contracting conversions
    "1.12345", // reduce S - this should become 1.12
    "10000.00", // reduce P - this should become null on overflow
    "10000.12345", // reduce P and S - this should become null on overflow
    "123456", // this should become null on overflow
  };

  private static final String TABLE_TO_TEST_EXPANSION = "expanding_parquet";
  private static final String[] VALUES_FOR_EXPANSION = new String[]{
    // expanding coercions
    "1234", // increase s - this should become 1234.00
    "1234.0", // increase s - this should become 1234.00
    "1234.1", // increase s - this should become 1234.10
    "1.00", // increase p - this should remain as is
    "10.00", // increase p - this should remain as is
    "10.0", // increase p and s - this should become 10.00
  };

  @BeforeClass
  public static void setup() throws Exception {
    dataGenerator.generateTestData(ITHiveParquetDecimalCoercions::generateTestData);
  }

  @Test
  public void testContractingDecimalCoercions() throws Exception {
    verifySqlQueryForSingleBaselineColumn(
      "SELECT * FROM hive." + TABLE_TO_TEST_CONTRACTION,
      "col1",
      new Object[]{bigD("1.12"), null, null, null});
  }

  @Test
  public void testExpandingDecimalCoercions() throws Exception {
    verifySqlQueryForSingleBaselineColumn(
      "SELECT * FROM hive." + TABLE_TO_TEST_EXPANSION,
      "col1",
      new Object[]{bigD("1234.00"), bigD("1234.00"), bigD("1234.10"), bigD("1.00"), bigD("10.00"), bigD("10.00")});
  }

  private BigDecimal bigD(String s) {
    return new BigDecimal(s);
  }

  private static Void generateTestData(Driver hiveDriver) {
    createTableAndInsertValues(hiveDriver, TABLE_TO_TEST_CONTRACTION, VALUES_FOR_CONTRACTION);
    createTableAndInsertValues(hiveDriver, TABLE_TO_TEST_EXPANSION, VALUES_FOR_EXPANSION);
    return null;
  }

  private static void createTableAndInsertValues(Driver hiveDriver, String contractingTableName, String[] contractingValues) {
    executeQuery(hiveDriver, "CREATE TABLE " + contractingTableName + " (col1 decimal(6,2)) STORED AS parquet");
    String joinedValue = Arrays.stream(contractingValues).map(s -> "(" + s + ")").collect(Collectors.joining(","));
    executeQuery(hiveDriver, "INSERT INTO " + contractingTableName + " VALUES " + joinedValue);
  }
}
