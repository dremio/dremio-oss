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
import static org.joda.time.DateTimeZone.UTC;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.Driver;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITHiveOrcCoercions extends LazyDataGeneratingHiveTestBase {
  @BeforeClass
  public static void setup() throws Exception {
    dataGenerator.generateTestData(hiveDriver -> generateTestData(hiveDriver, dataGenerator.getWhDir()));
  }

  @Test
  public void orcTestTimestampMilli() throws Exception {
    String query = "SELECT col1 FROM hive." + "timestamp" + "_orc";
    testBuilder().sqlQuery(query)
        .ordered()
        .baselineColumns("col1")
        .baselineValues(new LocalDateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC))
        .go();
  }

  @Test
  public void orcTestTinyIntToString() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.tinyint_to_string_orc_ext")
        .ordered()
        .baselineColumns("col1")
        .baselineValues("90")
        .go();
  }

  @Test
  public void orcTestTinyIntToBigInt() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.tinyint_to_bigint_orc_ext")
        .ordered()
        .baselineColumns("col1")
        .baselineValues(90L)
        .go();
  }

  @Test
  public void orcTestTypeConversions() throws Exception {
    Object[][] testcases = {
        //tinyint
        {"tinyint", "smallint", 90, "90"},
        {"tinyint", "int", 90, "90"},
        {"tinyint", "bigint", 90L, "90"},
        {"tinyint", "float", 90f, "90.0"},
        {"tinyint", "double", 90d, "90.0"},
        {"tinyint", "decimal", new BigDecimal(90), "90.0"},
        {"tinyint", "string", "90"},
        {"tinyint", "varchar", "90"},
        //smallint
        {"smallint", "int", 90, "90"},
        {"smallint", "bigint", 90L, "90"},
        {"smallint", "float", 90f, "90.0"},
        {"smallint", "double", 90d, "90.0"},
        {"smallint", "decimal", new BigDecimal(90), "90.0"},
        {"smallint", "string", "90"},
        {"smallint", "varchar", "90"},
        //int
        {"int", "bigint", 90L, "90"},
        {"int", "float", 90f, "90.0"},
        {"int", "double", 90d, "90.0"},
        {"int", "decimal", new BigDecimal(90), "90.0"},
        {"int", "string", "90"},
        {"int", "varchar", "90"},
        //bigint
        {"bigint", "float", 90f, "90.0"},
        {"bigint", "double", 90d, "90.0"},
        {"bigint", "decimal", new BigDecimal(90), "90.0"},
        {"bigint", "string", "90"},
        {"bigint", "varchar", "90"},
        //float
        {"float", "double", 90d, "90.0"},
        {"float", "decimal", new BigDecimal(90), "90.0"},
        {"float", "string", "90.0"},
        {"float", "varchar", "90.0"},
        //double
        {"double", "decimal", new BigDecimal(90), "90.0"},
        {"double", "string", "90.0"},
        {"double", "varchar", "90.0"},
        //decimal
        {"decimal", "string", "90"},
        {"decimal", "varchar", "90"},
        //string
        {"string", "double", 90d, "90.0"},
        {"string", "decimal", new BigDecimal(90), "90.0"},
        {"string", "varchar", "90"},
        //varchar
        {"varchar", "double", 90d, "90.0"},
        {"varchar", "decimal", new BigDecimal(90), "90.0"},
        {"varchar", "string", "90"},
        //timestamp
        {"timestamp", "string", Long.toString(new DateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC).getMillis())},
        {"timestamp", "varchar", Long.toString(new DateTime(Timestamp.valueOf("2019-03-14 11:17:31.119021").getTime(), UTC).getMillis())},
        //date
        {"date", "string", "17969"},
        {"date", "varchar", "17969"}
    };
    hiveTestTypeConversions(testcases);
  }

  private static Void generateTestData(Driver hiveDriver, String whDir) {
    final String[][] typeConversionTables = {
        {"tinyint", "", "90"},
        {"smallint", "", "90"},
        {"int", "", "90"},
        {"bigint", "", "90"},
        {"float", "", "90.0"},
        {"double", "", "90.0"},
        {"decimal", "", "90"},
        {"string", "", "90"},
        {"varchar", "(1024)", "90"},
        {"timestamp", "", "'2019-03-14 11:17:31.119021'"},
        {"date", "", "'2019-03-14'"}
    };
    for (String[] srcTable : typeConversionTables) {
      createTypeConversionSourceTable(hiveDriver, srcTable[0], srcTable[1], srcTable[2]);
    }

    final String[][] typeConversionDestTables = {
        //tinyint
        {"tinyint", "smallint", ""},
        {"tinyint", "int", ""},
        {"tinyint", "bigint", ""},
        {"tinyint", "float", ""},
        {"tinyint", "double", ""},
        {"tinyint", "decimal", ""},
        {"tinyint", "string", ""},
        {"tinyint", "varchar", "(1024)"},
        //smallint
        {"smallint", "int", ""},
        {"smallint", "bigint", ""},
        {"smallint", "float", ""},
        {"smallint", "double", ""},
        {"smallint", "decimal", ""},
        {"smallint", "string", ""},
        {"smallint", "varchar", "(1024)"},
        //int
        {"int", "bigint", ""},
        {"int", "float", ""},
        {"int", "double", ""},
        {"int", "decimal", ""},
        {"int", "string", ""},
        {"int", "varchar", "(1024)"},
        //bigint
        {"bigint", "float", ""},
        {"bigint", "double", ""},
        {"bigint", "decimal", ""},
        {"bigint", "string", ""},
        {"bigint", "varchar", "(1024)"},
        //float
        {"float", "double", ""},
        {"float", "decimal", ""},
        {"float", "string", ""},
        {"float", "varchar", "(1024)"},
        //double
        {"double", "decimal", ""},
        {"double", "string", ""},
        {"double", "varchar", "(1024)"},
        //decimal
        {"decimal", "string", ""},
        {"decimal", "varchar", "(1024)"},
        //string
        {"string", "double", ""},
        {"string", "decimal", ""},
        {"string", "varchar", "(1024)"},
        //varchar
        {"varchar", "double", ""},
        {"varchar", "decimal", ""},
        {"varchar", "string", ""},
        //timestamp
        {"timestamp", "string", ""},
        {"timestamp", "varchar", "(1024)"},
        //date
        {"date", "string", ""},
        {"date", "varchar", "(1024)"}
    };

    for (String[] destTable : typeConversionDestTables) {
      createTypeConversionDestinationTable(hiveDriver, whDir, destTable[0], destTable[1], destTable[2]);
    }
    return null;
  }

  private static void createTypeConversionSourceTable(Driver hiveDriver, String source, String sourceTypeArgs, String value) {
    String table = source + "_orc";
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 " + source + sourceTypeArgs + ") STORED AS orc";
    executeQuery(hiveDriver, datatable);
    String insertQuery = "INSERT INTO " + table + " VALUES (" + value + ")";
    executeQuery(hiveDriver, insertQuery);
  }

  private static void createTypeConversionDestinationTable(Driver driver, String whDir, String src, String dest, String destTypeArgs) {
    String table = src + "_to_" + dest + "_orc_ext";
    String sourceTable = src + "_orc";
    String extTable = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table +
        " (col1 " + dest + destTypeArgs + ") STORED AS orc" +
        " LOCATION 'file://" + whDir + "/" + sourceTable + "'";
    executeQuery(driver, extTable);
  }

  private void hiveTestTypeConversions(Object[][] testcases) throws Exception {
    for (Object[] testcase : testcases) {
      String query = "SELECT * FROM hive." + testcase[0] + "_to_" + testcase[1] + "_orc_ext";
      testBuilder().sqlQuery(query)
          .ordered()
          .baselineColumns("col1")
          .baselineValues(testcase[2])
          .go();
    }

    for (Object[] testcase : testcases) {
      if (testcase.length == 4) {
        String query = "SELECT * FROM hive." + testcase[0] + "_to_" + testcase[1] + "_orc_ext where col1 = " + testcase[3];
        testBuilder().sqlQuery(query)
            .ordered()
            .baselineColumns("col1")
            .baselineValues(testcase[2])
            .go();
      }
    }
  }
}
