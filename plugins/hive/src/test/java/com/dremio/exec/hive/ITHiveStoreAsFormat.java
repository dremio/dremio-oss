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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.hive.HiveTestDataGenerator;

@RunWith(Parameterized.class)
public class ITHiveStoreAsFormat extends HiveTestBase {
  private final String tableFormat;
  private static AutoCloseable mapEnabled;
  private static AutoCloseable complexTypeEnabled;

  @BeforeClass
  public static void enableMapFeature() {
    mapEnabled = enableMapDataType();
  }

  @AfterClass
  public static void resetMapFeature() throws Exception {
    mapEnabled.close();
  }

  @BeforeClass
  public static void enableComplexTypeFeature() {
    complexTypeEnabled = enableComplexHiveType();
  }

  @AfterClass
  public static void resetComplexTypeFeature() throws Exception {
    complexTypeEnabled.close();
  }

  @Parameterized.Parameters(name = "Table Format {0}")
  public static List<String> listTableFormats() {
    return HiveTestDataGenerator.listStoreAsFormatsForTests();
  }

  public ITHiveStoreAsFormat(String tableFormat) {
    this.tableFormat = tableFormat;
  }

  @Test
  public void readAllSupportedHiveDataTypes() throws Exception {
    readAllSupportedHiveDataTypes("readtest_" + this.tableFormat);
  }

  @Test
  public void readComplexHiveDataTypes() throws Exception {
    readComplexHiveDataTypes("orccomplex" + this.tableFormat);
  }

  @Test
  public void readListOfStruct() throws Exception {
    readListOfStruct("orccomplex" + this.tableFormat);
  }

  @Test
  public void readListHiveDataTypes() throws Exception {
    readListHiveDataTypes("orclist" + this.tableFormat);
  }

  @Test
  public void readStructHiveDataTypes() throws Exception {
    readStructHiveDataTypes("orcstruct" + this.tableFormat);
  }

  @Test
  public void testDropOfUnionHiveDataTypes() throws Exception {
    readTableWithUnionHiveDataTypes("orcunion" + this.tableFormat);
  }

  /**
   * Test to ensure Dremio fails to read union data from hive
   */
  private void readTableWithUnionHiveDataTypes(String table) {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int row : testrows) {
      try {
        testBuilder().sqlQuery("SELECT * FROM hive." + table + " order by rownum limit 1 offset " + row)
          .ordered()
          .baselineColumns("rownum")
          .baselineValues(row)
          .go();
      } catch (Exception e) {
        e.printStackTrace();
        assertThat(e.getMessage()).contains(BatchSchema.MIXED_TYPES_ERROR);
      }
    }
  }

  /**
   * Test to ensure Dremio reads list of primitive data types
   * @throws Exception
   */
  private void readStructHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int index : testrows) {
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<>();
      structrow1.put("tinyint_field", 1);
      structrow1.put("smallint_field", 1024);
      structrow1.put("int_field", index);
      structrow1.put("bigint_field", 90000000000L);
      structrow1.put("float_field", (float) index);
      structrow1.put("double_field", (double) index);
      structrow1.put("decimal_field", new BigDecimal(Double.toString(index)).setScale(2, RoundingMode.HALF_UP));
      structrow1.put("string_field", new Text(Integer.toString(index)));

      testBuilder().sqlQuery("SELECT * FROM hive." + table +
          " order by rownum limit 1 offset " + index)
          .ordered()
          .baselineColumns("rownum", "struct_field")
          .baselineValues(index, structrow1)
          .go();

      testBuilder().sqlQuery("SELECT rownum, struct_field['string_field'] AS string_field, struct_field['int_field'] AS int_field FROM hive." + table +
          " order by rownum limit 1 offset " + index)
          .ordered()
          .baselineColumns("rownum", "string_field", "int_field")
          .baselineValues(index, Integer.toString(index), index)
          .go();
    }
  }

  /**
   * Test to ensure Dremio reads list of primitive data types
   * @throws Exception
   */
  private void readListHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int testrow : testrows) {
      if (testrow % 7 == 0) {
        Integer index = testrow;
        testBuilder()
            .sqlQuery("SELECT rownum,double_field,string_field FROM hive." + table + " order by rownum limit 1 offset " + index.toString())
            .ordered()
            .baselineColumns("rownum", "double_field", "string_field")
            .baselineValues(index, null, null)
            .go();
      } else {
        JsonStringArrayList<Text> string_field = new JsonStringArrayList<>();
        string_field.add(new Text(Integer.toString(testrow)));
        string_field.add(new Text(Integer.toString(testrow + 1)));
        string_field.add(new Text(Integer.toString(testrow + 2)));
        string_field.add(new Text(Integer.toString(testrow + 3)));
        string_field.add(new Text(Integer.toString(testrow + 4)));

        testBuilder()
            .sqlQuery("SELECT rownum,double_field,string_field FROM hive." + table + " order by rownum limit 1 offset " + testrow)
            .ordered()
            .baselineColumns("rownum", "double_field", "string_field")
            .baselineValues(
                testrow,
                asList((double) testrow, (double) (testrow + 1), (double) (testrow + 2), (double) (testrow + 3), (double) (testrow + 4)),
                string_field)
            .go();
      }
    }
  }

  /**
   * Test to ensure Dremio reads the all ORC complex types correctly
   * @throws Exception
   */
  private void readComplexHiveDataTypes(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int index : testrows) {
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<>();
      structrow1.put("name", new Text("name" + index));
      structrow1.put("age", index);

      JsonStringHashMap<String, Object> structlistrow1 = new JsonStringHashMap<>();
      structlistrow1.put("type", new Text("type" + index));
      structlistrow1.put("value", new ArrayList<>(singletonList(new Text("elem" + index))));

      JsonStringHashMap<String, Object> liststruct1 = new JsonStringHashMap<>();
      liststruct1.put("name", new Text("name" + index));
      liststruct1.put("age", index);
      JsonStringHashMap<String, Object> liststruct2 = new JsonStringHashMap<>();
      liststruct2.put("name", new Text("name" + (index + 1)));
      liststruct2.put("age", index + 1);

      JsonStringHashMap<String, Object> mapstruct1 = new JsonStringHashMap<>();
      mapstruct1.put("key", new Text("name" + index));
      mapstruct1.put("value", index);
      JsonStringHashMap<String, Object> mapstruct2 = new JsonStringHashMap<>();
      mapstruct2.put("key", new Text("name" + (index + 1)));
      mapstruct2.put("value", index + 1);
      JsonStringHashMap<String, Object> mapstruct3 = null;
      if (index % 2 == 0) {
        mapstruct3 = new JsonStringHashMap<>();
        mapstruct3.put("key", new Text("name" + (index + 2)));
        mapstruct3.put("value", index + 2);
      }

      JsonStringHashMap<String, Object> mapstructValue = new JsonStringHashMap<>();
      mapstructValue.put("key", new Text("key" + index));
      JsonStringHashMap<String, Object> mapstructValue2 = new JsonStringHashMap<>();
      mapstructValue2.put("type", new Text("struct" + index));
      mapstructValue.put("value", mapstructValue2);

      testBuilder()
          .sqlQuery("SELECT * FROM hive." + table + " order by rownum limit 1 offset " + index)
          .ordered()
          .baselineColumns("rownum", "list_field", "struct_field", "struct_list_field", "list_struct_field", "map_field", "map_struct_field")
          .baselineValues(
              index,
              asList(index, index + 1, index + 2, index + 3, index + 4),
              structrow1,
              structlistrow1,
              asList(liststruct1, liststruct2),
              index % 2 == 0 ? asList(mapstruct1, mapstruct2, mapstruct3) : asList(mapstruct1, mapstruct2),
              asList(mapstructValue))
          .go();
    }
  }

  private void readListOfStruct(String table) throws Exception {
    int[] testrows = {0, 500, 1022, 1023, 1024, 4094, 4095, 4096, 4999};
    for (int index : testrows) {
      testBuilder().sqlQuery("SELECT list_struct_field[0].name as name FROM hive." + table +
          " order by rownum limit 1 offset " + index)
        .ordered()
        .baselineColumns("name")
        .baselineValues("name" + index)
        .go();
      testBuilder().sqlQuery("SELECT list_struct_field[1].name as name FROM hive." + table +
          " order by rownum limit 1 offset " + index)
        .ordered()
        .baselineColumns("name")
        .baselineValues("name" + (index + 1))
        .go();
      testBuilder().sqlQuery("SELECT list_struct_field[0].age as age FROM hive." + table +
          " order by rownum limit 1 offset " + index)
        .ordered()
        .baselineColumns("age")
        .baselineValues(index)
        .go();
      testBuilder().sqlQuery("SELECT list_struct_field[1].age as age FROM hive." + table +
          " order by rownum limit 1 offset " + index)
        .ordered()
        .baselineColumns("age")
        .baselineValues(index + 1)
        .go();
    }
  }

  /**
   * Test to ensure Dremio reads the all supported types correctly both normal fields (converted to Nullable types) and
   * partition fields (converted to Required types).
   * @throws Exception
   */
  private void readAllSupportedHiveDataTypes(String table) throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive." + table)
        .ordered()
        .baselineColumns(
            "binary_field",
            "boolean_field",
            "tinyint_field",
            "decimal0_field",
            "decimal9_field",
            "decimal18_field",
            "decimal28_field",
            "decimal38_field",
            "double_field",
            "float_field",
            "int_field",
            "bigint_field",
            "smallint_field",
            "string_field",
            "varchar_field",
            "timestamp_field",
            "date_field",
            "char_field",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary_part",
            "boolean_part",
            "tinyint_part",
            "decimal0_part",
            "decimal9_part",
            "decimal18_part",
            "decimal28_part",
            "decimal38_part",
            "double_part",
            "float_part",
            "int_part",
            "bigint_part",
            "smallint_part",
            "string_part",
            "varchar_part",
            "timestamp_part",
            "date_part",
            "char_part")
        .baselineValues(
            "binaryfield".getBytes(),
            false,
            34,
            new BigDecimal("66"),
            new BigDecimal("2347.92"),
            new BigDecimal("2758725827.99990"),
            new BigDecimal("29375892739852.8"),
            new BigDecimal("89853749534593985.783"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "stringfield",
            "varcharfield",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime(), UTC),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "charfield",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .baselineValues( // All fields are null, but partition fields have non-null values
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new LocalDateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new LocalDateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .go();
  }
}
