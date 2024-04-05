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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.TestBuilder;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import java.io.File;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;

/** Integration tests for COPY INTO ... parquet. */
public class CopyIntoParquetTests extends ITCopyIntoBase {

  public static void testParquetMixedSchemas(BufferAllocator allocator, String source)
      throws Exception {
    // two files, one with INT columns, the other one with BIGINT columns, target table with BIGINT
    // columns
    String tableName = "mixed";
    File location = CopyIntoTests.createTempLocation();
    // parquet file has `INT` type
    CopyIntoTests.createTable(
        tableName, ImmutableList.of(Pair.of("x", "BIGINT"), Pair.of("y", "BIGINT")));
    CopyIntoTests.createCopyIntoSourceFiles(
        new String[] {"ints.parquet", "longs.parquet"},
        location,
        ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILE_FORMAT 'parquet'",
            CopyIntoTests.TEMP_SCHEMA, tableName, storageLocation);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT SUM(x) AS sum_x, SUM(y) AS sum_y FROM %s.%s ", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("sum_x", "sum_y")
        .baselineValues(10100L, 10706L)
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetSingleFile(BufferAllocator allocator, String source)
      throws Exception {
    String tableName = "cars";
    String fileName = "cars.parquet";
    File location = CopyIntoTests.createTempLocation();
    // parquet file has `INT` type
    CopyIntoTests.createTableAndGenerateSourceFiles(
        tableName,
        ImmutableList.of(
            Pair.of("make_year", "BIGINT"),
            Pair.of("make", "VARCHAR"),
            Pair.of("model", "VARCHAR"),
            Pair.of("description", "VARCHAR"),
            Pair.of("price", "DOUBLE")),
        new String[] {fileName},
        location,
        ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILES (\'%s\')",
            TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("make_year", "make", "model", "description", "price")
        .baselineValues(1997L, "Ford", "E350", "ac, abs, moon", 3000.00)
        .baselineValues(1999L, "Chevy", "Venture \"Extended Edition\"", null, 4900.00)
        .baselineValues(1999L, "Chevy", "Venture \"Extended Edition, Very Large\"", null, 5000.00)
        .baselineValues(
            1996L, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded", 4799.00)
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetBinaryToStringColumn(BufferAllocator allocator, String source)
      throws Exception {
    String tableName = "binary_to_string";
    String fileName = "bytes.parquet";
    File location = CopyIntoTests.createTempLocation();
    // parquet file has `BYTE ARRAY` type
    CopyIntoTests.createTableAndGenerateSourceFiles(
        tableName,
        ImmutableList.of(Pair.of("bytes", "VARCHAR")),
        new String[] {fileName},
        location,
        ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILES (\'%s\')",
            TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT * FROM %s.%s ORDER BY bytes LIMIT 4", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("bytes")
        .baselineValues("1")
        .baselineValues("10")
        .baselineValues("100")
        .baselineValues("101")
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  // the parquet file lacks a column that is present in the target schema, and we
  // expect all values in the column to be NULL.
  public static void testParquetMissingColumns(BufferAllocator allocator, String source)
      throws Exception {
    String tableName = "cars";
    String fileName = "cars.parquet";
    File location = CopyIntoTests.createTempLocation();
    // parquet file has `INT` type
    String schema =
        "make_year BIGINT, make VARCHAR, model VARCHAR, description VARCHAR, price DOUBLE, missing INT";
    CopyIntoTests.createTableAndGenerateSourceFiles(
        tableName,
        ImmutableList.of(
            Pair.of("make_year", "BIGINT"),
            Pair.of("make", "VARCHAR"),
            Pair.of("model", "VARCHAR"),
            Pair.of("description", "VARCHAR"),
            Pair.of("price", "DOUBLE"),
            Pair.of("missing", "INT")),
        new String[] {fileName},
        location,
        ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILES (\'%s\')",
            TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("make_year", "make", "model", "description", "price", "missing")
        .baselineValues(1997L, "Ford", "E350", "ac, abs, moon", 3000.00, null)
        .baselineValues(1999L, "Chevy", "Venture \"Extended Edition\"", null, 4900.00, null)
        .baselineValues(
            1999L, "Chevy", "Venture \"Extended Edition, Very Large\"", null, 5000.00, null)
        .baselineValues(
            1996L, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded", 4799.00, null)
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetExtraColumns(BufferAllocator allocator, String source)
      throws Exception {
    String tableName = "cars";
    String fileName = "cars.parquet";
    File location = CopyIntoTests.createTempLocation();
    // parquet file has `INT` type
    String schema = "make_year BIGINT, make VARCHAR, model VARCHAR, description VARCHAR";
    CopyIntoTests.createTableAndGenerateSourceFiles(
        tableName,
        ImmutableList.of(
            Pair.of("make_year", "BIGINT"),
            Pair.of("make", "VARCHAR"),
            Pair.of("model", "VARCHAR"),
            Pair.of("description", "VARCHAR")),
        new String[] {fileName},
        location,
        ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILES (\'%s\')",
            TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("make_year", "make", "model", "description")
        .baselineValues(1997L, "Ford", "E350", "ac, abs, moon")
        .baselineValues(1999L, "Chevy", "Venture \"Extended Edition\"", null)
        .baselineValues(1999L, "Chevy", "Venture \"Extended Edition, Very Large\"", null)
        .baselineValues(1996L, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded")
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetColumnCaseInsensitive(BufferAllocator allocator, String source)
      throws Exception {
    // two files, one with INT columns, the other one with BIGINT columns, target table with BIGINT
    // columns
    String tableName = "mixed";
    File location = CopyIntoTests.createTempLocation();
    // column names in parquet are lower case, 'x', 'y', but uppercase in table schema
    CopyIntoTests.createTable(
        tableName, ImmutableList.of(Pair.of("X", "INT"), Pair.of("Y", "INT")));
    CopyIntoTests.createCopyIntoSourceFiles(
        new String[] {"ints.parquet"}, location, ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILE_FORMAT 'parquet'",
            CopyIntoTests.TEMP_SCHEMA, tableName, storageLocation);
    test(copyIntoQuery);

    new TestBuilder(allocator)
        .sqlQuery("SELECT x, y FROM %s.%s ORDER BY X LIMIT 1", TEMP_SCHEMA, tableName)
        .unOrdered()
        .baselineColumns("x", "y")
        .baselineValues(0, 3)
        .go();

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetNoColumnMatch(BufferAllocator allocator, String source)
      throws Exception {
    // two files, one with INT columns, the other one with BIGINT columns, target table with BIGINT
    // columns
    String tableName = "no_column_match";
    File location = CopyIntoTests.createTempLocation();
    // column names in parquet are lower case, 'x', 'y', but uppercase in table schema
    CopyIntoTests.createTable(
        tableName, ImmutableList.of(Pair.of("a", "INT"), Pair.of("b", "INT")));
    CopyIntoTests.createCopyIntoSourceFiles(
        new String[] {"ints.parquet"}, location, ITCopyIntoBase.FileFormat.PARQUET);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILE_FORMAT 'parquet'",
            CopyIntoTests.TEMP_SCHEMA, tableName, storageLocation);
    UserExceptionAssert.assertThatThrownBy(() -> test(copyIntoQuery))
        .satisfies(
            ex ->
                assertThat(ex)
                    .hasMessageContaining(
                        "Parquet file does not contain any of the fields expected in the output"));

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testParquetWithOnErrorContinue(String source) throws Exception {
    String tableName = "not_supported_on_error";
    File location = CopyIntoTests.createTempLocation();
    CopyIntoTests.createTable(
        tableName, ImmutableList.of(Pair.of("X", "INT"), Pair.of("Y", "INT")));
    CopyIntoTests.createCopyIntoSourceFiles(
        new String[] {"ints.parquet"}, location, ITCopyIntoBase.FileFormat.PARQUET);
    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s FILE_FORMAT 'parquet' (ON_ERROR 'continue')",
            CopyIntoTests.TEMP_SCHEMA, tableName, storageLocation);
    UserExceptionAssert.assertThatThrownBy(() -> test(copyIntoQuery))
        .satisfies(
            ex ->
                assertThat(ex)
                    .hasMessageContaining(
                        "ON_ERROR 'continue' option is not supported for parquet file format"));

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }
}
