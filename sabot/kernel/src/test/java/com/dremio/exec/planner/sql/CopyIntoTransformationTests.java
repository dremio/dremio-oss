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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;

public class CopyIntoTransformationTests extends ITCopyIntoBase {

  static void testNoMapping(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "NoMapping";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(Pair.of("m", "VARCHAR"), Pair.of("y", "BIGINT"), Pair.of("p", "DOUBLE"));
    String[] transformations = new String[] {"model", "make_year", "price"};
    String[] inputFileNames = new String[] {"cars"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          null,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation("ITCopyIntoTransformationsNoMapping.json", fileFormat))
          .go();
      dropTable(targetTableName);
    }
  }

  static void testWithMapping(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "WithMapping";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(Pair.of("m", "VARCHAR"), Pair.of("y", "BIGINT"), Pair.of("p", "DOUBLE"));
    String[] mappings = new String[] {"p", "m"};
    String[] transformations = new String[] {"price", "model"};
    String[] inputFileNames = new String[] {"cars"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .baselineColumns(
              tableSchema.stream()
                  .map(Pair::getLeft)
                  .collect(Collectors.toList())
                  .toArray(new String[tableSchema.size()]))
          .baselineValues("E350", null, 3000.00)
          .baselineValues("Venture \"Extended Edition\"", null, 4900.00)
          .baselineValues("Venture \"Extended Edition, Very Large\"", null, 5000.00)
          .baselineValues("Grand Cherokee", null, 4799.00)
          .go();
      dropTable(targetTableName);
    }
  }

  public static void testInvalidMapping(
      String source, OnErrorAction onErrorAction, FileFormat fileFormat) throws Exception {
    String targetTableName = "InvalidMapping";
    String[] inputFileNames = new String[] {"cars"};
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(Pair.of("m", "VARCHAR"), Pair.of("y", "BIGINT"), Pair.of("p", "DOUBLE"));

    Exception exception =
        assertThrows(
            UserException.class,
            () ->
                prepareInputAndRunCopyIntoTransformation(
                    source,
                    targetTableName,
                    tableSchema,
                    new String[] {"p", "e", "m", "d"},
                    new String[] {"price", "model"},
                    inputFileNames,
                    fileFormat,
                    onErrorAction));
    assertThat(exception.getMessage()).contains("Incorrect transformation mapping definition");

    exception =
        assertThrows(
            UserException.class,
            () ->
                prepareInputAndRunCopyIntoTransformation(
                    source,
                    targetTableName,
                    tableSchema,
                    new String[] {"p", "m"},
                    new String[] {"price", "model", "make_year"},
                    inputFileNames,
                    fileFormat,
                    onErrorAction));
    assertThat(exception.getMessage())
        .contains("Number of columns in mapping definition does not match");

    dropTable(targetTableName);
  }

  static void testUnsupportedFileFormat(String source, FileFormat fileFormat) throws Exception {
    String targetTableName = "UnsupportedFileFormat";
    String[] inputFileNames = new String[] {"cars"};
    List<Pair<String, String>> tableSchema = ImmutableList.of(Pair.of("a", "bigint"));
    Exception exception =
        assertThrows(
            UserException.class,
            () ->
                prepareInputAndRunCopyIntoTransformation(
                    source,
                    targetTableName,
                    tableSchema,
                    null,
                    new String[] {"b"},
                    inputFileNames,
                    fileFormat,
                    OnErrorAction.ABORT));
    assertThat(exception.getMessage())
        .contains("Copy Into transformations is only supported for Parquet inputs");

    dropTable(targetTableName);
  }

  static void testMultipleInputs(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "MultipleInputs";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("year", "bigint"), Pair.of("new_price", "double"), Pair.of("brand", "varchar"));
    String[] mappings = new String[] {"new_price", "brand", "year"};
    String[] transformations = new String[] {"price", "make", "make_year"};
    String[] inputFileNames = new String[] {"cars", "cars1", "cars2"};
    for (FileFormat fileFormat : fileFormats) {

      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation(
                  "ITCopyIntoTransformationsMultipleInputs.json", fileFormat))
          .go();

      dropTable(targetTableName);
    }
  }

  static void testIncompatibleTransformationTypes(
      BufferAllocator allocator, String source, OnErrorAction onErrorAction, FileFormat fileFormat)
      throws Exception {
    String targetTableName = "IncompatibleTransformationTypes";
    List<Pair<String, String>> tableSchema = ImmutableList.of(Pair.of("is_cheap", "boolean"));
    String[] transformations = new String[] {"model"};
    String[] inputFileNames = new String[] {"cars"};
    if (onErrorAction == OnErrorAction.ABORT) {
      UserException exception =
          assertThrows(
              UserException.class,
              () ->
                  prepareInputAndRunCopyIntoTransformation(
                      source,
                      targetTableName,
                      tableSchema,
                      null,
                      transformations,
                      inputFileNames,
                      fileFormat,
                      onErrorAction));

      assertThat(exception.getMessage()).contains("Invalid value for boolean");
    } else if (onErrorAction == OnErrorAction.SKIP_FILE) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          null,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .expectsEmptyResultSet()
          .go();
    }
    dropTable(targetTableName);
  }

  static void testPrimitiveTransformations(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "PrimitiveTransformations";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(Pair.of("new_name", "varchar"), Pair.of("brand", "varchar"));
    String[] mappings = new String[] {"brand", "new_name"};
    String[] transformations = new String[] {"\"make\"", "concat(\"model\", \"make\")"};
    String[] inputFileNames = new String[] {"cars1"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation(
                  "ITCopyIntoTransformationsPrimitiveTransformations.json", fileFormat))
          .go();

      dropTable(targetTableName);
    }
  }

  public static void testPrimitiveNestedTransformations(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "PrimitiveNestedTransformations";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("day", "bigint"), Pair.of("month", "int"), Pair.of("curr_year", "int"));
    String[] transformations =
        new String[] {
          "\"day\"(\"from\")",
          "extract(month from date_add(date_trunc('year', cast(\"to\" as date)), 42))",
          "extract(year from current_timestamp())"
        };
    String[] inputFileNames = new String[] {"dates"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          null,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      int currYear = LocalDate.now().getYear();

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .baselineColumns(
              tableSchema.stream()
                  .map(Pair::getLeft)
                  .collect(Collectors.toList())
                  .toArray(new String[tableSchema.size()]))
          .baselineValues(28L, 2, currYear)
          .baselineValues(13L, 2, currYear)
          .baselineValues(14L, 2, currYear)
          .baselineValues(6L, 2, currYear)
          .baselineValues(18L, 2, currYear)
          .go();

      dropTable(targetTableName);
    }
  }

  static void testPrimitiveFromComplex(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "PrimitiveFromComplex";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("struct_string", "varchar"),
            Pair.of("list_string", "varchar"),
            Pair.of("struct_list_string", "varchar"),
            Pair.of("list_struct_string", "varchar"));
    String[] mappings =
        new String[] {"list_struct_string", "struct_string", "list_string", "struct_list_string"};
    String[] transformations =
        new String[] {
          "\"nested_list\"[0]['string_value']",
          "\"struct\"['string_value']",
          "\"string_list\"[1]",
          "\"nested_struct\"['string_list'][0]",
        };
    String[] inputFileNames = new String[] {"nested_10rows"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation(
                  "ITCopyIntoTransformationsPrimitiveFromComplex.json", fileFormat))
          .go();

      dropTable(targetTableName);
    }
  }

  static void testMapComplexType(
      BufferAllocator allocator, String source, OnErrorAction onErrorAction, FileFormat fileFormat)
      throws Exception {
    String targetTableName = "MapComplexType";
    List<Pair<String, String>> tableSchema = ImmutableList.of(Pair.of("str", "varchar"));
    String[] transformations = new String[] {"int_string_map[1139133631]"};
    String[] inputFileNames = new String[] {"nested_10rows"};
    if (onErrorAction == OnErrorAction.ABORT) {
      UserException exception =
          assertThrows(
              UserException.class,
              () ->
                  prepareInputAndRunCopyIntoTransformation(
                      source,
                      targetTableName,
                      tableSchema,
                      null,
                      transformations,
                      inputFileNames,
                      fileFormat,
                      onErrorAction));
      assertThat(exception.getMessage())
          .contains("Dremio does not support casting or coercing map");
    } else if (onErrorAction == OnErrorAction.SKIP_FILE) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          null,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .expectsEmptyResultSet()
          .go();
    }
    dropTable(targetTableName);
  }

  static void testComplexFromComplex(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "ComplexFromComplex";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of(
                "mystruct", "struct<int_value int, string_value varchar, boolean_value boolean>"),
            Pair.of("mylist", "list<bigint>"));
    String[] mappings = new String[] {"mylist", "mystruct"};
    String[] transformations = new String[] {"long_list", "\"struct\""};
    String[] inputFileNames = new String[] {"nested_not_null_10rows"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation(
                  "ITCopyIntoTransformationsComplexFromComplex.json", fileFormat))
          .ignoreColumnTypes()
          .go();
      dropTable(targetTableName);
    }
  }

  static void testPrimitiveTransformationFromComplex(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "PrimitiveTransformationFromComplex";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("mod", "int"),
            Pair.of("sign", "int"),
            Pair.of("sin", "double"),
            Pair.of("radians", "float"));
    String[] mappings = new String[] {"sign", "sin", "mod", "radians"};
    String[] transformations =
        new String[] {
          "sign(nested_struct['long_list'][1])",
          "sin(nested_list[2]['int_value'])",
          "mod(long_list[0], 10)",
          "radians(long_list[1])",
        };
    String[] inputFileNames = new String[] {"nested_not_null_10rows"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .baselineColumns("mod", "sign", "sin", "radians ")
          .baselineValues(-1, 1, null, null)
          .baselineValues(-2, null, null, 5.4400614E15f)
          .baselineValues(1, null, null, 1.12961797E17f)
          .baselineValues(-2, -1, 0.4341074856445706d, -3.35783185E16f)
          .baselineValues(-2, null, -0.3427014530131323d, null)
          .baselineValues(null, null, 0.9645501176127014d, null)
          .baselineValues(-9, -1, null, -7.2323951E16f)
          .baselineValues(null, null, 0.8435350666478159d, null)
          .baselineValues(-2, null, 0.31598390919200564d, null)
          .baselineValues(-9, -1, -0.002531838264750627d, -9.2802776E16f)
          .go();

      dropTable(targetTableName);
    }
  }

  static void testSelectNonExistentColumnFromSource(
      String source, OnErrorAction onErrorAction, FileFormat... fileFormats) throws Exception {
    String targetTableName = "SelectNonExistentColumnFromSource";
    List<Pair<String, String>> tableSchema = ImmutableList.of(Pair.of("brand", "varchar"));
    String[] mappings = new String[] {"brand"};
    String[] transformations = new String[] {"not_existing_col_name"};
    String[] inputFileNames = new String[] {"cars"};
    for (FileFormat fileFormat : fileFormats) {
      UserException exception =
          assertThrows(
              UserException.class,
              () ->
                  prepareInputAndRunCopyIntoTransformation(
                      source,
                      targetTableName,
                      tableSchema,
                      mappings,
                      transformations,
                      inputFileNames,
                      fileFormat,
                      onErrorAction));
      assertThat(exception.getMessage()).contains("Copy Into transformation select list");
      dropTable(targetTableName);
    }
  }

  static void testTypeError(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "TypeError";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(Pair.of("new_name", "VARCHAR"), Pair.of("new_age", "int"));
    String[] transformations = new String[] {"length(name), age - 4"};
    String[] inputFileNames = new String[] {"typeError"};

    for (FileFormat fileFormat : fileFormats) {
      if (onErrorAction == OnErrorAction.ABORT) {
        assertThrows(
            UserException.class,
            () ->
                prepareInputAndRunCopyIntoTransformation(
                    source,
                    targetTableName,
                    tableSchema,
                    null,
                    transformations,
                    inputFileNames,
                    fileFormat,
                    onErrorAction));
      } else if (onErrorAction == OnErrorAction.SKIP_FILE) {
        prepareInputAndRunCopyIntoTransformation(
            source,
            targetTableName,
            tableSchema,
            null,
            transformations,
            inputFileNames,
            fileFormat,
            onErrorAction);
      } else {
        fail(String.format("Unknown on_error action %s", onErrorAction));
      }

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .expectsEmptyResultSet()
          .go();

      dropTable(targetTableName);
    }
  }

  static void testSyntaxError(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "SyntaxError";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("tmp_id", "int"),
            Pair.of("tmp_customerName", "varchar"),
            Pair.of("tmp_address", "varchar"));
    String[] transformations = new String[] {"id", "customerName", "address"};
    String[] inputFileNames = new String[] {"syntaxError"};
    for (FileFormat fileFormat : fileFormats) {
      if (onErrorAction == OnErrorAction.ABORT) {
        UserException exception =
            assertThrows(
                UserException.class,
                () ->
                    prepareInputAndRunCopyIntoTransformation(
                        source,
                        targetTableName,
                        tableSchema,
                        null,
                        transformations,
                        inputFileNames,
                        fileFormat,
                        onErrorAction));
        assertThat(exception.getMessage()).contains("is not in Parquet format.");
      } else if (onErrorAction == OnErrorAction.SKIP_FILE) {
        prepareInputAndRunCopyIntoTransformation(
            source,
            targetTableName,
            tableSchema,
            null,
            transformations,
            inputFileNames,
            fileFormat,
            onErrorAction);
      } else {
        fail(String.format("Unknown on_error action %s", onErrorAction));
      }

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .expectsEmptyResultSet()
          .go();
      dropTable(targetTableName);
    }
  }

  static void testMultipleInputsWithError(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "MultipleInputsWithError";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("name_with_age", "varchar"),
            Pair.of("age_mod", "int"),
            Pair.of("is_adult", "boolean"));
    String[] mappings = new String[] {"is_adult", "age_mod", "name_with_age"};
    String[] transformations = new String[] {"age >= 18", "mod(age, 10)", "concat(name, age)"};
    String[] inputFileNames =
        new String[] {
          "typeError",
          "typeError1",
          "typeError2",
          "typeError3",
          "typeError4",
          "typeError5",
          "typeError6",
          "typeError7",
          "typeError8"
        };
    for (FileFormat fileFormat : fileFormats) {
      if (onErrorAction == OnErrorAction.ABORT) {
        assertThrows(
            UserException.class,
            () ->
                prepareInputAndRunCopyIntoTransformation(
                    source,
                    targetTableName,
                    tableSchema,
                    mappings,
                    transformations,
                    inputFileNames,
                    fileFormat,
                    onErrorAction));

        new TestBuilder(allocator)
            .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
            .unOrdered()
            .expectsEmptyResultSet()
            .go();
      } else if (onErrorAction == OnErrorAction.SKIP_FILE) {
        prepareInputAndRunCopyIntoTransformation(
            source,
            targetTableName,
            tableSchema,
            mappings,
            transformations,
            inputFileNames,
            fileFormat,
            onErrorAction);

        new TestBuilder(allocator)
            .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
            .unOrdered()
            .jsonBaselineFile(
                getCopyIntoResultFileLocation(
                    "ITCopyIntoTransformationsMultipleInputsWithError.json", fileFormat))
            .ignoreColumnTypes()
            .go();
      }

      dropTable(targetTableName);
    }
  }

  static void testComplexWithRepeatingNames(
      BufferAllocator allocator,
      String source,
      OnErrorAction onErrorAction,
      FileFormat... fileFormats)
      throws Exception {
    String targetTableName = "tComplexWithRepeatingNames";
    List<Pair<String, String>> tableSchema =
        ImmutableList.of(
            Pair.of("id", "bigint"),
            Pair.of("city_with_street", "varchar"),
            Pair.of("number", "int"),
            Pair.of("address", "struct<city varchar, street varchar, number int, zip int>"),
            Pair.of("first_customer_name", "varchar"),
            Pair.of("first_customer_address_city", "varchar"));
    String[] mappings =
        new String[] {
          "first_customer_address_city",
          "first_customer_name",
          "address",
          "number",
          "city_with_street",
          "id"
        };
    String[] transformations =
        new String[] {
          "customers[0]['address']['city']",
          "customers[0]['name']",
          "address",
          "address['number']",
          "concat(address['city'], address['street'])",
          "id"
        };
    String[] inputFileNames = new String[] {"nested_with_repeating_names"};
    for (FileFormat fileFormat : fileFormats) {
      prepareInputAndRunCopyIntoTransformation(
          source,
          targetTableName,
          tableSchema,
          mappings,
          transformations,
          inputFileNames,
          fileFormat,
          onErrorAction);

      new TestBuilder(allocator)
          .sqlQuery("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTableName)
          .unOrdered()
          .jsonBaselineFile(
              getCopyIntoResultFileLocation(
                  "ITCopyIntoTransformationsComplexWithRepeatingNames.json", fileFormat))
          .ignoreColumnTypes()
          .go();

      dropTable(targetTableName);
    }
  }

  private static void prepareInputAndRunCopyIntoTransformation(
      String source,
      String targetTableName,
      List<Pair<String, String>> tableSchema,
      String[] mappings,
      String[] transformations,
      String[] inputFileNames,
      FileFormat fileFormat,
      OnErrorAction onErrorAction)
      throws Exception {
    File inputFilesLocation = createTempLocation();
    String[] inputFileNamesWithExtension =
        Arrays.stream(inputFileNames)
            .map(n -> n + "." + fileFormat.name().toLowerCase())
            .toArray(String[]::new);
    File[] newSourceFiles =
        createTableAndGenerateSourceFiles(
            targetTableName,
            tableSchema,
            inputFileNamesWithExtension,
            inputFilesLocation,
            fileFormat);

    runCopyIntoTransformation(
        source,
        inputFilesLocation,
        targetTableName,
        mappings,
        transformations,
        inputFileNamesWithExtension,
        fileFormat,
        onErrorAction);

    Arrays.stream(newSourceFiles).forEach(f -> assertThat(f.delete()).isTrue());
  }

  private static void runCopyIntoTransformation(
      String source,
      File inputFilesLocation,
      String tableName,
      String[] mappings,
      String[] transformations,
      String[] fileNames,
      FileFormat fileFormat,
      CopyIntoTableContext.OnErrorAction onErrorAction)
      throws Exception {
    String storageLocation = "'@" + source + "/" + inputFilesLocation.getName() + "'";
    StringBuilder copyIntoQuery =
        new StringBuilder("COPY INTO ").append(TEMP_SCHEMA).append(".").append(tableName);
    if (mappings != null) {
      copyIntoQuery
          .append(" ( ")
          .append(
              Arrays.stream(mappings)
                  .map(m -> String.format("\"%s\"", m))
                  .collect(Collectors.joining(", ")))
          .append(") ");
    }
    copyIntoQuery
        .append(" FROM ")
        .append(" (SELECT ")
        .append(String.join(", ", transformations))
        .append(" FROM ")
        .append(storageLocation)
        .append(" ) ")
        .append(" FILES( ")
        .append(serializeFileListForQuery(fileNames))
        .append(" ) ")
        .append(" FILE_FORMAT '")
        .append(fileFormat.name())
        .append("'");
    if (onErrorAction != null) {
      copyIntoQuery.append(" (ON_ERROR '").append(onErrorAction.name()).append("')");
    }
    test(copyIntoQuery.toString());
  }
}
