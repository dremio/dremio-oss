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

import static org.junit.Assert.assertThrows;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;

import com.dremio.TestBuilder;
import com.dremio.TestResult;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.SystemIcebergViewMetadataFactory;
import com.google.common.collect.ImmutableList;

public class CopyIntoErrorsTests extends ITCopyIntoBase {

  public static final String SYS_NAMESPACE = "sys";
  public static final String COPY_JOB_HISTORY_TABLE_NAME = SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME;
  public static final String COPY_FILE_HISTORY_TABLE_NAME = SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
  public static final String COPY_ERRORS_HISTORY_TABLE_NAME = SystemIcebergViewMetadataFactory.COPY_ERRORS_HISTORY_VIEW_NAME;

  public static void testNoError(BufferAllocator allocator, String source) throws Exception {
    String tableName = "noError";
    String selectTemplate = "SELECT * FROM %s.%s";
    String selectQuery = String.format(selectTemplate, TEMP_SCHEMA, tableName);
    List<Pair<String, String>> colNameTypePairs = ImmutableList.of(
      Pair.of("decimalCol", "decimal(6,3)"),
      Pair.of("intCol", "int"),
      Pair.of("floatCol", "float"),
      Pair.of("doubleCol", "double"),
      Pair.of("varcharCol", "varchar"),
      Pair.of("booleanCol", "boolean")
    );
    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      String inputFileName = "noError." + fileFormat.name().toLowerCase();
      File inputFilesLocation = createTempLocation();
      File newSourceFile = createTableAndGenerateSourceFile(tableName, colNameTypePairs, inputFileName, inputFilesLocation, fileFormat);

      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);

      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList()).toArray(new String[colNameTypePairs.size()]))
        .baselineValues(BigDecimal.valueOf(123.456), 42, 3.14f, 2.71828, "Hello, World!", true)
        .baselineValues(BigDecimal.valueOf(789.123), 17, 2.718f, 3.14159265, "FooBar", false)
        .baselineValues(BigDecimal.valueOf(456.789), 73, 1.234f, 2.718281828459045, "JSON Example", true)
        .baselineValues(BigDecimal.valueOf(987.654), 99, 0.001f, 1.23456789, "Random Data", false)
        .go();

      dropTable(tableName);
      Assertions.assertTrue(newSourceFile.delete());
    }

    new TestBuilder(allocator)
      .sqlQuery(String.format(selectTemplate, SYS_NAMESPACE, COPY_ERRORS_HISTORY_TABLE_NAME))
      .unOrdered()
      .expectsEmptyResultSet();
  }

  public static void testFeatureFlag(String source) throws Exception {
    setSystemOption(ExecConstants.ENABLE_COPY_INTO_CONTINUE, "false");
    String tableName = "featureFlag";
    File inputFilesLocation = createTempLocation();
    String inputFileName = "typeError.json";
    File newSourceFile = createTableAndGenerateSourceFile(tableName, ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int")),
      inputFileName, inputFilesLocation, FileFormat.JSON);
    try {
      assertThrows("Copy Into with continue option is not supported", UserException.class, () ->
        runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName,
          CopyIntoTableContext.OnErrorAction.CONTINUE));
    } finally {
      Assertions.assertTrue(newSourceFile.delete());
    }
  }

  public static void testTypeError(BufferAllocator allocator, String source) throws Exception {
    String tableName = "typeError";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);

    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      String inputFileName = "typeError." + fileFormat.name().toLowerCase();
      File inputFilesLocation = createTempLocation();
      File newSourceFile = createTableAndGenerateSourceFile(tableName, colNameTypePairs, inputFileName, inputFilesLocation, fileFormat);

      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);

      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
          .toArray(new String[colNameTypePairs.size()]))
        .baselineValues("Ben", 28)
        .baselineValues("Bob", 25)
        .baselineValues("George", 30)
        .go();

      dropTable(tableName);

      Assertions.assertTrue(newSourceFile.delete());
    }
  }

  public static void testSyntaxError(BufferAllocator allocator, String source) throws Exception {
    String tableName = "syntaxError";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("id", "int"), Pair.of("customerName", "varchar"),
      Pair.of("address", "varchar"));
    String[] inputFileNames = new String[]{"syntaxError.json", "syntaxError1.json", "syntaxError2.json"};
    File inputFilesLocation = createTempLocation();
    File[] newSourceFiles = createTableAndGenerateSourceFiles(tableName, colNameTypePairs, inputFileNames,
      inputFilesLocation, FileFormat.JSON);
    runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames, CopyIntoTableContext.OnErrorAction.CONTINUE);

    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);
    new TestBuilder(allocator)
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns(colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
        .toArray(new String[colNameTypePairs.size()]))
      .baselineValues(1, "GoodCustomer", "11111, New York, 2nd Street 876")
      .baselineValues(3, "AnotherCustomer", "789710, Boston, Main Road 123")
      .go();

    dropTable(tableName);

    for (File file : newSourceFiles) {
      Assertions.assertTrue(file.delete());
    }
  }

  public static void testMultipleInputFiles(BufferAllocator allocator, String source) throws Exception {
    String tableName = "multiFiles";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    String[] columns = colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
      .toArray(new String[colNameTypePairs.size()]);
    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);
    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      final String[] inputFileNames = new String[]{"typeError", "typeError1", "typeError2", "typeError3"};
      IntStream.range(0, inputFileNames.length).forEach(i -> inputFileNames[i] += "." + fileFormat.name().toLowerCase());
      File inputFilesLocation = createTempLocation();
      File[] newSourceFiles = createTableAndGenerateSourceFiles(tableName, colNameTypePairs, inputFileNames, inputFilesLocation, fileFormat);

      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames,
        CopyIntoTableContext.OnErrorAction.CONTINUE);


      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues("Ben", 28)
        .baselineValues("Bob", 25)
        .baselineValues("George", 30)
        .baselineValues("Sarah", 30)
        .baselineValues("Jim", 40)
        .go();

      for (File file : newSourceFiles) {
        Assertions.assertTrue(file.delete());
      }

      String[] inputFileNames2 = new String[]{"typeError3", "typeError4"};
      IntStream.range(0, inputFileNames2.length).forEach(i -> inputFileNames2[i] += "." + fileFormat.name().toLowerCase());
      newSourceFiles = createCopyIntoSourceFiles(inputFileNames2, inputFilesLocation, fileFormat);
      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames2, CopyIntoTableContext.OnErrorAction.CONTINUE);
      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues("Ben", 28)
        .baselineValues("Bob", 25)
        .baselineValues("George", 30)
        .baselineValues("Sarah", 30)
        .baselineValues("Jim", 40)
        .baselineValues("Jim", 40)
        .baselineValues("Max", 33)
        .baselineValues("Andrew", 40)
        .baselineValues("Pam", 32)
        .go();

      dropTable(tableName);

      for (File file : newSourceFiles) {
        Assertions.assertTrue(file.delete());
      }
    }
  }

  public static void testDifferentFileFormats(BufferAllocator allocator, String source) throws Exception {
    String tableName = "multiFormats";

    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    String[] columns = colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
      .toArray(new String[colNameTypePairs.size()]);

    String inputFileName = "typeError.json";
    File inputFilesLocation = createTempLocation();
    File newSourceFile = createTableAndGenerateSourceFile(tableName, colNameTypePairs, inputFileName,
      inputFilesLocation, FileFormat.JSON);
    runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);

    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);
    new TestBuilder(allocator)
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns(columns)
      .baselineValues("Ben", 28)
      .baselineValues("Bob", 25)
      .baselineValues("George", 30)
      .go();

    Assertions.assertTrue(newSourceFile.delete());

    inputFileName = "typeError1.csv";
    newSourceFile = createCopyIntoSourceFile(inputFileName, inputFilesLocation, FileFormat.CSV);
    runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);

    new TestBuilder(allocator)
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns(columns)
      .baselineValues("Ben", 28)
      .baselineValues("Bob", 25)
      .baselineValues("George", 30)
      .baselineValues("Sarah", 30)
      .go();

    Assertions.assertTrue(newSourceFile.delete());

    inputFileName = "typeError2.json";
    newSourceFile = createCopyIntoSourceFile(inputFileName, inputFilesLocation, FileFormat.JSON);
    runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);

    new TestBuilder(allocator)
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns(columns)
      .baselineValues("Ben", 28)
      .baselineValues("Bob", 25)
      .baselineValues("George", 30)
      .baselineValues("Sarah", 30)
      .go();

    Assertions.assertTrue(newSourceFile.delete());


    inputFileName = "typeError3.csv";
    newSourceFile = createCopyIntoSourceFile(inputFileName, inputFilesLocation, FileFormat.CSV);
    runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileName, CopyIntoTableContext.OnErrorAction.CONTINUE);
    new TestBuilder(allocator)
      .sqlQuery(selectQuery)
      .unOrdered()
      .baselineColumns(columns)
      .baselineValues("Ben", 28)
      .baselineValues("Bob", 25)
      .baselineValues("George", 30)
      .baselineValues("Sarah", 30)
      .baselineValues("Jim", 40)
      .go();

    dropTable(tableName);

    Assertions.assertTrue(newSourceFile.delete());
  }

  public static void testCopyIntoErrorOutput(BufferAllocator allocator, String source) throws Exception {
    File location = createTempLocation();
    String tableName = "output";
    List<Pair<String, String>> columnNameTypeList = ImmutableList.of(
      Pair.of("name", "varchar"),
      Pair.of("age", "int")
    );
    String fileName = "typeError.csv";
    File newSourceFile = createTableAndGenerateSourceFile(tableName, columnNameTypeList, fileName, location, FileFormat.CSV);
    String storageLocation = "'@" + source + "/" + location.getName() + "'";
    String copyIntoQuery = String.format("COPY INTO %s.%s FROM %s regex '%s' (RECORD_DELIMITER '\n', ON_ERROR 'continue')", TEMP_SCHEMA, tableName, storageLocation, fileName);
    String[] copyIntoErrorOutputColNames = new String[]{"`Records`", "`RejectedRecords`"};

    new TestBuilder(allocator)
      .sqlQuery(copyIntoQuery)
      .unOrdered()
      .baselineColumns(copyIntoErrorOutputColNames)
      .baselineValues(3L, 4L)
      .go();

    Assert.assertTrue(newSourceFile.delete());

    fileName = "typeError.json";
    newSourceFile = createCopyIntoSourceFile(fileName, location, FileFormat.JSON);
    copyIntoQuery = String.format("COPY INTO %s.%s FROM %s regex '%s' (RECORD_DELIMITER '\n', ON_ERROR 'continue')", TEMP_SCHEMA, tableName, storageLocation, fileName);

    new TestBuilder(allocator)
      .sqlQuery(copyIntoQuery)
      .unOrdered()
      .baselineColumns(copyIntoErrorOutputColNames)
      .baselineValues(3L, 1L)
      .go();

    Assert.assertTrue(newSourceFile.delete());

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName);
    test(dropQuery);
  }

  public static void testPartialSchema(BufferAllocator allocator, String source) throws Exception {
    String tableName = "partialSchema";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);

    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      String[] inputFileNames = new String[]{"typeError5." + fileFormat.name().toLowerCase(),
        "typeError6." + fileFormat.name().toLowerCase(), "typeError7." + fileFormat.name().toLowerCase()};
      File inputFilesLocation = createTempLocation();
      File[] newSourceFiles = createTableAndGenerateSourceFiles(tableName, colNameTypePairs, inputFileNames, inputFilesLocation, fileFormat);

      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames, CopyIntoTableContext.OnErrorAction.CONTINUE);

      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
          .toArray(new String[colNameTypePairs.size()]))
        .baselineValues("Jane", 28)
        .baselineValues("Sam", 30)
        .baselineValues("Angela", 40)
        .go();

      dropTable(tableName);
      for (File file : newSourceFiles) {
        Assertions.assertTrue(file.delete());
      }
    }
  }

  public static void testCSVExtractHeaderAndSkipLines(BufferAllocator allocator, String source) throws Exception {
    String tableName = "cars";
    String fileName = "cars_noheader.csv";
    File location = createTempLocation();
    createTableAndGenerateSourceFile(tableName,
      ImmutableList.of(Pair.of("make_year", "INT"), Pair.of("make", "VARCHAR"),
        Pair.of("model", "VARCHAR"), Pair.of("description", "VARCHAR"),
        Pair.of("price", "DOUBLE")),
      fileName, location, FileFormat.CSV);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery = String.format(
      "COPY INTO %s.%s FROM %s FILES (\'%s\') (RECORD_DELIMITER '\n', EXTRACT_HEADER 'true', SKIP_LINES 2, ON_ERROR 'continue')",
      TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    TestResult testResult = new TestBuilder(allocator)
      .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, tableName)
      .unOrdered()
      .baselineColumns("make_year", "make", "model", "description", "price")
      .expectsEmptyResultSet()
      .go();

    test(String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName));
  }

  public static void testCSVSkipLines(BufferAllocator allocator, String source) throws Exception {
    String tableName = "cars";
    String fileName = "cars_noheader.csv";
    File location = createTempLocation();
    createTableAndGenerateSourceFile(tableName,
      ImmutableList.of(Pair.of("make_year", "INT"), Pair.of("make", "VARCHAR"),
        Pair.of("model", "VARCHAR"), Pair.of("description", "VARCHAR"),
        Pair.of("price", "DOUBLE")),
      fileName, location, FileFormat.CSV);

    String storageLocation = "\'@" + source + "/" + location.getName() + "\'";
    final String copyIntoQuery = String.format(
      "COPY INTO %s.%s FROM %s FILES (\'%s\') (RECORD_DELIMITER '\n', EXTRACT_HEADER 'false', SKIP_LINES 2, ON_ERROR 'continue')",
      TEMP_SCHEMA, tableName, storageLocation, fileName);
    test(copyIntoQuery);

    TestResult testResult = new TestBuilder(allocator)
      .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, tableName)
      .unOrdered()
      .baselineColumns("make_year", "make", "model", "description", "price")
      .baselineValues(1999, "Chevy", "Venture \"Extended Edition, Very Large\"", null, 5000.00)
      .baselineValues(1996, "Jeep", "Grand Cherokee", "MUST SELL! air, moon roof, loaded", 4799.00)
      .go();

    test(String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName));
  }

  public static void testOnIdentityPartitionedTable(BufferAllocator allocator, String source) throws Exception {
    String tableName = "identityPartitionedTable";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    ImmutableList<Triple<String, String, String>> partitionDef = ImmutableList.of(Triple.of("identity", "name", null));
    String[] columns = colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
      .toArray(new String[colNameTypePairs.size()]);
    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);
    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      final String[] inputFileNames = new String[]{"typeError", "typeError1", "typeError2", "typeError3"};
      IntStream.range(0, inputFileNames.length).forEach(i -> inputFileNames[i] += "." + fileFormat.name().toLowerCase());
      File inputFilesLocation = createTempLocation();
      File[] newSourceFiles = createTableAndGenerateSourceFiles(tableName, colNameTypePairs, partitionDef, inputFileNames, inputFilesLocation, fileFormat);
      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames,
        CopyIntoTableContext.OnErrorAction.CONTINUE);

      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues("Ben", 28)
        .baselineValues("Bob", 25)
        .baselineValues("George", 30)
        .baselineValues("Sarah", 30)
        .baselineValues("Jim", 40)
        .go();

      for (File file : newSourceFiles) {
        Assertions.assertTrue(file.delete());
      }

      dropTable(tableName);
    }
  }

  public static void testOnMultiPartitionedTable(BufferAllocator allocator, String source) throws Exception {
    String tableName = "multiPartitionedTable";
    ImmutableList<Pair<String, String>> colNameTypePairs = ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));
    ImmutableList<Triple<String, String, String>> partitionDef = ImmutableList.of(Triple.of("truncate", "name", "1"), Triple.of("bucket", "age", "2"));
    String[] columns = colNameTypePairs.stream().map(Pair::getLeft).collect(Collectors.toList())
      .toArray(new String[colNameTypePairs.size()]);
    String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, tableName);
    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      final String[] inputFileNames = new String[]{"typeError", "typeError1", "typeError2", "typeError3"};
      IntStream.range(0, inputFileNames.length).forEach(i -> inputFileNames[i] += "." + fileFormat.name().toLowerCase());
      File inputFilesLocation = createTempLocation();
      File[] newSourceFiles = createTableAndGenerateSourceFiles(tableName, colNameTypePairs, partitionDef, inputFileNames, inputFilesLocation, fileFormat);
      runCopyIntoOnError(source, inputFilesLocation, tableName, inputFileNames,
        CopyIntoTableContext.OnErrorAction.CONTINUE);

      new TestBuilder(allocator)
        .sqlQuery(selectQuery)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues("Ben", 28)
        .baselineValues("Bob", 25)
        .baselineValues("George", 30)
        .baselineValues("Sarah", 30)
        .baselineValues("Jim", 40)
        .go();

      for (File file : newSourceFiles) {
        Assertions.assertTrue(file.delete());
      }

      dropTable(tableName);
    }

  }
}
