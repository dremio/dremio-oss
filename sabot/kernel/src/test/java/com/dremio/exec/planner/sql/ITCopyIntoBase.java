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

import com.dremio.common.util.FileUtils;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.text.RandomStringGenerator;

public abstract class ITCopyIntoBase extends ITDmlQueryBase {

  protected static final String JSON_SOURCE_FOLDER_COPY_INTO = "/store/json/copyintosource/";
  protected static final String CSV_SOURCE_FOLDER_COPY_INTO = "/store/text/copyintosource/";
  protected static final String PARQUET_SOURCE_FOLDER_COPY_INTO = "/store/parquet/copyintosource/";

  public static File createTempLocation() {
    Random random = new Random(System.currentTimeMillis());
    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder()
            .usingRandom(random::nextInt)
            .withinRange('0', '9')
            .build();
    String locationName = randomStringGenerator.generate(8);
    File location = new File(getDfsTestTmpSchemaLocation(), locationName);
    location.mkdirs();
    return location;
  }

  protected static File[] createTableAndGenerateSourceFiles(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      String[] inputFileNames,
      File destLocation,
      FileFormat inputFileFormat)
      throws Exception {
    createTable(tableName, colNameTypePairs);
    return createCopyIntoSourceFiles(inputFileNames, destLocation, inputFileFormat);
  }

  protected static File[] createTableAndGenerateSourceFiles(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      List<Triple<String, String, String>> partitionDef,
      String[] inputFileNames,
      File destLocation,
      FileFormat inputFileFormat)
      throws Exception {
    createTable(tableName, colNameTypePairs, partitionDef);
    return createCopyIntoSourceFiles(inputFileNames, destLocation, inputFileFormat);
  }

  protected static File createTableAndGenerateSourceFile(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      String inputFilName,
      File destLocation,
      FileFormat fileFormat)
      throws Exception {
    return createTableAndGenerateSourceFiles(
        tableName, colNameTypePairs, new String[] {inputFilName}, destLocation, fileFormat)[0];
  }

  public static void createTable(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      List<Triple<String, String, String>> partitionDef)
      throws Exception {
    String schema =
        colNameTypePairs.stream()
            .map(p -> String.format("\"%s\" %s", p.getLeft(), p.getRight()))
            .collect(Collectors.joining(","));
    String createQuery =
        String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s)", TEMP_SCHEMA, tableName, schema);
    if (partitionDef != null && !partitionDef.isEmpty()) {
      createQuery +=
          " PARTITION BY ("
              + partitionDef.stream()
                  .map(
                      p ->
                          p.getRight() == null
                              ? String.format("%s(%s)", p.getLeft(), p.getMiddle())
                              : String.format(
                                  "%s(%s,%s)", p.getLeft(), p.getRight(), p.getMiddle()))
                  .collect(Collectors.joining(", "))
              + ")";
    }
    test(createQuery);
  }

  protected static void createTable(String tableName, List<Pair<String, String>> colNameTypePairs)
      throws Exception {
    createTable(tableName, colNameTypePairs, null);
  }

  protected static File createCopyIntoSourceFile(
      String inputFileName, File destLocation, FileFormat inputFileFormat) throws IOException {
    return createCopyIntoSourceFiles(new String[] {inputFileName}, destLocation, inputFileFormat)[
        0];
  }

  protected static File[] createCopyIntoSourceFiles(
      String[] inputFileNames, File destLocation, FileFormat inputFileFormat) throws IOException {
    String sourceLocation;
    switch (inputFileFormat) {
      case CSV:
        sourceLocation = CSV_SOURCE_FOLDER_COPY_INTO;
        break;
      case JSON:
        sourceLocation = JSON_SOURCE_FOLDER_COPY_INTO;
        break;
      case PARQUET:
        sourceLocation = PARQUET_SOURCE_FOLDER_COPY_INTO;
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized file format.");
    }

    File[] files = new File[inputFileNames.length];
    for (int i = 0; i < inputFileNames.length; i++) {
      File newSourceFile = new File(destLocation.toString(), inputFileNames[i]);
      File oldSourceFile = FileUtils.getResourceAsFile(sourceLocation + inputFileNames[i]);
      Files.copy(oldSourceFile, newSourceFile);
      files[i] = newSourceFile;
    }
    return files;
  }

  protected static void dropTable(String tableName) throws Exception {
    test(String.format("DROP TABLE %s.%s", TEMP_SCHEMA, tableName));
  }

  protected static void runCopyIntoOnError(
      String source,
      File inputFileLocation,
      String tableName,
      FileFormat fileFormat,
      String inputFileName,
      CopyIntoTableContext.OnErrorAction onErrorAction)
      throws Exception {
    runCopyIntoOnError(
        source,
        inputFileLocation,
        tableName,
        fileFormat,
        new String[] {inputFileName},
        onErrorAction);
  }

  protected static void runCopyIntoOnError(
      String source,
      File inputFilesLocation,
      String tableName,
      FileFormat fileFormat,
      String[] fileNames,
      CopyIntoTableContext.OnErrorAction onErrorAction)
      throws Exception {
    String storageLocation = "'@" + source + "/" + inputFilesLocation.getName() + "'";
    StringBuilder copyIntoQuery =
        new StringBuilder("COPY INTO ")
            .append(TEMP_SCHEMA)
            .append(".")
            .append(tableName)
            .append(" FROM ")
            .append(storageLocation)
            .append(" FILES(");
    copyIntoQuery.append(serializeFileListForQuery(fileNames));
    copyIntoQuery.append(")");
    copyIntoQuery.append(" FILE_FORMAT '").append(fileFormat.name()).append("'");
    copyIntoQuery.append(" (").append("ON_ERROR '").append(onErrorAction.name()).append("')");
    test(copyIntoQuery.toString());
  }

  protected static String serializeFileListForQuery(String[] fileNames) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fileNames.length; i++) {
      sb.append("'").append(fileNames[i]).append("'");
      if (i < fileNames.length - 1) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  protected static File[] createTableWithSortOrderAndGenerateSourceFiles(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      String[] inputFileNames,
      File destLocation,
      FileFormat inputFileFormat,
      List<String> sortColumns)
      throws Exception {
    createTableWithSortOrder(tableName, colNameTypePairs, sortColumns);

    return createCopyIntoSourceFiles(inputFileNames, destLocation, inputFileFormat);
  }

  public static File createTableWithSortOrderAndGenerateSourceFile(
      String tableName,
      List<Pair<String, String>> colNameTypePairs,
      String inputFileName,
      File destLocation,
      FileFormat fileFormat,
      List<String> sortColumns)
      throws Exception {

    return createTableWithSortOrderAndGenerateSourceFiles(
        tableName,
        colNameTypePairs,
        new String[] {inputFileName},
        destLocation,
        fileFormat,
        sortColumns)[0];
  }

  protected static void createTableWithSortOrder(
      String tableName, List<Pair<String, String>> colNameTypePairs, List<String> sortColumns)
      throws Exception {
    String sortColumnsString = String.join(", ", sortColumns);
    String schema =
        colNameTypePairs.stream()
            .map(p -> String.format("%s %s", p.getLeft(), p.getRight()))
            .collect(Collectors.joining(","));
    String createQuery =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableName, schema, sortColumnsString);
    test(createQuery);
  }

  public enum FileFormat {
    CSV,
    JSON,
    PARQUET
  }
}
