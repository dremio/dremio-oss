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

import static com.dremio.exec.planner.sql.CopyIntoErrorsTests.COPY_FILE_HISTORY_TABLE_NAME;
import static com.dremio.exec.planner.sql.CopyIntoErrorsTests.COPY_JOB_HISTORY_TABLE_NAME;
import static com.dremio.exec.planner.sql.CopyIntoErrorsTests.SYS_NAMESPACE;
import static com.dremio.exec.tablefunctions.copyerrors.CopyErrorsPrule.COPY_INTO_JOB_NOT_FOUND_EXCEPTION;
import static com.dremio.exec.tablefunctions.copyerrors.CopyErrorsTranslatableTable.COPY_ERRORS_TABLEFUNCTION_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.Describer;
import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.planner.CopyIntoTablePlanBuilderBase;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.io.file.Path;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CopyErrorsTests extends ITCopyIntoBase {

  private static final String MOCK_COPY_JOB_HISTORY_TABLE_NAME =
      TEMP_SCHEMA + "." + COPY_JOB_HISTORY_TABLE_NAME;
  private static final String MOCK_COPY_FILE_HISTORY_TABLE_NAME =
      TEMP_SCHEMA + "." + COPY_FILE_HISTORY_TABLE_NAME;
  private static final String ORIGINAL_COPY_JOB_HISTORY_TABLE_NAME =
      SYS_NAMESPACE + "." + COPY_JOB_HISTORY_TABLE_NAME;
  private static final String ORIGINAL_COPY_FILE_HISTORY_TABLE_NAME =
      SYS_NAMESPACE + "." + COPY_FILE_HISTORY_TABLE_NAME;
  private static final long COPY_JOB_HISTORY_SCHEMA_VERSION = 1L;
  private static final long COPY_FILE_HISTORY_TABLE_SCHEMA_VERSION = 1L;
  private static final String[] COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES =
      COPY_ERRORS_TABLEFUNCTION_SCHEMA.stream().map(Triple::getLeft).toArray(String[]::new);

  private static BufferAllocator fallbackAllocator;

  @BeforeClass
  public static void setupTables() throws Exception {
    fallbackAllocator =
        getSabotContext()
            .getAllocator()
            .newChildAllocator(CopyErrorsTests.class.getName() + "-fallback", 0, Long.MAX_VALUE);

    createTable(
        COPY_JOB_HISTORY_TABLE_NAME,
        getColNameTypePairs(
            CopyJobHistoryTableSchemaProvider.getSchema(COPY_JOB_HISTORY_SCHEMA_VERSION)));
    createTable(
        COPY_FILE_HISTORY_TABLE_NAME,
        getColNameTypePairs(
            CopyFileHistoryTableSchemaProvider.getSchema(COPY_FILE_HISTORY_TABLE_SCHEMA_VERSION)));
    // We're working with empty target tables and fake system table content in these tests
  }

  private static List<Pair<String, String>> getColNameTypePairs(Schema schema) {
    BatchSchema batchSchema = SchemaConverter.getBuilder().build().fromIceberg(schema);
    return StreamSupport.stream(batchSchema.spliterator(), false)
        .map(
            f -> {
              String typeName = Describer.describe(f, false);
              if ("int64".equals(typeName)) {
                typeName = "bigint";
              }
              return Pair.of(f.getName(), typeName);
            })
        .collect(Collectors.toList());
  }

  private static void addJobAndFileHistoryEntry(
      String jobId,
      String tableName,
      String storageLocation,
      List<String> filesWithRejection,
      Map<CopyIntoTableContext.FormatOption, Object> formatOptions,
      String format)
      throws Exception {
    String insertQuery =
        String.format(
            "INSERT INTO  %s.%s VALUES"
                + "( now(), '%s', '%s.%s', null, null, '%s', 'anonymous', null, '%s.%s', '%s')",
            TEMP_SCHEMA,
            COPY_JOB_HISTORY_TABLE_NAME,
            jobId,
            TEMP_SCHEMA,
            tableName,
            CopyIntoFileLoadInfo.Util.getJson(formatOptions),
            TEMP_SCHEMA_HADOOP,
            storageLocation,
            format);

    test(insertQuery);

    insertQuery =
        String.format(
            "INSERT INTO %s.%s VALUES %s",
            TEMP_SCHEMA,
            COPY_FILE_HISTORY_TABLE_NAME,
            filesWithRejection.stream()
                .map(s -> String.format("( now(), '%s', '%s', 'PARTIALLY_LOADED')", jobId, s))
                .collect(Collectors.joining(", ")));
    test(insertQuery);
  }

  @AfterClass
  public static void dropCopyErrorsTable() throws Exception {
    dropTable(COPY_JOB_HISTORY_TABLE_NAME);
    dropTable(COPY_FILE_HISTORY_TABLE_NAME);

    fallbackAllocator.close();
  }

  public static List<RecordBatchHolder> handleCopyErrorsTest(String query) throws Exception {
    query =
        query
            .replaceAll(ORIGINAL_COPY_JOB_HISTORY_TABLE_NAME, MOCK_COPY_JOB_HISTORY_TABLE_NAME)
            .replaceAll(ORIGINAL_COPY_FILE_HISTORY_TABLE_NAME, MOCK_COPY_FILE_HISTORY_TABLE_NAME);
    List<QueryDataBatch> result = testSqlWithResults(query);
    BufferAllocator allocator =
        result.get(0).getData() != null
            ? result.get(0).getData().getReferenceManager().getAllocator()
            : fallbackAllocator;
    try (RecordBatchLoader loader = new RecordBatchLoader(allocator)) {
      loader.load(result.get(0).getHeader().getDef(), result.get(0).getData());
      return ImmutableList.of(
          RecordBatchHolder.newRecordBatchHolder(
              new RecordBatchData(loader, allocator), 0, loader.getRecordCount()));
    }
  }

  @Test
  public void testCopyErrorsTableFunction() throws Exception {

    String targetTableName = "multiFiles";
    ImmutableList<Pair<String, String>> colNameTypePairs =
        ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));

    final String[] inputFileNamesNoExtension =
        new String[] {"typeError", "typeError1", "no_column_match", "typeError2"};

    for (FileFormat fileFormat : ImmutableList.of(FileFormat.CSV, FileFormat.JSON)) {
      String[] inputFileNames =
          Arrays.stream(inputFileNamesNoExtension)
              .map(f -> f + "." + fileFormat.name().toLowerCase())
              .toArray(String[]::new);
      File inputFilesLocation = createTempLocation();
      File[] inputFiles =
          createTableAndGenerateSourceFiles(
              targetTableName, colNameTypePairs, inputFileNames, inputFilesLocation, fileFormat);

      List<String> rejectedFiles =
          Arrays.stream(inputFiles).map(File::getAbsolutePath).collect(Collectors.toList());

      String jobId = "1b06239f-c613-dee4-d9b6-5f79e00cbe0" + fileFormat.name().length();
      addJobAndFileHistoryEntry(
          jobId,
          targetTableName,
          inputFilesLocation.getName(),
          rejectedFiles.subList(2, 3),
          new HashMap<>(),
          fileFormat.name());

      String jobId2 = "99999999-c613-dee4-d9b6-5f79e00cbe0" + fileFormat.name().length();
      addJobAndFileHistoryEntry(
          jobId2,
          targetTableName,
          inputFilesLocation.getName(),
          rejectedFiles.subList(0, 3),
          new HashMap<>(),
          fileFormat.name());

      // Only targetTableName is specified, so jobId2 should be picked for being the most recent
      // Testing for type coercion errors + header/no column match errors
      if (FileFormat.CSV.equals(fileFormat)) {
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(targetTableName, null))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId2,
                rejectedFiles.get(0),
                3L,
                2L,
                "age",
                "While processing field \"age\". Could not convert \"NaN\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(0),
                5L,
                4L,
                "age",
                "While processing field \"age\". Could not convert \"aaa\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(0),
                7L,
                6L,
                "age",
                "While processing field \"age\". Could not convert \"Barbie\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(0),
                8L,
                7L,
                "age",
                "While processing field \"age\". Could not convert \"young\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(1),
                3L,
                2L,
                "age",
                "While processing field \"age\". Could not convert \"false\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(2),
                1L,
                0L,
                null,
                "No column name matches target schema(name::varchar, age::int32) in file "
                    + rejectedFiles.get(2))
            .go();
      } else {
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(targetTableName, null))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId2,
                rejectedFiles.get(0),
                15L,
                4L,
                "age",
                "While processing field \"age\". Could not convert \"NaN\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(1),
                7L,
                2L,
                "age",
                "While processing field \"age\". Could not convert \"false\" to INT.")
            .baselineValues(
                jobId2,
                rejectedFiles.get(2),
                4L,
                1L,
                null,
                "No column name matches target schema(name::varchar, age::int32)")
            .go();
      }

      if (FileFormat.JSON.equals(fileFormat)) {
        File[] syntaxErrorinputFiles =
            createTableAndGenerateSourceFiles(
                targetTableName,
                colNameTypePairs,
                new String[] {"syntaxError3.json"},
                inputFilesLocation,
                fileFormat);
        List<String> syntaxErrorFiles =
            Arrays.stream(syntaxErrorinputFiles)
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());

        String jobId3 = "12345678-c613-dee4-d9b6-5f79e00cbe0" + fileFormat.name().length();
        addJobAndFileHistoryEntry(
            jobId3,
            targetTableName,
            inputFilesLocation.getName(),
            syntaxErrorFiles,
            new HashMap<>(),
            fileFormat.name());
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(targetTableName, jobId3))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId3,
                syntaxErrorFiles.get(0),
                3L,
                1L,
                null,
                "Unexpected character ('\"' (code 34)): was expecting comma to separate Object entries")
            .go();
      }

      // CSV or Non-fileformat specific tests
      if (FileFormat.CSV.equals(fileFormat)) {
        // A (correct) JobId specified
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(targetTableName, jobId))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId,
                rejectedFiles.get(2),
                1L,
                0L,
                null,
                "No column name matches target schema(name::varchar, age::int32) in file "
                    + rejectedFiles.get(2))
            .go();

        // Incorrect JobId specified
        Exception e =
            assertThrows(
                RpcException.class,
                () ->
                    testBuilder()
                        .sqlQuery(
                            getCopyErrorsQuery(
                                targetTableName, "00000000-0000-0000-0000-000000000000"))
                        .unOrdered()
                        .baselineColumns("dummy")
                        .baselineValues("dummy")
                        .go());
        assertTrue(
            "Expected exception message doesn't match.",
            e.getMessage().contains(COPY_INTO_JOB_NOT_FOUND_EXCEPTION.getMessage()));

        // No header CSV
        String jobId3 = "12345678-c613-dee4-d9b6-5f79e00cbe0" + fileFormat.name().length();
        addJobAndFileHistoryEntry(
            jobId3,
            targetTableName,
            inputFilesLocation.getName(),
            rejectedFiles.subList(3, 4),
            ImmutableMap.of(CopyIntoTableContext.FormatOption.EXTRACT_HEADER, false),
            fileFormat.name());
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(targetTableName, jobId3))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId3,
                rejectedFiles.get(3),
                1L,
                0L,
                "age",
                "While processing field \"age\". Could not convert \"age\" to INT.")
            .baselineValues(
                jobId3,
                rejectedFiles.get(3),
                2L,
                1L,
                "age",
                "While processing field \"age\". Could not convert \"abc\" to INT.")
            .go();

        // Unmatched quote symbol with EOF
        String quoteTestTable = "unmatchedquotetest";
        File[] quoteTestInputFiles =
            createTableAndGenerateSourceFiles(
                quoteTestTable,
                ImmutableList.of(Pair.of("num", "int"), Pair.of("txt", "varchar")),
                new String[] {"unmatchedQuote.csv"},
                inputFilesLocation,
                fileFormat);
        List<String> quoteRejectedFiles =
            Arrays.stream(quoteTestInputFiles)
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
        String jobId4 = "12345678-4613-dee4-d9b6-5f79e00cbe05";
        addJobAndFileHistoryEntry(
            jobId4,
            quoteTestTable,
            inputFilesLocation.getName(),
            quoteRejectedFiles,
            new HashMap<>(),
            fileFormat.name());
        testBuilder()
            .sqlQuery(getCopyErrorsQuery(quoteTestTable, jobId4))
            .unOrdered()
            .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
            .baselineValues(
                jobId4,
                quoteRejectedFiles.get(0),
                3L,
                2L,
                "num",
                "While processing field \"num\". Could not convert \"two\" to INT.")
            .baselineValues(
                jobId4,
                quoteRejectedFiles.get(0),
                5L,
                4L,
                "txt",
                "Malformed CSV file: expected closing quote symbol for a quoted value, started in line 5, but encountered EOF in line 23.")
            .go();

        // Unmatched quote symbol with size limit (re-doing same test query with low field size
        // limit)
        try (AutoCloseable autoCloseable =
            setSystemOptionWithAutoReset("limits.single_field_size_bytes", "300")) {
          testBuilder()
              .sqlQuery(getCopyErrorsQuery(quoteTestTable, jobId4))
              .unOrdered()
              .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
              .baselineValues(
                  jobId4,
                  quoteRejectedFiles.get(0),
                  3L,
                  2L,
                  "num",
                  "While processing field \"num\". Could not convert \"two\" to INT.")
              .baselineValues(
                  jobId4,
                  quoteRejectedFiles.get(0),
                  5L,
                  4L,
                  "txt",
                  "Malformed CSV file: expected closing quote symbol for a quoted value, started in line 5, but encountered a size limit exception in line 14. No further lines will be processed from this file. Field with index 1 exceeds the size limit of 300 bytes, actual size is 301 bytes.")
              .go();
        }

        dropTable(quoteTestTable);
      }
    }
    dropTable(targetTableName);
  }

  @Test
  public void testComplexJsonStruct() throws Exception {
    String targetTableName = "complex_struct";
    ImmutableList<Pair<String, String>> colNameTypePairs =
        ImmutableList.of(
            Pair.of("col1", "array< array<struct<c1: int, c2: int, c3: varchar>>>"),
            Pair.of("col3", "varchar"));

    String[] inputFileNames = new String[] {"source4_nestedtypeerror.json"};
    File inputFilesLocation = createTempLocation();
    File[] inputFiles =
        createTableAndGenerateSourceFiles(
            targetTableName, colNameTypePairs, inputFileNames, inputFilesLocation, FileFormat.JSON);
    List<String> rejectedFiles =
        Arrays.stream(inputFiles).map(File::getAbsolutePath).collect(Collectors.toList());

    String jobId = "1b06239f-c613-dee4-d9b6-5f79e00cbe09";
    addJobAndFileHistoryEntry(
        jobId,
        targetTableName,
        inputFilesLocation.getName(),
        rejectedFiles,
        new HashMap<>(),
        "json");

    testBuilder()
        .sqlQuery(getCopyErrorsQuery(targetTableName, jobId))
        .unOrdered()
        .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
        .baselineValues(
            jobId,
            rejectedFiles.get(0),
            6L,
            1L,
            "c2",
            "While processing field \"c2\". Could not convert \"eleven\" to INT.")
        .go();

    dropTable(targetTableName);
  }

  @Test
  public void testComplexJsonList() throws Exception {
    List<List<Pair<String, String>>> testSchemas =
        ImmutableList.of(
            ImmutableList.of(Pair.of("ooa", "array<array<int>>")),
            ImmutableList.of(Pair.of("ooa", "array<int>")));

    List<Object[]> expectedErrorDetails =
        ImmutableList.of(
            // table matching schema, but type error in the file
            new Object[] {2L, 2L, null, "Could not convert \"fifty\" to INT."},
            // schema mismatch in a nested level
            new Object[] {
              1L,
              1L,
              "",
              "Field  having List datatype in the file cannot be coerced into Int(32, true) datatype of the target table."
            });

    for (int i = 0; i < testSchemas.size(); ++i) {
      String targetTableName = "complex_list" + i;
      List<Pair<String, String>> colNameTypePairs = testSchemas.get(i);

      String[] inputFileNames = new String[] {"list_of_list_nestedtypeerror.json"};
      File inputFilesLocation = createTempLocation();
      File[] inputFiles =
          createTableAndGenerateSourceFiles(
              targetTableName,
              colNameTypePairs,
              inputFileNames,
              inputFilesLocation,
              FileFormat.JSON);
      List<String> rejectedFiles =
          Arrays.stream(inputFiles).map(File::getAbsolutePath).collect(Collectors.toList());

      String jobId = "1b06239f-c613-dee4-d9b6-5f79e00cbef" + i;
      addJobAndFileHistoryEntry(
          jobId,
          targetTableName,
          inputFilesLocation.getName(),
          rejectedFiles,
          new HashMap<>(),
          "json");

      testBuilder()
          .sqlQuery(getCopyErrorsQuery(targetTableName, jobId))
          .unOrdered()
          .baselineColumns(COPY_ERRORS_TABLEFUNCTION_COLUMN_NAMES)
          .baselineValues(
              jobId,
              rejectedFiles.get(0),
              expectedErrorDetails.get(i)[0],
              expectedErrorDetails.get(i)[1],
              expectedErrorDetails.get(i)[2],
              expectedErrorDetails.get(i)[3])
          .go();

      dropTable(targetTableName);
    }
  }

  @Test
  public void testParquetValidation() throws Exception {
    String targetTableName = "parquet_test";
    ImmutableList<Pair<String, String>> colNameTypePairs =
        ImmutableList.of(Pair.of("name", "varchar"), Pair.of("age", "int"));

    final String[] inputFileNames =
        new String[] {
          "typeError5.parquet",
          "typeError4.parquet",
          "syntaxError.parquet",
          "corruption_in_2nd_rowgroup.parquet",
          "corruption_in_2nd_rowgroup_without_corruption.parquet"
        };
    File inputFilesLocation = createTempLocation();
    File[] inputFiles =
        createTableAndGenerateSourceFiles(
            targetTableName,
            colNameTypePairs,
            inputFileNames,
            inputFilesLocation,
            FileFormat.PARQUET);
    List<String> rejectedFiles =
        Arrays.stream(inputFiles).map(File::getAbsolutePath).collect(Collectors.toList());

    String jobId = "1b06239f-c613-dee4-d9b6-5f79e0098765";
    addJobAndFileHistoryEntry(
        jobId,
        targetTableName,
        inputFilesLocation.getName(),
        rejectedFiles,
        new HashMap<>(),
        "parquet");

    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        new HashMap<>() {
          {
            put("`job_id`", jobId);
            put("`file_name`", rejectedFiles.get(3));
            put("`error`", "Failed to read data from parquet file in rowgroup 1");
            put("`line_number`", null);
          }
        });
    recordBuilder.add(
        new HashMap<>() {
          {
            put("`job_id`", jobId);
            put("`file_name`", rejectedFiles.get(2));
            put("`error`", "The file " + rejectedFiles.get(2) + " is not in Parquet format.");
            put("`line_number`", null);
          }
        });
    recordBuilder.add(
        new HashMap<>() {
          {
            put("`job_id`", jobId);
            put("`file_name`", rejectedFiles.get(0));
            put(
                "`error`",
                "Failure while attempting to cast value 'not an age' to Integer. in rowgroup 0");
            put("`line_number`", 2L);
          }
        });
    testBuilder()
        .sqlQuery(
            getCopyErrorsQuery(
                targetTableName, jobId, "job_id", "file_name", "error", "line_number"))
        .unOrdered()
        .baselineRecords(recordBuilder.build())
        .go();

    dropTable(targetTableName);
  }

  @Test
  public void testParquetValidationWithComlexTypes() throws Exception {
    String targetTableName = "parquet_complex_test";
    ImmutableList<Pair<String, String>> colNameTypePairs =
        ImmutableList.of(
            Pair.of("id", "int"),
            Pair.of(
                "s",
                "STRUCT<id INT, city VARCHAR, positions LIST<STRUCT<lon DOUBLE, lat DOUBLE>>>"));

    final String[] inputFileNames = new String[] {"struct_coercion_error.parquet"};
    File inputFilesLocation = createTempLocation();
    File[] inputFiles =
        createTableAndGenerateSourceFiles(
            targetTableName,
            colNameTypePairs,
            inputFileNames,
            inputFilesLocation,
            FileFormat.PARQUET);
    List<String> rejectedFiles =
        Arrays.stream(inputFiles).map(File::getAbsolutePath).collect(Collectors.toList());

    String jobId = "1b06239f-c613-dee4-d9b6-5f79e0098921";
    addJobAndFileHistoryEntry(
        jobId,
        targetTableName,
        inputFilesLocation.getName(),
        rejectedFiles,
        new HashMap<>(),
        "parquet");

    ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        new HashMap<>() {
          {
            put("`job_id`", jobId);
            put("`file_name`", rejectedFiles.get(0));
            put(
                "`error`",
                "Failure while attempting to cast value 'three' to Integer. in rowgroup 0");
            put("`line_number`", 3L);
          }
        });
    testBuilder()
        .sqlQuery(
            getCopyErrorsQuery(
                targetTableName, jobId, "job_id", "file_name", "error", "line_number"))
        .unOrdered()
        .baselineRecords(recordBuilder.build())
        .go();

    try (AutoCloseable autoCloseable =
        setSystemOptionWithAutoReset(
            "dremio.copy.into.errors_first_error_of_record_only", "false")) {
      recordBuilder = ImmutableList.builder();
      recordBuilder.add(
          new HashMap<>() {
            {
              put("`job_id`", jobId);
              put("`file_name`", rejectedFiles.get(0));
              put(
                  "`error`",
                  "Failure while attempting to cast value 'three' to Integer., Failure while attempting to cast value 'thirty' to Integer., For input string: \"forty\" in rowgroup 0");
              put("`line_number`", 3L);
            }
          });
      testBuilder()
          .sqlQuery(
              getCopyErrorsQuery(
                  targetTableName, jobId, "job_id", "file_name", "error", "line_number"))
          .unOrdered()
          .baselineRecords(recordBuilder.build())
          .go();
    }

    dropTable(targetTableName);
  }

  @Test
  public void testRelativeRejectedFilePathConstruction() {
    List<String> result =
        CopyIntoTablePlanBuilderBase.constructRelativeRejectedFilePathes(
            // should work on paths both with & without scheme part
            ImmutableList.of(
                "dremioS3:/bucket.name/folder/subfolder/subfile.csv",
                "/bucket.name/folder/file.csv"),
            "s3Source.folder",
            Path.of("dremioS3:/bucket.name/folder"));
    assertEquals(ImmutableList.of("/subfolder/subfile.csv", "/file.csv"), result);

    result =
        CopyIntoTablePlanBuilderBase.constructRelativeRejectedFilePathes(
            ImmutableList.of("dremioS3:/bucket.name/folder/subfolder/subfile.csv"),
            "s3Source.folder.subfolder",
            Path.of("dremioS3:/bucket.name/folder/subfolder"));
    assertEquals(ImmutableList.of("/subfile.csv"), result);

    result =
        CopyIntoTablePlanBuilderBase.constructRelativeRejectedFilePathes(
            ImmutableList.of("dremioS3:/bucket.name/folder/tmp/tmp/tmp/file.csv"),
            "s3Source.folder.tmp/tmp",
            Path.of("dremioS3:/bucket.name/folder/tmp/tmp"));
    assertEquals(ImmutableList.of("/tmp/file.csv"), result);

    Throwable exception =
        assertThrows(
            UserException.class,
            () ->
                CopyIntoTablePlanBuilderBase.constructRelativeRejectedFilePathes(
                    ImmutableList.of("dremioS3:/bucket.name/folder/subfolder/subfile.csv"),
                    "s3.folder",
                    Path.of("dremioS3:/other.name/other")));

    assertTrue(
        exception
            .getMessage()
            .startsWith(
                "Found a source file with rejection that has a different path compared to root of the provided source path."));
  }

  private static String getCopyErrorsQuery(String targetTableName, String jobId, String... cols) {
    String projection = "*";
    if (cols != null && cols.length > 0) {
      projection = String.join(",", cols);
    }
    StringBuilder sb = new StringBuilder("SELECT ").append(projection);
    sb.append(" FROM TABLE(COPY_ERRORS(");
    sb.append(String.format("'%s.%s'", TEMP_SCHEMA, targetTableName));

    if (!StringUtils.isEmpty(jobId)) {
      sb.append(String.format(", '%s'", jobId));
    } else {
      sb.append(", NULL");
    }

    // disable strict_consistency for these tests as they're not fully end-2-end and they don't
    // really write target table
    sb.append(", false))");
    return sb.toString();
  }
}
