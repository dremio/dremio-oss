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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.planner.sql.ITCopyIntoBase.createTableWithSortOrderAndGenerateSourceFile;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.sql.ITCopyIntoBase;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.parquet.AllRowGroupsParquetReader;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderOptions;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIcebergSortOrder extends BaseTestQuery {

  private static final List<String> TABLE_1_COLUMN_LIST =
      Collections.singletonList("col1 INT, col2 INT, col3 DOUBLE, col4 FLOAT");
  private static final List<String> TABLE_1_COLUMN_LIST_SMALL =
      Collections.singletonList("col1 INT, col2 INT");
  private static final List<String> TABLE_BOOLEAN_COLUMN_LIST =
      Collections.singletonList("a BOOLEAN, b BOOLEAN, c BOOLEAN, d BOOLEAN");
  private OperatorContextImpl context;
  private FileSystem fs;
  private SampleMutator mutator;

  protected final List<AutoCloseable> testCloseables = new ArrayList<>();

  protected static final String TEMP_SCHEMA = "dfs_test";

  public static Supplier<IcebergTestTables.Table> NATION =
      () -> getTable("iceberg/nation", "dfs_hadoop", "/tmp/iceberg");

  private static final String COL_1 = "col1";
  private static final String COL_2 = "col2";
  private static final String COL_3 = "col3";
  private static final String COL_4 = "col4";
  private static final String COL_BOOL_A = "a";
  private static final String COL_BOOL_B = "b";
  private static final String COL_BOOL_C = "c";
  private static final String COL_BOOL_D = "d";
  private static final String COL_DATE = "colDate";

  private static final ParquetScanProjectedColumns BOOLEAN_TABLE_COLUMNS =
      ParquetScanProjectedColumns.fromSchemaPaths(
          ImmutableList.of(
              SchemaPath.getSimplePath(COL_BOOL_A),
              SchemaPath.getSimplePath(COL_BOOL_B),
              SchemaPath.getSimplePath(COL_BOOL_C),
              SchemaPath.getSimplePath(COL_BOOL_D)));

  private static final ParquetScanProjectedColumns TABLE_1_ALL_COLUMNS =
      ParquetScanProjectedColumns.fromSchemaPaths(
          ImmutableList.of(
              SchemaPath.getSimplePath(COL_1),
              SchemaPath.getSimplePath(COL_2),
              SchemaPath.getSimplePath(COL_3),
              SchemaPath.getSimplePath(COL_4)));

  private static final ParquetScanProjectedColumns TABLE_DATE =
      ParquetScanProjectedColumns.fromSchemaPaths(
          ImmutableList.of(SchemaPath.getSimplePath(COL_1), SchemaPath.getSimplePath(COL_DATE)));

  private static final ParquetReaderOptions DEFAULT_READER_OPTIONS =
      ParquetReaderOptions.builder().enablePrefetching(false).setNumSplitsToPrefetch(0).build();

  private static final BatchSchema SORT_ORDER_TABLE_FULL =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable(COL_1, new ArrowType.Int(32, true)),
              Field.nullable(COL_2, new ArrowType.Int(32, true)),
              Field.nullable(COL_3, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullable(COL_4, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));

  private static final BatchSchema SORT_ORDER_TABLE_SMALL =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable(COL_1, new ArrowType.Int(32, true)),
              Field.nullable(COL_2, new ArrowType.Int(32, true))));

  private static final BatchSchema SORT_ORDER_TABLE_DATE =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable(COL_1, new ArrowType.Int(32, true)),
              Field.nullable(COL_DATE, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));

  private static final BatchSchema SORT_ORDER_TABLE_BOOLEAN =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable(COL_BOOL_A, new ArrowType.Bool()),
              Field.nullable(COL_BOOL_B, new ArrowType.Bool()),
              Field.nullable(COL_BOOL_C, new ArrowType.Bool()),
              Field.nullable(COL_BOOL_D, new ArrowType.Bool())));

  private static final List<Integer> COLUMN_ONE =
      Arrays.asList(17, 3, 11, 19, 8, 14, 2, 6, 20, 15, 9, 10, 5, 4, 1, 12, 7, 13, 16, 18);
  public static final List<Integer> COLUMN_TWO =
      Arrays.asList(
          30, 120, 10, 40, 60, 90, 200, 170, 150, 50, 190, 70, 110, 100, 180, 160, 20, 140, 130,
          80);

  public static final List<Double> COLUMN_THREE =
      Arrays.asList(
          -4.0, -3.0, -2.5, -0.5, 2.0, 3.0, -1.0, 1.5, -5.0, 3.5, 0.0, 1.0, 0.5, 4.0, 2.5, -1.5,
          -3.5, -2.0, 0.0, 4.5);

  public static final List<Float> COLUMN_FOUR =
      Arrays.asList(
          -3999.875f,
          -2999.875f,
          -4999.875f,
          -6999.875f,
          -5999.875f,
          -1999.875f,
          125.0f,
          -999.875f,
          -8999.875f,
          -9999.875f,
          -7999.875f,
          1125.0f,
          -2999.875f,
          2125.0f,
          3125.0f,
          4125.0f,
          5125.0f,
          6125.0f,
          7125.0f,
          8125.0f);

  public static final List<Boolean> COLUMN_BOOL_TIER_1 =
      Arrays.asList(
          false, false, false, false, false, false, false, false, true, true, true, true, true,
          true, true, true);

  public static final List<Boolean> COLUMN_BOOL_TIER_2 =
      Arrays.asList(
          false, false, false, false, true, true, true, true, false, false, false, false, true,
          true, true, true);

  public static final List<Boolean> COLUMN_BOOL_TIER_3 =
      Arrays.asList(
          false, false,
          true, true,
          false, false,
          true, true,
          false, false,
          true, true,
          false, false,
          true, true);

  public static final List<Boolean> COLUMN_BOOL_TIER_4 =
      Arrays.asList(
          false, true, false, true, false, true, false, true, false, true, false, true, false, true,
          false, true);

  public static String buildValues() {
    return IntStream.range(0, COLUMN_ONE.size())
        .mapToObj(
            i ->
                String.format(
                    "(%d, %d, %.1f, %.3f)",
                    COLUMN_ONE.get(i), COLUMN_TWO.get(i), COLUMN_THREE.get(i), COLUMN_FOUR.get(i)))
        .collect(Collectors.joining(", "));
  }

  public static String buildBooleanTableValuesForInsert() {

    List<Boolean> columnOneShuffled = new ArrayList<>(COLUMN_BOOL_TIER_1);
    Collections.shuffle(columnOneShuffled);

    List<Boolean> columnTwoShuffled = new ArrayList<>(COLUMN_BOOL_TIER_2);
    Collections.shuffle(columnTwoShuffled);

    List<Boolean> columnThreeShuffled = new ArrayList<>(COLUMN_BOOL_TIER_3);
    Collections.shuffle(columnThreeShuffled);

    List<Boolean> columnFourShuffled = new ArrayList<>(COLUMN_BOOL_TIER_4);
    Collections.shuffle(columnFourShuffled);

    return IntStream.range(0, COLUMN_BOOL_TIER_1.size())
        .mapToObj(
            i ->
                String.format(
                    "(%b, %b, %b, %b)",
                    COLUMN_BOOL_TIER_1.get(i),
                    COLUMN_BOOL_TIER_2.get(i),
                    COLUMN_BOOL_TIER_3.get(i),
                    COLUMN_BOOL_TIER_4.get(i)))
        .collect(Collectors.joining(", "));
  }

  public static String buildValuesSmall() {
    return IntStream.range(0, COLUMN_ONE.size())
        .mapToObj(i -> String.format("(%d, %d)", COLUMN_ONE.get(i), COLUMN_TWO.get(i)))
        .collect(Collectors.joining(", "));
  }

  @BeforeClass
  public static void setup() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_DML, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, "true");
  }

  @AfterClass
  public static void reset() {
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER.getOptionName());
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_DML.getOptionName());
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getOptionName());
  }

  @Before
  public void before() throws Exception {
    context = getNewOperatorContext(getAllocator());
    testCloseables.add(context);
    fs = HadoopFileSystem.getLocal(new Configuration());
    mutator = new SampleMutator(getAllocator());
    testCloseables.add(mutator);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    Collections.reverse(testCloseables);
    AutoCloseables.close(testCloseables);
  }

  public String getColumnsForCreate(List<String> columns) {
    return String.join(", ", columns);
  }

  @Test
  public void testSortOrderInMetadataJsonWithoutTransformations() throws Exception {
    String tableName = "temp_table0";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (col1 int, col2 date) LOCALSORT BY (col1, col2)",
            TEMP_SCHEMA, tableName));
    String tableRootLoc = getDfsTestTmpSchemaLocation();
    String metadataLoc = tableRootLoc + String.format("/%s/metadata", tableName);
    File metadataFolder = new File(metadataLoc);
    String metadataJson = "";

    Assert.assertNotNull(metadataFolder.listFiles());
    for (File currFile : metadataFolder.listFiles()) {
      if (currFile.getName().endsWith("metadata.json")) {
        // We'll only have one metadata.json file as this is the first transaction for table
        // temp_table0
        metadataJson =
            new String(
                Files.readAllBytes(Paths.get(currFile.getPath())), StandardCharsets.US_ASCII);
        break;
      }
    }
    Assert.assertNotNull(metadataJson);

    JsonElement metadataJsonElement = new JsonParser().parse(metadataJson);
    JsonObject metadataJsonObject = metadataJsonElement.getAsJsonObject();
    Assert.assertEquals(1, metadataJsonObject.get("default-sort-order-id").getAsInt());

    JsonArray sortOrdersJson = metadataJsonObject.getAsJsonArray("sort-orders");
    Assert.assertEquals(1, sortOrdersJson.get(0).getAsJsonObject().get("order-id").getAsInt());

    JsonArray sortOrderFields = sortOrdersJson.get(0).getAsJsonObject().getAsJsonArray("fields");
    // Orders of the following two objects are mapped according to order of columns passed to
    // LOCALSORT in the create table sql command.
    // The command substring, "LOCALSORT BY (col1, col2)" means sortOrdersJson.get(0) maps to col1
    // and sortOrdersJson.get(1) maps to col2
    JsonObject sortOrderCol1Json = sortOrderFields.get(0).getAsJsonObject();
    JsonObject sortOrderCol2Json = sortOrderFields.get(1).getAsJsonObject();

    Assert.assertEquals("identity", sortOrderCol1Json.get("transform").getAsString());
    Assert.assertEquals(1, sortOrderCol1Json.get("source-id").getAsInt());
    Assert.assertEquals("asc", sortOrderCol1Json.get("direction").getAsString());
    Assert.assertEquals("nulls-first", sortOrderCol1Json.get("null-order").getAsString());

    Assert.assertEquals("identity", sortOrderCol2Json.get("transform").getAsString());
    Assert.assertEquals(2, sortOrderCol2Json.get("source-id").getAsInt());
    Assert.assertEquals("asc", sortOrderCol2Json.get("direction").getAsString());
    Assert.assertEquals("nulls-first", sortOrderCol2Json.get("null-order").getAsString());
  }

  @Test
  public void testSortByInt() throws Exception {
    String tableName = "temp_table1";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (col1)",
            TEMP_SCHEMA, tableName, getColumnsForCreate(TABLE_1_COLUMN_LIST)));
    runSQL(String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableName, buildValues()));

    List<String> fileList = scanFolders(tableName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_FULL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_FULL);

      IntVector col1IntVector = (IntVector) mutator.getVector(COL_1);
      List<Integer> actualRows = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRows.add(col1IntVector.get(i));
        }
      }
      assertThat(actualRows)
          .isEqualTo(
              Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
    }
  }

  @Test
  public void testSortByInt2() throws Exception {
    String tableName = "temp_table2";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableName, getColumnsForCreate(TABLE_1_COLUMN_LIST), COL_2));
    runSQL(String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableName, buildValues()));

    List<String> fileList = scanFolders(tableName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_FULL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_FULL);

      IntVector col2IntVector = (IntVector) mutator.getVector(COL_2);
      List<Integer> actualRows = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRows.add(col2IntVector.getObject(i));
        }
      }
      ArrayList<Integer> sortedList = new ArrayList<>(COLUMN_TWO);
      Collections.sort(sortedList);
      assertThat(actualRows).isEqualTo(sortedList);
    }
  }

  @Test
  public void testSortByDouble() throws Exception {
    String tableName = "temp_table3";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableName, getColumnsForCreate(TABLE_1_COLUMN_LIST), COL_3));
    runSQL(String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableName, buildValues()));

    List<String> fileList = scanFolders(tableName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_FULL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_FULL);

      Float8Vector doubleVector = (Float8Vector) mutator.getVector(COL_3);
      List<Double> actualRows = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRows.add(doubleVector.getObject(i));
        }
      }
      ArrayList<Double> sortedList = new ArrayList<>(COLUMN_THREE);
      Collections.sort(sortedList);
      assertThat(actualRows).isEqualTo(sortedList);
    }
  }

  @Test
  public void testSortByFloat() throws Exception {
    String tableName = "temp_table4";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableName, getColumnsForCreate(TABLE_1_COLUMN_LIST), COL_4));
    runSQL(String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableName, buildValues()));

    List<String> fileList = scanFolders(tableName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_FULL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_FULL);

      Float4Vector floatVector = (Float4Vector) mutator.getVector(COL_4);
      List<Float> actualRows = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRows.add(floatVector.getObject(i));
        }
      }
      ArrayList<Float> sortedList = new ArrayList<>(COLUMN_FOUR);
      Collections.sort(sortedList);
      assertThat(actualRows).isEqualTo(sortedList);
    }
  }

  @Test
  public void testSortOnMerge() throws Exception {
    String tableHome = "temp_table5";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableHome, getColumnsForCreate(TABLE_1_COLUMN_LIST_SMALL), COL_2));
    runSQL(
        String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableHome, buildValuesSmall()));

    String tableMerge = "table_to_merge";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s)",
            TEMP_SCHEMA, tableMerge, getColumnsForCreate(TABLE_1_COLUMN_LIST_SMALL)));
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES (0, 100), (-17, -100), (-4, 30)", TEMP_SCHEMA, tableHome));

    runSQL(
        String.format(
            "MERGE INTO %s.%s USING %s.%s "
                + "on (%s.%s.%s = %s.%s.%s) "
                + "WHEN MATCHED THEN UPDATE SET * "
                + "WHEN NOT MATCHED THEN INSERT *;",
            TEMP_SCHEMA,
            tableHome,
            TEMP_SCHEMA,
            tableMerge,
            TEMP_SCHEMA,
            tableHome,
            COL_2,
            TEMP_SCHEMA,
            tableMerge,
            COL_2));

    List<String> fileList = scanFolders(tableHome);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_SMALL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_SMALL);

      IntVector col2Vector = (IntVector) mutator.getVector(COL_2);
      List<Integer> actualRowValsForCol2 = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowValsForCol2.add(col2Vector.getObject(i));
        }
      }
      ArrayList<Integer> sortedList = new ArrayList<>(COLUMN_TWO);
      Collections.sort(sortedList);
      List<Integer> sortedList2 = Arrays.asList(-100, 30, 100);
      List<Integer> matchList =
          Objects.deepEquals(actualRowValsForCol2, sortedList) ? sortedList : sortedList2;
      assertThat(actualRowValsForCol2).isEqualTo(matchList);
    }
  }

  @Test
  public void testSortOnMergeOnTablesWithDiffSortingConfigs() throws Exception {
    String tableHome = "temp_table6";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableHome, getColumnsForCreate(TABLE_1_COLUMN_LIST_SMALL), COL_2));
    runSQL(
        String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableHome, buildValuesSmall()));

    String tableMerge = "table_to_merge_sorted";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableMerge, getColumnsForCreate(TABLE_1_COLUMN_LIST_SMALL), COL_1));
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES (-5000, 100), (-17, -100), (-4, 30)",
            TEMP_SCHEMA, tableHome));

    runSQL(
        String.format(
            "MERGE INTO %s.%s USING %s.%s "
                + "on (%s.%s.%s = %s.%s.%s) "
                + "WHEN MATCHED THEN UPDATE SET * "
                + "WHEN NOT MATCHED THEN INSERT *;",
            TEMP_SCHEMA,
            tableHome,
            TEMP_SCHEMA,
            tableMerge,
            TEMP_SCHEMA,
            tableHome,
            COL_2,
            TEMP_SCHEMA,
            tableMerge,
            COL_2));

    List<String> fileList = scanFolders(tableHome);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_SMALL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_SMALL);

      IntVector col1Vector = (IntVector) mutator.getVector(COL_1);
      IntVector col2Vector = (IntVector) mutator.getVector(COL_2);
      List<Integer> actualRowValsForCol1 = new ArrayList<>();
      List<Integer> actualRowValsForCol2 = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowValsForCol1.add(col1Vector.getObject(i));
          actualRowValsForCol2.add(col2Vector.getObject(i));
        }
      }
      ArrayList<Integer> sortedList = new ArrayList<>(COLUMN_TWO);
      Collections.sort(sortedList);
      List<Integer> sortedList2 = Arrays.asList(-100, 30, 100);
      List<Integer> matchList =
          Objects.deepEquals(actualRowValsForCol2, sortedList) ? sortedList : sortedList2;
      assertThat(actualRowValsForCol2).isEqualTo(matchList);
    }
  }

  @Test
  public void testAlterSortOrder() throws Exception {
    String tableName = "temp_table_bool";
    String initialColumnSortOrder = "a, b, c, d";
    String alteredColumnSortOrder = "c, a, d, b";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA,
            tableName,
            getColumnsForCreate(TABLE_BOOLEAN_COLUMN_LIST),
            initialColumnSortOrder));
    runSQL(
        String.format(
            "ALTER TABLE %s.%s LOCALSORT BY (%s)", TEMP_SCHEMA, tableName, alteredColumnSortOrder));
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES %s",
            TEMP_SCHEMA, tableName, buildBooleanTableValuesForInsert()));

    List<String> fileList = scanFolders(tableName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_BOOLEAN, BOOLEAN_TABLE_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_BOOLEAN);

      BitVector boolVectorA = (BitVector) mutator.getVector(COL_BOOL_A);
      BitVector boolVectorB = (BitVector) mutator.getVector(COL_BOOL_B);
      BitVector boolVectorC = (BitVector) mutator.getVector(COL_BOOL_C);
      BitVector boolVectorD = (BitVector) mutator.getVector(COL_BOOL_D);
      List<Boolean> actualRowsA = new ArrayList<>();
      List<Boolean> actualRowsB = new ArrayList<>();
      List<Boolean> actualRowsC = new ArrayList<>();
      List<Boolean> actualRowsD = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowsA.add(boolVectorA.getObject(i));
          actualRowsB.add(boolVectorB.getObject(i));
          actualRowsC.add(boolVectorC.getObject(i));
          actualRowsD.add(boolVectorD.getObject(i));
        }
      }

      assertThat(actualRowsA).isEqualTo(COLUMN_BOOL_TIER_2);
      assertThat(actualRowsB).isEqualTo(COLUMN_BOOL_TIER_4);
      assertThat(actualRowsC).isEqualTo(COLUMN_BOOL_TIER_1);
      assertThat(actualRowsD).isEqualTo(COLUMN_BOOL_TIER_3);
    }
  }

  @Test
  public void testSqlFunctions() throws Exception {
    String tableName = "temp_table15";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (col1 int, col2 int) LOCALSORT BY (col1)", TEMP_SCHEMA, tableName));
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES (4, 74), (2, 1), (7, 47), (0, -20), (1, 6)",
            TEMP_SCHEMA, tableName));
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES (3, 50), (-10, 55), (9, -23), (23, 0), (-8, 4)",
            TEMP_SCHEMA, tableName));

    testBuilder()
        .ordered()
        .sqlQuery(String.format("SELECT MEDIAN(col1) FROM %s.%s", TEMP_SCHEMA, tableName))
        .baselineColumns("EXPR$0")
        .baselineValues(2.5)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            String.format("SELECT ABS(col1) FROM %s.%s WHERE col1 = -10", TEMP_SCHEMA, tableName))
        .baselineColumns("EXPR$0")
        .baselineValues(10)
        .go();

    testBuilder()
        .ordered()
        .sqlQuery(
            String.format("SELECT AVG(col1) FROM %s.%s WHERE col1 >= 0", TEMP_SCHEMA, tableName))
        .baselineColumns("EXPR$0")
        .baselineValues(6.125)
        .go();
  }

  @Test
  public void testDescribeTableOnSortOrder() throws Exception {
    String tableNameA = "temp_table16_A";
    String tableNameB = "temp_table16_B";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (a BOOLEAN, aaa BOOLEAN, aa BOOLEAN) LOCALSORT BY (aaa, a, aa)",
            TEMP_SCHEMA, tableNameA));
    testBuilder()
        .ordered()
        .sqlQuery(String.format("DESCRIBE %s.%s", TEMP_SCHEMA, tableNameA))
        .baselineColumns(
            "COLUMN_NAME",
            "DATA_TYPE",
            "IS_NULLABLE",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "EXTENDED_PROPERTIES",
            "MASKING_POLICY",
            "SORT_ORDER_PRIORITY")
        .baselineValues("aa", "BOOLEAN", "YES", null, null, "[]", "[]", 3)
        .baselineValues("aaa", "BOOLEAN", "YES", null, null, "[]", "[]", 1)
        .baselineValues("a", "BOOLEAN", "YES", null, null, "[]", "[]", 2);

    runSQL(
        String.format(
            "CREATE TABLE %s.%s (bb BOOLEAN, bbb BOOLEAN, b BOOLEAN) LOCALSORT BY (b, bbb)",
            TEMP_SCHEMA, tableNameB));
    testBuilder()
        .ordered()
        .sqlQuery(String.format("DESCRIBE %s.%s", TEMP_SCHEMA, tableNameB))
        .baselineColumns(
            "COLUMN_NAME",
            "DATA_TYPE",
            "IS_NULLABLE",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "EXTENDED_PROPERTIES",
            "MASKING_POLICY",
            "SORT_ORDER_PRIORITY")
        .baselineValues("bb", "BOOLEAN", "YES", null, null, "[]", "[]", null)
        .baselineValues("bbb", "BOOLEAN", "YES", null, null, "[]", "[]", 2)
        .baselineValues("b", "BOOLEAN", "YES", null, null, "[]", "[]", 1);
  }

  @Test
  public void testSortOnDateAndCastFunction() throws Exception {
    String tableDateName = "temp_table16";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s INT, %s TIMESTAMP) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableDateName, COL_1, COL_DATE, COL_DATE));

    // Running an insert query that inserts 5 randomly ordered dates with integers representing
    // sorted order.
    runSQL(
        String.format(
            "INSERT INTO %s.%s VALUES (4, TIMESTAMP '2008-11-01 13:15:37.12'), (3, TIMESTAMP '2008-10-01 12:14:36.12'), (5, TIMESTAMP '2023-07-27 14:16:38.12'), (2, TIMESTAMP '2003-09-30 11:13:35.12'), (1, TIMESTAMP '2003-09-29 10:12:34.12')",
            TEMP_SCHEMA, tableDateName));

    List<String> fileList = scanFolders(tableDateName);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_DATE, TABLE_DATE);
      setupReader(reader, SORT_ORDER_TABLE_DATE);

      TimeStampMilliVector col2Vector = (TimeStampMilliVector) mutator.getVector(COL_DATE);
      List<LocalDate> actualRowDates = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowDates.add(col2Vector.getObject(i).toLocalDate());
        }
      }

      List<LocalDate> sortedList =
          Arrays.asList(
              LocalDate.of(2003, 9, 29),
              LocalDate.of(2003, 9, 30),
              LocalDate.of(2008, 10, 1),
              LocalDate.of(2008, 11, 1),
              LocalDate.of(2023, 7, 27));

      assertThat(actualRowDates).isEqualTo(sortedList);
    }

    testBuilder()
        .ordered()
        .sqlQuery(
            String.format(
                "SELECT CAST(YEAR(%s) AS INTEGER) FROM %s.%s",
                COL_DATE, TEMP_SCHEMA, tableDateName))
        .baselineColumns("EXPR$0")
        .baselineValues(2003)
        .baselineValues(2003)
        .baselineValues(2008)
        .baselineValues(2008)
        .baselineValues(2023)
        .go();
  }

  @Test
  public void testSortOnUpdate() throws Exception {
    String tableHome = "temp_table8";
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (%s) LOCALSORT BY (%s)",
            TEMP_SCHEMA, tableHome, getColumnsForCreate(TABLE_1_COLUMN_LIST_SMALL), COL_1));
    runSQL(
        String.format("INSERT INTO %s.%s VALUES %s", TEMP_SCHEMA, tableHome, buildValuesSmall()));
    runSQL(
        String.format(
            "UPDATE %s.%s SET %s = 450 WHERE %s = 100", TEMP_SCHEMA, tableHome, COL_1, COL_2));

    List<String> fileList = scanFolders(tableHome);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_SMALL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_SMALL);

      IntVector col1Vector = (IntVector) mutator.getVector(COL_1);
      List<Integer> actualRowValsForCol1 = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowValsForCol1.add(col1Vector.getObject(i));
        }
      }
      ArrayList<Integer> sortedList = new ArrayList<>(COLUMN_ONE);
      Collections.sort(sortedList);

      ArrayList<Integer> updatedList = new ArrayList<>(sortedList);
      updatedList.remove((Object) 4);
      updatedList.add(450);
      ArrayList<Integer> matchList =
          actualRowValsForCol1.equals(sortedList) ? sortedList : updatedList;
      assertThat(actualRowValsForCol1).isEqualTo(matchList);
    }
  }

  @Test
  public void testSortOrderOnCtas() throws Exception {
    String tableName = "temp_table_7";
    String tableNameCtas = "temp_table_ctas";

    runSQL(String.format("CREATE TABLE %s.%s (%s int)", TEMP_SCHEMA, tableName, COL_1));

    // Running an insert query that inserts 5 randomly ordered dates with integers representing
    // sorted order.
    runSQL(
        String.format("INSERT INTO %s.%s VALUES (4), (3), (5), (2), (1)", TEMP_SCHEMA, tableName));
    runSQL(
        String.format(
            "CREATE TABLE %s.%s LOCALSORT BY (%s) AS (SELECT * FROM %s.%s)",
            TEMP_SCHEMA, tableNameCtas, COL_1, TEMP_SCHEMA, tableName));

    List<String> fileList = scanFolders(tableNameCtas);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_SMALL, TABLE_1_ALL_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_SMALL);

      IntVector col1Vector = (IntVector) mutator.getVector(COL_1);
      List<Integer> actualRowValsForCol1 = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowValsForCol1.add(col1Vector.getObject(i));
        }
      }
      List<Integer> sortedList = Arrays.asList(1, 2, 3, 4, 5);

      assertThat(actualRowValsForCol1).isEqualTo(sortedList);
    }
  }

  @Test
  public void testSortOnCopyInto() throws Exception {

    File location = createTempLocation();
    String targetTable = "singleBoolColTable";
    List<String> sortColumns =
        new ArrayList<String>() {
          {
            add("a");
          }
        };

    File newSourceFile =
        createTableWithSortOrderAndGenerateSourceFile(
            targetTable,
            ImmutableList.of(Pair.of("a", "BOOLEAN")),
            "singleBoolColUnsorted.csv",
            location,
            ITCopyIntoBase.FileFormat.CSV,
            sortColumns);

    runSQL(String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, targetTable));
    String storageLocation = "'@" + TEMP_SCHEMA + "/" + location.getName() + "'";
    String copyIntoQuery =
        String.format(
            "COPY INTO %s.%s FROM %s regex 'singleBoolColUnsorted\\.csv' (RECORD_DELIMITER '\n')",
            TEMP_SCHEMA, targetTable, storageLocation);
    test(copyIntoQuery);

    List<String> fileList = scanFolders(targetTable);
    for (String file : fileList) {
      AllRowGroupsParquetReader reader =
          initializeReader(Path.of(file), SORT_ORDER_TABLE_BOOLEAN, BOOLEAN_TABLE_COLUMNS);
      setupReader(reader, SORT_ORDER_TABLE_BOOLEAN);

      BitVector col1Vector = (BitVector) mutator.getVector(COL_BOOL_A);
      List<Boolean> actualRowValsForCol1 = new ArrayList<>();

      int records;
      while ((records = reader.next()) != 0) {
        for (int i = 0; i < records; i++) {
          actualRowValsForCol1.add(col1Vector.getObject(i));
        }
      }

      List<Boolean> sortedList =
          Arrays.asList(
              false, false, false, false, false, false, false, true, true, true, true, true);

      assertThat(sortedList).isEqualTo(actualRowValsForCol1);
    }

    testBuilder()
        .sqlQuery("SELECT * FROM %s.%s ", TEMP_SCHEMA, targetTable)
        .ordered()
        .baselineColumns("a")
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .go();

    Assert.assertTrue(newSourceFile.delete());

    String dropQuery = String.format("DROP TABLE %s.%s", TEMP_SCHEMA, targetTable);
    test(dropQuery);
  }

  public static void testDisabledFeatureFlag(String source) {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER, "false");
    String tableName = "badTable";
    String query =
        String.format("CREATE TABLE %s.%s (col1 INT) LOCALSORT BY (col1)", source, tableName);
    Assertions.assertThatThrownBy(
            () -> {
              // Replace with the actual call to your foo method
              runSQL(query);
            })
        .isInstanceOf(UserException.class) // Replace with your actual exception class
        .hasMessageContaining("Iceberg Sort Order Operations are disabled");
    resetSystemOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER.getOptionName());
  }

  protected static File createTempLocation() {
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

  private void setupReader(AllRowGroupsParquetReader reader, BatchSchema schema) throws Exception {
    for (Field field : schema.getFields()) {
      mutator.addField(field, TypeHelper.getValueVectorClass(field));
    }
    mutator.getContainer().buildSchema();
    mutator.getAndResetSchemaChanged();

    reader.setup(mutator);
    reader.allocate(mutator.getFieldVectorMap());
  }

  private AllRowGroupsParquetReader initializeReader(
      Path path, BatchSchema schema, ParquetScanProjectedColumns columns) {
    AllRowGroupsParquetReader reader =
        new AllRowGroupsParquetReader(
            context,
            path,
            null,
            fs,
            InputStreamProviderFactory.DEFAULT,
            ParquetReaderFactory.NONE,
            schema,
            columns,
            ParquetFilters.NONE,
            DEFAULT_READER_OPTIONS);

    testCloseables.add(reader);
    return reader;
  }

  private static IcebergTestTables.Table getTable(
      String resourcePath, String source, String location) {
    return getTable(resourcePath, source, "", location);
  }

  private static IcebergTestTables.Table getTable(
      String resourcePath, String source, String sourceRoot, String location) {
    try {
      return new IcebergTestTables.Table(resourcePath, source, sourceRoot, location);
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    }
  }

  public List<String> scanFolders(String tableName) throws IOException {
    ArrayList<String> fileList = new ArrayList<>();
    try (DirectoryStream<java.nio.file.Path> dataFolders =
        Files.newDirectoryStream(Paths.get(getDfsTestTmpSchemaLocation()).resolve(tableName))) {
      for (java.nio.file.Path folder : dataFolders) {
        if (folder.getFileName().endsWith("metadata".toLowerCase())) {
          continue;
        }
        if (folder.toFile().isFile()) {
          if (folder.toFile().getPath().endsWith(".parquet".toLowerCase())) {
            fileList.add(folder.toString());
          }
          continue;
        }
        ArrayList<String> filesInCurrFolder = new ArrayList<>();
        fileList.addAll(scanFiles(folder, filesInCurrFolder));
      }
      return fileList;
    }
  }

  public List<String> scanFiles(java.nio.file.Path folder, List<String> fileList)
      throws IOException {
    try (DirectoryStream<java.nio.file.Path> parquetDataFiles = Files.newDirectoryStream(folder)) {
      for (java.nio.file.Path file : parquetDataFiles) {
        if (file.toFile().isFile() && file.toFile().getPath().endsWith(".parquet".toLowerCase())) {
          fileList.add(file.toString());
        }
      }
      return fileList;
    }
  }

  private OperatorContextImpl getNewOperatorContext(BufferAllocator allocator) {
    return new OperatorContextImpl(
        getSabotContext().getConfig(),
        null, // DremioConfig
        null, // FragmentHandle
        null, // popConfig
        allocator,
        null, // output allocator
        null, // code compiler
        new OperatorStats(new OpProfileDef(0, 0, 0, 0), getAllocator()), // stats
        null, // execution controls
        null, // fragment executor builder
        null, // executor service
        null, // function lookup context
        null, // context information
        getSabotContext().getOptionManager(),
        null, // spill service
        null, // node debug context provider
        1000, // target batch size
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }
}
