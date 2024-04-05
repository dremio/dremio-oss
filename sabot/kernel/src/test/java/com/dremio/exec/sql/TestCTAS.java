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
package com.dremio.exec.sql;

import static com.dremio.TestBuilder.listOf;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergFormatMatcher;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.parquet.SingletonParquetFooterCache;
import com.dremio.io.file.FileSystem;
import com.dremio.test.TemporarySystemProperties;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class TestCTAS extends PlanTestBase {
  private static boolean combineSmallFile;
  private static String expectedDataFile = "0_0_0.parquet";

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void setUp() throws Exception {
    SabotContext context = getSabotContext();
    combineSmallFile =
        context
            .getOptionManager()
            .getOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML);

    // the writer plan fragment id is 0 before the plan change of combining small files
    // after the plan change, the plan fragment id for the first-round writer is 4
    expectedDataFile = combineSmallFile ? "3_0_0.parquet" : "0_0_0.parquet";
  }

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef1() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s AS SELECT region_id, region_id FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "region_id"));
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef2() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "sales_city"));
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef3() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, regionid) "
            + "AS SELECT region_id, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "regionid"));
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef4() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, salescity) "
            + "AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "salescity"));
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef5() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, SalesCity) "
            + "AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "SalesCity"));
  }

  @Test // DRILL-2589
  public void whenInEqualColumnCountInTableDefVsInTableQuery() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) "
            + "AS SELECT region_id, sales_city, sales_region FROM cp.\"region.json\"",
        "table's field list and the table's query field list have different counts.");
  }

  @Test // DRILL-2589
  @Ignore
  public void whenTableQueryColumnHasStarAndTableFiledListIsSpecified() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) "
            + "AS SELECT region_id, * FROM cp.\"region.json\"",
        "table's query field list has a '*', which is invalid when table's field list is specified.");
  }

  @Test // DRILL-2422
  public void createTableWhenATableWithSameNameAlreadyExists() throws Exception {
    final String newTblName = "createTableWhenTableAlreadyExists";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s AS SELECT * from cp.\"region.json\"", TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      errorMsgTestHelper(
          ctasQuery,
          String.format(
              "A table or view with given name [%s.%s] already exists", TEMP_SCHEMA, newTblName));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-2422
  public void createTableWhenAViewWithSameNameAlreadyExists() throws Exception {
    final String newTblName = "createTableWhenAViewWithSameNameAlreadyExists";

    try {
      test(
          String.format(
              "CREATE VIEW %s.%s AS SELECT * from cp.\"region.json\"", TEMP_SCHEMA, newTblName));

      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s AS SELECT * FROM cp.\"employee.json\"", TEMP_SCHEMA, newTblName);

      errorMsgTestHelper(
          ctasQuery,
          String.format(
              "A table or view with given name [%s.%s] already exists", "dfs_test", newTblName));
    } finally {
      test(String.format("DROP VIEW %s.%s", TEMP_SCHEMA, newTblName));
    }
  }

  @Test
  public void ctasPartitionWithEmptyList() throws Exception {
    final String newTblName = "ctasPartitionWithEmptyList";
    final String ctasQuery =
        String.format(
            "CREATE TABLE %s.%s PARTITION BY AS SELECT * from cp.\"region.json\"",
            TEMP_SCHEMA, newTblName);

    try {
      errorTypeTestHelper(ctasQuery, ErrorType.PARSE);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3377
  public void partitionByCtasColList() throws Exception {
    final String newTblName = "partitionByCtasColList";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) "
                  + "AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" group by n_regionkey",
              TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable =
          String.format(" select cnt, rkey from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery =
          "select count(*) as cnt, n_regionkey as rkey from cp.\"tpch/nation.parquet\" group by n_regionkey";
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DX - 16118
  public void testParquetComplexWithNull() throws Exception {
    final String newTblName = "parquetComplexWithNull";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s AS SELECT index from "
                  + "dfs.\"${WORKING_PATH}/src/test/resources/complex_with_null.parquet\" where play_name is not null",
              TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable =
          String.format(
              "select count(*) as cnt from %s.%s where index is null", TEMP_SCHEMA, newTblName);
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(111396L)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3374
  public void partitionByCtasFromView() throws Exception {
    final String newTblName = "partitionByCtasColList2";
    final String newView = "partitionByCtasColListView";
    try {
      final String viewCreate =
          String.format(
              "create or replace view %s.%s (col_int, col_varchar)  "
                  + "AS select cast(n_nationkey as int), cast(n_name as varchar(30)) from cp.\"tpch/nation.parquet\"",
              TEMP_SCHEMA, newView);

      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (col_int) AS SELECT * from %s.%s",
              TEMP_SCHEMA, newTblName, TEMP_SCHEMA, newView);

      test(viewCreate);
      test(ctasQuery);

      final String baselineQuery =
          "select cast(n_nationkey as int) as col_int, cast(n_name as varchar(30)) as col_varchar "
              + "from cp.\"tpch/nation.parquet\"";
      final String selectFromCreatedTable =
          String.format("select col_int, col_varchar from %s.%s", TEMP_SCHEMA, newTblName);
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();

      final String viewDrop = String.format("DROP VIEW %s.%s", TEMP_SCHEMA, newView);
      test(viewDrop);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3382
  public void ctasWithQueryOrderby() throws Exception {
    final String newTblName = "ctasWithQueryOrderby";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s   "
                  + "AS SELECT n_nationkey, n_name, n_comment from cp.\"tpch/nation.parquet\" order by n_nationkey",
              TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable =
          String.format(
              " select n_nationkey, n_name, n_comment from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery =
          "select n_nationkey, n_name, n_comment from cp.\"tpch/nation.parquet\" order by n_nationkey";

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-4392
  public void ctasWithPartition() throws Exception {
    final String newTblName = "nation_ctas";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s   "
                  + "partition by (n_regionkey) AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" order by n_nationkey limit 1",
              TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable =
          String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName);

      test(String.format("select * from %s.%s", TEMP_SCHEMA, newTblName));

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .baselineColumns("dir0", "n_nationkey", "n_regionkey")
          .baselineValues("0_0", 0, 0)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasFailures() throws Exception {
    String inputTable = "ctasFailureInput";
    String fileName = "f.json";
    File directory = new File(getDfsTestTmpSchemaLocation(), inputTable);
    directory.mkdir();
    PrintStream ps = new PrintStream(new File(directory, fileName));

    for (int i = 0; i < 100_000; i++) {
      ps.println("{ a : 1 }");
    }
    ps.println("{ a : ");
    ps.close();

    try {
      test("create table dfs_test.ctasFailure as select * from dfs_test." + inputTable);
    } catch (Exception e) {
      // no op
    }

    String outputTable = "ctasFailure";

    assertFalse(new File(getDfsTestTmpSchemaLocation(), outputTable).exists());
  }

  @Test
  public void testCTASLegacyTableFormatGetQueryIdFails() throws Exception {
    final String newTblName = "LegacyTblPartitionByCtasColList";

    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) "
                  + "STORE AS (type => 'parquet') AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" "
                  + "group by n_regionkey",
              TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      File[] filesInTableDirectory =
          new File(getDfsTestTmpSchemaLocation(), newTblName).listFiles();
      Assert.assertNotNull(filesInTableDirectory);
      Assert.assertEquals(1, filesInTableDirectory.length);

      try {
        QueryIdHelper.getQueryIdFromString(filesInTableDirectory[0].getName());
      } catch (IllegalArgumentException ex) {
        Assert.assertTrue(ex.getMessage().startsWith("Invalid UUID string"));
      }

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  // gets query ID folder after first write to the table
  // it works if there is only one query ID folder along with metadata folder
  private File getQueryIDFolderAfterFirstWrite(File tableFolder) {
    final File[] dataFolder = new File[1];
    Assert.assertEquals(
        2,
        Objects.requireNonNull(
                tableFolder.listFiles(
                    new FileFilter() {
                      @Override
                      public boolean accept(File pathname) {
                        if (!pathname.isDirectory()) {
                          return false;
                        }
                        if (pathname.getName().equals("metadata")) {
                          return true;
                        }
                        dataFolder[0] = pathname;
                        return true;
                      }
                    }))
            .length);
    return dataFolder[0];
  }

  private void verifyFieldHasColumnId(Type field) {
    System.out.println("Verifying column " + field.getName());
    assertTrue("Field " + field.getName() + " does not have column id", field.getId() != null);
    if (field instanceof GroupType) {
      GroupType groupType = (GroupType) field;
      if (groupType.getOriginalType() == OriginalType.LIST) {
        groupType = groupType.getFields().get(0).asGroupType();
      }
      for (Type child : groupType.getFields()) {
        verifyFieldHasColumnId(child);
      }
    }
  }

  private void verifyParquetFileHasColumnIds(File parquetFile) throws Exception {
    FileSystem fs = HadoopFileSystem.get(parquetFile.toURI(), new Configuration(), false);
    ParquetMetadata footer =
        SingletonParquetFooterCache.readFooter(
            fs,
            com.dremio.io.file.Path.of(parquetFile.toURI()),
            ParquetMetadataConverter.NO_FILTER,
            Long.MAX_VALUE);
    for (Type field : footer.getFileMetaData().getSchema().getFields()) {
      verifyFieldHasColumnId(field);
    }
  }

  private void testCtasSimpleTransactionalTable(final String newTblName, String schema)
      throws Exception {
    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
              schema, newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      File parquetDataFile = new File(dataFolder, expectedDataFile);
      assertTrue(parquetDataFile.exists()); // parquet data file
      verifyParquetFileHasColumnIds(parquetDataFile);

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          2,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro");
                        }
                      }))
              .length); // manifest list and manifest files

      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", schema, newTblName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasSimpleTransactionalTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "nation_ctas_tt";
      testCtasSimpleTransactionalTable("nation_ctas_tt_v2", TEMP_SCHEMA_HADOOP);
    }
  }

  @Test
  public void ctasTableWithComplexTypes() throws Exception {
    final String newTblName = "complex_ctas_tt";
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/complexJson";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  " + " AS SELECT * from dfs.\"" + parquetFiles + "\"",
              TEMP_SCHEMA_HADOOP,
              newTblName);

      test(ctasQuery);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);
      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      File parquetDataFile = new File(dataFolder, expectedDataFile);
      assertTrue(parquetDataFile.exists()); // parquet data file
      verifyParquetFileHasColumnIds(parquetDataFile);
    } finally {
      FileUtils.deleteQuietly(tableFolder);
    }
  }

  private void testCtasMultiSplitTransactionalTable(
      String newTblName, String dfsSchema, String testSchema) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/supplier";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT * from "
                  + dfsSchema
                  + ".\""
                  + parquetFiles
                  + "\"",
              testSchema,
              newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      // writers are in different phase

      // without combineSmallFile, there are two 8mb data files generated
      // with combineSmallFile, above two 8mb data files are combined into one 16mb data file.
      // After orphan files removal, there are 1 data file in the folder.
      int expectedDataFiles = combineSmallFile ? 1 : 2;
      assertEquals(
          expectedDataFiles,
          dataFolder.listFiles(
                  new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                      return name.endsWith(".parquet");
                    }
                  })
              .length);

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(
          Objects.requireNonNull(
                      metadataFolder.listFiles(
                          new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                              return name.endsWith(".avro");
                            }
                          }))
                  .length
              >= 1); // manifest list and manifest files
      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", testSchema, newTblName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(200000L)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasMultiSplitTransactionalTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "supplier_ctas_multisplit_tt";
      testCtasMultiSplitTransactionalTable(
          "supplier_ctas_multisplit_tt_new", "dfs_hadoop", TEMP_SCHEMA_HADOOP);
    }
  }

  private void testInsertSimpleTransactionalTable(String newTblName, String schema)
      throws Exception {
    try {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
              schema, newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);
      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      assertTrue(new File(dataFolder, expectedDataFile).exists()); // parquet data file

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          2,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro");
                        }
                      }))
              .length); // manifest list and manifest files

      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", schema, newTblName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();

      final String insertQuery =
          String.format(
              "INSERT INTO %s.%s SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
              schema, newTblName);
      test(insertQuery);

      assertEquals(
          2,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith("metadata.json");
                        }
                      }))
              .length);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void insertSimpleTransactionalTable() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "nation_insert_tt";
      testInsertSimpleTransactionalTable("nation_insert_tt_new", TEMP_SCHEMA_HADOOP);
    }
  }

  @Test
  public void testCaseSensitiveCTASTransactionalTable() throws Exception {
    final String newTblName = "case_sensitive_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String complexJson = testWorkingPath + "/src/test/resources/iceberg/complexJson";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT name, flatten(likes) from dfs_hadoop.\""
                  + complexJson
                  + "\"",
              TEMP_SCHEMA,
              newTblName);

      test(ctasQuery);
      Thread.sleep(1001);

      testBuilder()
          .sqlQuery(String.format("select *  from %s.%s", TEMP_SCHEMA, newTblName))
          .unOrdered()
          .baselineColumns("name", "EXPR$1")
          .baselineValues("John", "Pasta")
          .baselineValues("John", "Pizza")
          .baselineValues("John", "Noodles")
          .build()
          .run();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void insertComplexTransactionalTable() throws Exception {
    final String newTblName = "complex_insert_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String complexJson = testWorkingPath + "/src/test/resources/iceberg/complexJson";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  " + " AS SELECT * from dfs_hadoop.\"" + complexJson + "\"",
              TEMP_SCHEMA_HADOOP,
              newTblName);

      test(ctasQuery);
      Thread.sleep(1001);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      assertTrue(new File(dataFolder, expectedDataFile).exists()); // parquet data file

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
      assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
      assertEquals(
          2,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro");
                        }
                      }))
              .length); // manifest list and manifest files

      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA_HADOOP, newTblName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .build()
          .run();

      final String insertQuery =
          String.format(
              "INSERT INTO %s.%s  " + " SELECT * from dfs_hadoop.\"" + complexJson + "\"",
              TEMP_SCHEMA_HADOOP,
              newTblName);
      test(insertQuery);
      Thread.sleep(1001);

      assertTrue(new File(metadataFolder, "v2.metadata.json").exists());

      testBuilder()
          .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA_HADOOP, newTblName))
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(2L)
          .build()
          .run();

      FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();

      IcebergHadoopModel icebergHadoopModel = new IcebergHadoopModel(fileSystemPlugin);
      when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergHadoopModel);
      Table table =
          icebergHadoopModel.getIcebergTable(
              icebergHadoopModel.getTableIdentifier(tableFolder.toString()));
      SchemaConverter schemaConverter =
          SchemaConverter.getBuilder().setTableName(table.name()).build();
      BatchSchema icebergSchema = schemaConverter.fromIceberg(table.schema());
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      schemaBuilder.addField(CompleteType.VARCHAR.toField("name"));
      schemaBuilder.addField(
          CompleteType.struct(
                  CompleteType.BIGINT.toField("age"), CompleteType.VARCHAR.toField("gender"))
              .toField("info"));
      schemaBuilder.addField(
          CompleteType.struct(
                  new Field(
                      "arr",
                      new FieldType(true, Types.MinorType.LIST.getType(), null),
                      Collections.singletonList(CompleteType.BIGINT.toField("$data$"))),
                  new Field(
                      "strArr",
                      new FieldType(true, Types.MinorType.LIST.getType(), null),
                      Collections.singletonList(CompleteType.VARCHAR.toField("$data$"))))
              .toField("structWithArray"));

      schemaBuilder.addField(
          CompleteType.struct(
                  CompleteType.struct(
                          CompleteType.BIGINT.toField("num"),
                          new Field(
                              "lis",
                              FieldType.nullable(Types.MinorType.LIST.getType()),
                              Collections.singletonList(CompleteType.BIGINT.toField("$data$"))),
                          CompleteType.struct(CompleteType.BIGINT.toField("num2")).toField("struc"))
                      .toField("innerStruct"))
              .toField("structWithStruct"));
      schemaBuilder.addField(
          new Field(
              "likes",
              new FieldType(true, Types.MinorType.LIST.getType(), null),
              Collections.singletonList(CompleteType.VARCHAR.toField("$data$"))));
      Field structChild =
          CompleteType.struct(
                  CompleteType.VARCHAR.toField("name"),
                  CompleteType.VARCHAR.toField("gender"),
                  CompleteType.BIGINT.toField("age"))
              .toField("$data$");

      schemaBuilder.addField(
          new Field(
              "children",
              new FieldType(true, Types.MinorType.LIST.getType(), null),
              Collections.singletonList(structChild)));

      Field listChild =
          new Field(
              "$data$",
              new FieldType(true, Types.MinorType.LIST.getType(), null),
              Collections.singletonList(CompleteType.BIGINT.toField("$data$")));

      schemaBuilder.addField(
          new Field(
              "matrix",
              new FieldType(true, Types.MinorType.LIST.getType(), null),
              Collections.singletonList(listChild)));

      assertEquals(schemaBuilder.build(), icebergSchema);

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testCTASIcebergTableFormat() throws Exception {
    final String newTblName = "IcebergTblPartitionByCtasColList";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) "
                  + " AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" "
                  + "group by n_regionkey",
              TEMP_SCHEMA_HADOOP, newTblName);

      test(ctasQuery);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      File[] filesInTableDirectory = tableFolder.listFiles();
      Assert.assertNotNull(filesInTableDirectory);
      // table folder contains metadata folder and queryId folder
      File queryIDFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      // This should throw exception if the directory name is not a queryId
      QueryIdHelper.getQueryIdFromString(queryIDFolder.getName());

      final String selectFromCreatedTable =
          String.format(" select cnt, rkey from %s.%s", TEMP_SCHEMA_HADOOP, newTblName);
      final String baselineQuery =
          "select count(*) as cnt, n_regionkey as rkey from cp.\"tpch/nation.parquet\" group by n_regionkey";
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testSchemaValidationsForInsertDifferentNames() throws Exception {

    final String intTable1 = "int1";
    final String intTable2 = "int2";

    try (AutoCloseable c = enableIcebergTables()) {
      final String intQuery1 = String.format("CREATE TABLE %s.%s(id int)", TEMP_SCHEMA, intTable1);
      test(intQuery1);

      final String intQuery2 =
          String.format("CREATE TABLE %s.%s(n_nationkey int)", TEMP_SCHEMA, intTable2);
      test(intQuery2);

      final String insertQuery =
          String.format(
              "INSERT INTO %s.%s SELECT n_nationkey from %s.%s",
              TEMP_SCHEMA, intTable1, TEMP_SCHEMA, intTable2);

      test(insertQuery);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), intTable1));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), intTable2));
    }
  }

  @Test
  public void testSchemaValidationsForInsertDifferentTypes() throws Exception {

    final String intString = "int_string";
    final String intInt = "int_int";

    try (AutoCloseable c = enableIcebergTables()) {
      final String intStringQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, intString);
      test(intStringQuery);

      final String intIntQuery =
          String.format(
              "CREATE TABLE %s.%s(n_nationkey int, n_regionkey int)", TEMP_SCHEMA_HADOOP, intInt);
      test(intIntQuery);

      final String intIntInToIntString =
          String.format(
              "INSERT INTO %s.%s SELECT n_nationkey, n_regionkey n_name from %s.%s",
              TEMP_SCHEMA_HADOOP, intString, TEMP_SCHEMA_HADOOP, intInt);

      ctasErrorTestHelper(
          intIntInToIntString,
          "Table schema(n_nationkey::int32, n_name::varchar) "
              + "doesn't match with query schema(n_nationkey::int32, n_regionkey::int32)");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), intInt));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), intString));
    }
  }

  @Test
  public void testSchemaValidationsForInsertDecimals() throws Exception {

    final String decimal1 = "scale3";
    final String decimal2 = "prec1";

    try (AutoCloseable c = enableIcebergTables()) {
      final String decimal1Query =
          String.format("CREATE TABLE %s.%s(id int, code decimal(18, 3))", TEMP_SCHEMA, decimal1);
      test(decimal1Query);

      final String decimal2Query =
          String.format("CREATE TABLE %s.%s(id int, code decimal(18, 1))", TEMP_SCHEMA, decimal2);
      test(decimal2Query);

      final String decimal2IntoDecimal1 =
          String.format(
              "INSERT INTO %s.%s SELECT id, code from %s.%s",
              TEMP_SCHEMA, decimal1, TEMP_SCHEMA, decimal2);

      ctasErrorTestHelper(
          decimal2IntoDecimal1,
          "Table schema(id::int32, code::decimal(18,3)) doesn't match "
              + "with query schema(id::int32, code::decimal(18,1))");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), decimal1));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), decimal2));
    }
  }

  @Test
  public void TestNewSnapshotOnEmptyDataAppend() throws Exception {
    final String newTblName = "ctas_with_data";
    final String emptyTblName = "empty_table";
    final String ctasEmptyTblName = "ctas_empty_table";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, newTblName);

      final String createEmptyTableQuery =
          String.format(
              "CREATE TABLE %s.%s(n_nationkey int, n_regionkey int)",
              TEMP_SCHEMA_HADOOP, emptyTblName);

      test(createEmptyTableQuery);
      Path emptyTableDirectoryPath = new File(getDfsTestTmpSchemaLocation(), emptyTblName).toPath();
      File emptyTableMetadataDirectory =
          new File(emptyTableDirectoryPath.resolve(IcebergFormatMatcher.METADATA_DIR_NAME).toUri());
      Assert.assertNotNull(emptyTableMetadataDirectory);
      // snapshot on create empty table
      assertEquals(
          1,
          Objects.requireNonNull(
                  emptyTableMetadataDirectory.listFiles((dir, name) -> name.startsWith("snap")))
              .length);

      test(ctasQuery);
      Path ctasPath = new File(getDfsTestTmpSchemaLocation(), newTblName).toPath();
      File ctasTableMetadataDirectory =
          new File(ctasPath.resolve(IcebergFormatMatcher.METADATA_DIR_NAME).toUri());
      Assert.assertNotNull(ctasTableMetadataDirectory);
      // snapshot on ctas
      assertEquals(
          1,
          Objects.requireNonNull(
                  ctasTableMetadataDirectory.listFiles((dir, name) -> name.startsWith("snap")))
              .length);

      // ctas empty data
      final String insertQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 0",
              TEMP_SCHEMA_HADOOP, ctasEmptyTblName);
      test(insertQuery);
      // // snapshot on ctas empty data
      assertEquals(
          1,
          Objects.requireNonNull(
                  ctasTableMetadataDirectory.listFiles((dir, name) -> name.startsWith("snap")))
              .length);

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), emptyTblName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), ctasEmptyTblName));
    }
  }

  private static void ctasErrorTestHelper(final String ctasSql, final String expErrorMsg)
      throws Exception {
    final String createTableSql = String.format(ctasSql, TEMP_SCHEMA, "testTableName");
    errorMsgTestHelper(createTableSql, expErrorMsg);
  }

  @Test
  public void testCreateTableCommandInvalidPath() throws Exception {
    final String tblName = "invalid_path_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String createTableQuery = String.format("CREATE TABLE %s(id int, code int)", tblName);
      UserExceptionAssert.assertThatThrownBy(() -> test(createTableQuery))
          .hasMessageContaining(
              String.format("Invalid path. Given path, [%s] is not valid.", tblName));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCaseSensitiveDateColumn() throws Exception {
    final String caseSensitiveTest = "case_sensitive_date_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql =
          "create table "
              + TEMP_SCHEMA
              + "."
              + caseSensitiveTest
              + " as "
              + "select * from (values (to_date(cast('2020-01-09' as date) + interval '1' day )))";
      test(createCommandSql);
      DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss Z");
      DateTime expectedDate = DateTime.parse("10/01/2020 00:00:00 +00:00", dateTimeFormatter);
      String selectCommand = "select * from " + TEMP_SCHEMA + "." + caseSensitiveTest;
      testBuilder()
          .sqlQuery(selectCommand)
          .unOrdered()
          .baselineColumns("EXPR$0")
          .baselineValues(new LocalDateTime(expectedDate.getMillis(), UTC))
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), caseSensitiveTest));
    }
  }

  @Test
  public void testIncorrectCatalog() throws Exception {
    final String newTblName = "test_incorrect_iceberg_catalog";
    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, newTblName);
      test(ctasQuery);
      // Try with wrong catalog (TEMP_SCHEMA is configured to use Nessie catalog)
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      IcebergModel icebergModel = getIcebergModel(TEMP_SCHEMA);
      UserExceptionAssert.assertThatThrownBy(
              () ->
                  icebergModel.getIcebergTable(
                      icebergModel.getTableIdentifier(tableFolder.getPath())))
          .hasMessageContaining("Failed to load the Iceberg table.");
    }
  }

  @Test
  public void testManifestWriter() throws Exception {

    final String icebergTable = "icebergTable";

    try (AutoCloseable c = enableIcebergTables()) {
      final String query =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, icebergTable);
      test(query);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), icebergTable);
      assertTrue(tableFolder.exists());

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertEquals(
          1,
          Objects.requireNonNull(
                  metadataFolder.listFiles(
                      new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                          return name.endsWith(".avro") && name.length() == 32 + 4 + 5;
                        }
                      }))
              .length);
      // Manifest file example f22926dd-04c9-4d11-ab3f-23adc25e1959.avro
      // Format : UUID.avro  so 32(uuid) + 4(dash) + 5(.avro extension)
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), icebergTable));
    }
  }

  // test to check if CTAS command supports storage option as iceberg
  @Test
  public void testCTASCreateIcebergTableWithStorageoption() throws Exception {

    final String icebergTable = "icebergTableCTASStorageOption";

    try (AutoCloseable c = enableIcebergTables()) {
      final String query =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " STORE AS (type => 'iceberg') AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA, icebergTable);
      test(query);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), icebergTable);
      assertTrue(tableFolder.exists());

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists());
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), icebergTable));
    }
  }

  // TODO: Revert with DX-48616
  /*@Test
  public void testOutputColumnsForCreateIcebergTableWithStorageoption() throws Exception {

    final String icebergTable = "outputSchemaIcebergTableCTASStorageOption";

    try (AutoCloseable c = enableIcebergTables()) {
      final String query = String.format("CREATE TABLE %s.%s  " +
          " STORE AS (type => 'iceberg') AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
        TEMP_SCHEMA, icebergTable);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(RecordWriter.RECORDS_COLUMN)
        .baselineValues(1L)
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), icebergTable));
    }
  }*/

  // test to check if CTAS command creates the table in default format when storage option not
  // provided
  @Test
  public void testCTASDefaultStorageOption() throws Exception {

    final String testCtasDefaultTable = "testCtasDefault";

    try (AutoCloseable c = enableIcebergTables()) {
      final String query =
          String.format(
              "CREATE TABLE %s.%s  "
                  + "AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
              TEMP_SCHEMA_HADOOP, testCtasDefaultTable);
      test(query);
      String format = getDfsTestTmpDefaultCtasFormat(TEMP_SCHEMA_HADOOP);
      assertEquals("iceberg", format);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), testCtasDefaultTable);
      assertTrue(tableFolder.exists());
      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists());
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), testCtasDefaultTable));
    }
  }

  @Test
  public void testCTASCreateIcebergWithMapColumn() throws Exception {
    final String icebergTable = "icebergWithMapColumn";

    try (AutoCloseable c = enableIcebergTables();
        AutoCloseable c2 = enableMapDataType()) {
      final String query =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " STORE AS (type => 'iceberg') AS SELECT * from cp.\"parquet/map_data_types/mapcolumn.parquet\"",
              TEMP_SCHEMA, icebergTable);
      test(query);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), icebergTable);
      assertTrue(tableFolder.exists());

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists());

      final String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, icebergTable);
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<>();
      JsonStringHashMap<String, Object> structrow2 = new JsonStringHashMap<>();
      structrow1.put("key", new Text("b"));
      structrow1.put("value", new Text("bb"));
      structrow2.put("key", new Text("c"));
      structrow2.put("value", new Text("cc"));
      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(2, listOf(structrow1), "def")
          .baselineValues(12, listOf(structrow2), "abc")
          .go();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), icebergTable));
    }
  }

  @Test
  public void testCTASCreateParquetWithMapColumn() throws Exception {
    final String parquetTable = "parquetWithMapColumn";

    try (AutoCloseable c = enableIcebergTables();
        AutoCloseable c2 = enableMapDataType()) {
      final String query =
          String.format(
              "CREATE TABLE %s.%s  "
                  + " STORE AS (type => 'parquet') AS SELECT * from cp.\"parquet/map_data_types/mapcolumn.parquet\"",
              TEMP_SCHEMA, parquetTable);
      test(query);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), parquetTable);
      assertTrue(tableFolder.exists());

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(!metadataFolder.exists());

      final String selectQuery = String.format("SELECT * FROM %s.%s", TEMP_SCHEMA, parquetTable);
      JsonStringHashMap<String, Object> structrow1 = new JsonStringHashMap<>();
      JsonStringHashMap<String, Object> structrow2 = new JsonStringHashMap<>();
      structrow1.put("key", new Text("b"));
      structrow1.put("value", new Text("bb"));
      structrow2.put("key", new Text("c"));
      structrow2.put("value", new Text("cc"));
      testBuilder()
          .sqlQuery(selectQuery)
          .unOrdered()
          .baselineColumns("col1", "col2", "col3")
          .baselineValues(2, listOf(structrow1), "def")
          .baselineValues(12, listOf(structrow2), "abc")
          .go();

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), parquetTable));
    }
  }

  // DX-65794
  // Derived column names from CTAS should be maintained when the table
  // is created, even when there is an implicit cast on one of the columns.
  // We do this by using the validated row type field names when the CTAS
  // does not specify explicit column names.
  @Test
  public void testColumnNamesMaintained() throws Exception {
    final String col = "col";
    final String tablePrefix = "TestColumnNamesMaintained";
    final String tableA = String.format("%s.%sA", TEMP_SCHEMA, tablePrefix);
    final String tableB = String.format("%s.%sB", TEMP_SCHEMA, tablePrefix);
    final String tableC = String.format("%s.%sC", TEMP_SCHEMA, tablePrefix);

    try {
      // Create Table A.
      final String createA =
          String.format(
              "create table %s (%s int) as " + "select * from (values (1), (2))", tableA, col);
      runSQL(createA);

      // Create table B.
      final String createB =
          String.format(
              "create table %s (%s varchar) as " + "select * from (values ('3'), ('4'))",
              tableB, col);
      runSQL(createB);

      // Create table C from union of tables A and B.
      // Column type is inconsistent between tables,
      // so an implicit cast will be applied to one side
      // of the union. The column name of new table
      // should be maintained.
      final String createC =
          String.format(
              "create table %s as " + "(select %s from %s " + "union all " + "select %s from %s)",
              tableC, col, tableA, col, tableB);
      runSQL(createC);

      final SchemaPath schemaPath = SchemaPath.getSimplePath(col);
      final TypeProtos.MajorType type =
          com.dremio.common.types.Types.optional(TypeProtos.MinorType.VARCHAR);
      final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema =
          Lists.newArrayList(Pair.of(schemaPath, type));

      // Query table C and verify column name.
      final String query = String.format("select * from %s", tableC);
      testBuilder().sqlQuery(query).schemaBaseLine(expectedSchema).go();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableA));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableB));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableC));
    }
  }

  @Test
  public void testCTASRoundRobin() throws Exception {
    String table = "round_robin";
    String sql =
        String.format(
            "create table dfs_test.%s as select * from cp.csv.\"nationsWithCapitals.csv\"", table);
    try (AutoCloseable ac = withOption(ExecConstants.SLICE_TARGET_OPTION, 1);
        AutoCloseable ac0 = withOption(PlannerSettings.CTAS_ROUND_ROBIN, true);
        AutoCloseable ac1 = withOption(ExecConstants.TARGET_BATCH_RECORDS_MIN, 1);
        AutoCloseable ac2 = withOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, 2)) {
      test(sql);
    }

    File tableFolder = new File(getDfsTestTmpSchemaLocation(), table);
    // Check that more than one file was created (ignoring the .crc files)
    Assert.assertTrue(
        Objects.requireNonNull(tableFolder.listFiles((dir, name) -> !name.contains("crc"))).length
            > 1);
  }
}
