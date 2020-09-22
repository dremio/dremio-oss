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

import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.iceberg.IcebergFormatMatcher;
import com.dremio.exec.store.iceberg.IcebergTableOperations;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.parquet.SingletonParquetFooterCache;
import com.dremio.io.file.FileSystem;
import com.dremio.test.TemporarySystemProperties;

public class TestCTAS extends PlanTestBase {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef1() throws Exception {
    ctasErrorTestHelper("CREATE TABLE %s.%s AS SELECT region_id, region_id FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "region_id")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef2() throws Exception {
    ctasErrorTestHelper("CREATE TABLE %s.%s AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "sales_city")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef3() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, regionid) " +
            "AS SELECT region_id, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "regionid")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef4() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, salescity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "salescity")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef5() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, SalesCity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.\"region.json\"",
        String.format("Duplicate column name [%s]", "SalesCity")
    );
  }

  @Test // DRILL-2589
  public void whenInEqualColumnCountInTableDefVsInTableQuery() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) " +
            "AS SELECT region_id, sales_city, sales_region FROM cp.\"region.json\"",
        "table's field list and the table's query field list have different counts."
    );
  }

  @Test // DRILL-2589
  @Ignore
  public void whenTableQueryColumnHasStarAndTableFiledListIsSpecified() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) " +
            "AS SELECT region_id, * FROM cp.\"region.json\"",
        "table's query field list has a '*', which is invalid when table's field list is specified."
    );
  }

  @Test // DRILL-2422
  public void createTableWhenATableWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTableWhenTableAlreadyExists";

    try {
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s AS SELECT * from cp.\"region.json\"", TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      errorMsgTestHelper(ctasQuery,
          String.format("A table or view with given name [%s.%s] already exists", TEMP_SCHEMA, newTblName));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-2422
  public void createTableWhenAViewWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTableWhenAViewWithSameNameAlreadyExists";

    try {
      test(String.format("CREATE VIEW %s.%s AS SELECT * from cp.\"region.json\"", TEMP_SCHEMA, newTblName));

      final String ctasQuery =
          String.format("CREATE TABLE %s.%s AS SELECT * FROM cp.\"employee.json\"", TEMP_SCHEMA, newTblName);

      errorMsgTestHelper(ctasQuery,
          String.format("A table or view with given name [%s.%s] already exists",
              "dfs_test", newTblName));
    } finally {
      test(String.format("DROP VIEW %s.%s", TEMP_SCHEMA, newTblName));
    }
  }

  @Test
  public void ctasPartitionWithEmptyList() throws Exception {
    final String newTblName = "ctasPartitionWithEmptyList";
    final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY AS SELECT * from cp.\"region.json\"", TEMP_SCHEMA, newTblName);

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
      final String ctasQuery = String.format("CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) " +
          "AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" group by n_regionkey",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select cnt, rkey from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select count(*) as cnt, n_regionkey as rkey from cp.\"tpch/nation.parquet\" group by n_regionkey";
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
      final String ctasQuery = String.format("CREATE TABLE %s.%s AS SELECT index from " +
          "dfs.\"${WORKING_PATH}/src/test/resources/complex_with_null.parquet\" where play_name is not null",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format("select count(*) as cnt from %s.%s where index is null", TEMP_SCHEMA, newTblName);
      testBuilder()
        .sqlQuery(selectFromCreatedTable)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(111396l)
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
      final String viewCreate = String.format("create or replace view %s.%s (col_int, col_varchar)  " +
          "AS select cast(n_nationkey as int), cast(n_name as varchar(30)) from cp.\"tpch/nation.parquet\"",
          TEMP_SCHEMA, newView);

      final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (col_int) AS SELECT * from %s.%s",
          TEMP_SCHEMA, newTblName, TEMP_SCHEMA, newView);

      test(viewCreate);
      test(ctasQuery);

      final String baselineQuery = "select cast(n_nationkey as int) as col_int, cast(n_name as varchar(30)) as col_varchar " +
        "from cp.\"tpch/nation.parquet\"";
      final String selectFromCreatedTable = String.format("select col_int, col_varchar from %s.%s", TEMP_SCHEMA, newTblName);
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
      final String ctasQuery = String.format("CREATE TABLE %s.%s   " +
          "AS SELECT n_nationkey, n_name, n_comment from cp.\"tpch/nation.parquet\" order by n_nationkey",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select n_nationkey, n_name, n_comment from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select n_nationkey, n_name, n_comment from cp.\"tpch/nation.parquet\" order by n_nationkey";

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
      final String ctasQuery = String.format("CREATE TABLE %s.%s   " +
          "partition by (n_regionkey) AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" order by n_nationkey limit 1",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName);

      test(String.format("select * from %s.%s", TEMP_SCHEMA, newTblName));

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .baselineColumns("dir0", "n_nationkey", "n_regionkey")
          .baselineValues("0", 0, 0)
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
      final String ctasQuery = String.format("CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) " +
          "STORE AS (type => 'parquet') AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" " +
          "group by n_regionkey",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      File[] filesInTableDirectory = new File(getDfsTestTmpSchemaLocation(), newTblName).listFiles();
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
    Assert.assertEquals(2, Objects.requireNonNull(tableFolder.listFiles(new FileFilter() {
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
    })).length);
    return dataFolder[0];
  }

  private void verifyFieldHasColumnId(Type field) {
    System.out.println("Verifying column " + field.getName());
    assertTrue("Field " + field.getName() + " does not have column id", field.getId() != null);
    if (field instanceof GroupType) {
      GroupType groupType = (GroupType)field;
      if (groupType.getOriginalType() == OriginalType.LIST) {
        groupType = groupType.getFields().get(0).asGroupType();
      }
      for(Type child : groupType.getFields()) {
        verifyFieldHasColumnId(child);
      }
    }
  }

  private void verifyParquetFileHasColumnIds(File parquetFile) throws Exception {
    FileSystem fs = HadoopFileSystem.get(parquetFile.toURI(), new Configuration(), false);
    ParquetMetadata footer = SingletonParquetFooterCache.readFooter(fs, com.dremio.io.file.Path.of(parquetFile.toURI()), ParquetMetadataConverter.NO_FILTER, Long.MAX_VALUE);
    for(Type field : footer.getFileMetaData().getSchema().getFields()) {
      verifyFieldHasColumnId(field);
    }
  }

  @Test
  public void ctasSimpleTransactionalTable() throws Exception {
    final String newTblName = "nation_ctas_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      File parquetDataFile = new File(dataFolder, "0_0_0.parquet");
      assertTrue(parquetDataFile.exists()); // parquet data file
      verifyParquetFileHasColumnIds(parquetDataFile);

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
      assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
      assertEquals(2, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro");
        }
      })).length); // manifest list and manifest files

      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, newTblName))
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
  public void ctasTableWithComplexTypes() throws Exception {
    final String newTblName = "complex_ctas_tt";
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/complexJson";
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT * from dfs.\"" + parquetFiles + "\"",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      File parquetDataFile = new File(dataFolder, "0_0_0.parquet");
      assertTrue(parquetDataFile.exists()); // parquet data file
      verifyParquetFileHasColumnIds(parquetDataFile);
    } finally {
       FileUtils.deleteQuietly(tableFolder);
    }
  }

  @Test
  public void ctasMultiSplitTransactionalTable() throws Exception {
    final String newTblName = "supplier_ctas_multisplit_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/supplier";
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT * from dfs.\"" + parquetFiles +"\"",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      //writers are in different phase
      assertTrue(new File(dataFolder, "1_0_0.parquet").exists()); // parquet data file
      assertTrue(new File(dataFolder, "1_1_0.parquet").exists()); // parquet data file

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
      assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
      assertEquals(2, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro");
        }
      })).length); // manifest list and manifest files

      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, newTblName))
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
  public void insertSimpleTransactionalTable() throws Exception {
    final String newTblName = "nation_insert_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      assertTrue(new File(dataFolder, "0_0_0.parquet").exists()); // parquet data file

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
      assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
      assertEquals(2, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro");
        }
      })).length); // manifest list and manifest files

      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, newTblName))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();

      final String insertQuery = String.format("INSERT INTO %s.%s SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1", TEMP_SCHEMA, newTblName);
      test(insertQuery);

      assertTrue(new File(metadataFolder, "v2.metadata.json").exists());


    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testCaseSensitiveCTASTransactionalTable() throws Exception {
    final String newTblName = "case_sensitive_tt";

    try (AutoCloseable c = enableIcebergTables()) {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String complexJson = testWorkingPath + "/src/test/resources/iceberg/complexJson";
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT name, flatten(likes) from dfs.\"" + complexJson +"\"",
        TEMP_SCHEMA, newTblName);

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
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT * from dfs.\"" + complexJson +"\"",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);
      Thread.sleep(1001);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder

      final File dataFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      assertTrue(dataFolder.exists()); // query ID folder, which contains data
      assertTrue(new File(dataFolder, "0_0_0.parquet").exists()); // parquet data file

      File metadataFolder = new File(tableFolder, "metadata");
      assertTrue(metadataFolder.exists()); // metadata folder
      assertTrue(new File(metadataFolder, "v1.metadata.json").exists()); // snapshot metadata
      assertTrue(new File(metadataFolder, "version-hint.text").exists()); // root pointer file
      assertEquals(2, Objects.requireNonNull(metadataFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".avro");
        }
      })).length); // manifest list and manifest files

      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, newTblName))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(1L)
        .build()
        .run();

      final String insertQuery = String.format("INSERT INTO %s.%s  " +
          " SELECT * from dfs.\"" + complexJson +"\"",
        TEMP_SCHEMA, newTblName);
      test(insertQuery);
      Thread.sleep(1001);

      assertTrue(new File(metadataFolder, "v2.metadata.json").exists());

      testBuilder()
        .sqlQuery(String.format("select count(*) c from %s.%s", TEMP_SCHEMA, newTblName))
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(2L)
        .build()
        .run();

      IcebergTableOperations tableOperations = new IcebergTableOperations(
        new org.apache.hadoop.fs.Path(tableFolder.toString()), new Configuration());
      BatchSchema icebergSchema = new SchemaConverter().fromIceberg(
        tableOperations.current().schema());
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      schemaBuilder.addField(CompleteType.VARCHAR.toField("name"));
      schemaBuilder.addField(CompleteType.struct(CompleteType.BIGINT.toField("age"), CompleteType.VARCHAR.toField("gender")).toField("info"));
      schemaBuilder.addField(CompleteType.struct(new Field("arr", true, Types.MinorType.LIST.getType(),
          Collections.singletonList(CompleteType.BIGINT.toField("$data$"))),
        new Field("strArr", true, Types.MinorType.LIST.getType(),
          Collections.singletonList(CompleteType.VARCHAR.toField("$data$")))).toField("structWithArray"));

      schemaBuilder.addField(CompleteType.struct(CompleteType.struct(
        CompleteType.BIGINT.toField("num"),
        new Field("lis", FieldType.nullable(Types.MinorType.LIST.getType()),
          Collections.singletonList(CompleteType.BIGINT.toField("$data$"))),
        CompleteType.struct(CompleteType.BIGINT.toField("num2"))
          .toField("struc"))
        .toField("innerStruct"))
        .toField("structWithStruct"));
      schemaBuilder.addField(new Field("likes", true, Types.MinorType.LIST.getType(),
        Collections.singletonList(CompleteType.VARCHAR.toField("$data$"))));
      Field structChild = CompleteType.struct(
        CompleteType.VARCHAR.toField("name"),
        CompleteType.VARCHAR.toField("gender"),
          CompleteType.BIGINT.toField("age")).toField("$data$");

      schemaBuilder.addField(new Field("children", true, Types.MinorType.LIST.getType(),
          Collections.singletonList(structChild)));

      Field listChild = new Field("$data$", true, Types.MinorType.LIST.getType(),
        Collections.singletonList(CompleteType.BIGINT.toField("$data$")));

      schemaBuilder.addField(new Field("matrix", true, Types.MinorType.LIST.getType(),
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
      final String ctasQuery = String.format("CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) " +
          " AS SELECT count(*), n_regionkey from cp.\"tpch/nation.parquet\" " +
          "group by n_regionkey",
        TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      File[] filesInTableDirectory = tableFolder.listFiles();
      Assert.assertNotNull(filesInTableDirectory);
      // table folder contains metadata folder and queryId folder
      File queryIDFolder = getQueryIDFolderAfterFirstWrite(tableFolder);

      // This should throw exception if the directory name is not a queryId
      QueryIdHelper.getQueryIdFromString(queryIDFolder.getName());

      final String selectFromCreatedTable = String.format(" select cnt, rkey from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select count(*) as cnt, n_regionkey as rkey from cp.\"tpch/nation.parquet\" group by n_regionkey";
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
      final String intQuery1 = String.format("CREATE TABLE %s.%s(id int)",
        TEMP_SCHEMA, intTable1);
      test(intQuery1);

      final String intQuery2 = String.format("CREATE TABLE %s.%s(n_nationkey int)",
        TEMP_SCHEMA, intTable2);
      test(intQuery2);

      final String insertQuery = String.format("INSERT INTO %s.%s SELECT n_nationkey from %s.%s",
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
      final String intStringQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT n_nationkey, n_name from cp.\"tpch/nation.parquet\" limit 1",
        TEMP_SCHEMA, intString);
      test(intStringQuery);

      final String intIntQuery = String.format("CREATE TABLE %s.%s(n_nationkey int, n_regionkey int)",
        TEMP_SCHEMA, intInt);
      test(intIntQuery);

      final String intIntInToIntString = String.format("INSERT INTO %s.%s SELECT n_nationkey, n_regionkey n_name from %s.%s",
        TEMP_SCHEMA, intString, TEMP_SCHEMA, intInt);

      ctasErrorTestHelper(intIntInToIntString, "Table schema(n_nationkey::int32, n_name::varchar) " +
        "doesn't match with query schema(n_nationkey::int32, n_regionkey::int32)");

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
      final String decimal1Query = String.format("CREATE TABLE %s.%s(id int, code decimal(18, 3))",
        TEMP_SCHEMA, decimal1);
      test(decimal1Query);

      final String decimal2Query = String.format("CREATE TABLE %s.%s(id int, code decimal(18, 1))",
        TEMP_SCHEMA, decimal2);
      test(decimal2Query);

      final String decimal2IntoDecimal1 = String.format("INSERT INTO %s.%s SELECT id, code from %s.%s",
        TEMP_SCHEMA, decimal1, TEMP_SCHEMA, decimal2);

      ctasErrorTestHelper(decimal2IntoDecimal1, "Table schema(id::int32, code::decimal(18,3)) doesn't match " +
        "with query schema(id::int32, code::decimal(18,1))");

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
      final String ctasQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 1",
        TEMP_SCHEMA, newTblName);

      final String createEmptyTableQuery = String.format("CREATE TABLE %s.%s(n_nationkey int, n_regionkey int)",
        TEMP_SCHEMA, emptyTblName);

      test(createEmptyTableQuery);
      Path emptyTableDirectoryPath = new File(getDfsTestTmpSchemaLocation(), emptyTblName).toPath();
      File emptyTableMetadataDirectory = new File(emptyTableDirectoryPath.resolve(IcebergFormatMatcher.METADATA_DIR_NAME).toUri());
      Assert.assertNotNull(emptyTableMetadataDirectory);
      // snapshot on create empty table
      assertEquals(1, Objects.requireNonNull(emptyTableMetadataDirectory.listFiles(
        (dir, name) -> name.startsWith("snap"))).length);

      test(ctasQuery);
      Path ctasPath = new File(getDfsTestTmpSchemaLocation(), newTblName).toPath();
      File ctasTableMetadataDirectory = new File(ctasPath.resolve(IcebergFormatMatcher.METADATA_DIR_NAME).toUri());
      Assert.assertNotNull(ctasTableMetadataDirectory);
      // snapshot on ctas
      assertEquals(1, Objects.requireNonNull(ctasTableMetadataDirectory.listFiles(
        (dir, name) -> name.startsWith("snap"))).length);

      // ctas empty data
      final String insertQuery = String.format("CREATE TABLE %s.%s  " +
          " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit 0",
        TEMP_SCHEMA, ctasEmptyTblName);
      test(insertQuery);
      // // snapshot on ctas empty data
      assertEquals(1, Objects.requireNonNull(ctasTableMetadataDirectory.listFiles(
        (dir, name) -> name.startsWith("snap"))).length);

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), emptyTblName));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), ctasEmptyTblName));
    }
  }

  private static void ctasErrorTestHelper(final String ctasSql, final String expErrorMsg) throws Exception {
    final String createTableSql = String.format(ctasSql, TEMP_SCHEMA, "testTableName");
    errorMsgTestHelper(createTableSql, expErrorMsg);
  }

  @Test
  public void testCreateTableCommandInvalidPath() throws Exception {
    final String tblName = "invalid_path_test";
    try (AutoCloseable c = enableIcebergTables()) {
      final String createTableQuery = String.format("CREATE TABLE %s(id int, code int)", tblName);
      expectedEx.expect(UserException.class);
      expectedEx.expectMessage(String.format("Invalid path. Given path, [%s] is not valid.", tblName));
      test(createTableQuery);
    }
    finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tblName));
    }
  }

  @Test
  public void testCaseSensitiveDateColumn() throws Exception {
    final String caseSensitiveTest = "case_sensitive_date_test";
    try (AutoCloseable c = enableIcebergTables()) {
      String createCommandSql = "create table " + TEMP_SCHEMA + "." +
        caseSensitiveTest + " as " +
        "select * from (values (to_date(cast('2020-01-09' as date) + interval '1' day )))";
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
}
