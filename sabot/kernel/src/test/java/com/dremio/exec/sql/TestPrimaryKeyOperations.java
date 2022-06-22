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

import static com.dremio.exec.store.dfs.PrimaryKeyOperations.DREMIO_PRIMARY_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * Test class for primary key add/drop operations
 * and verify iceberg metadata.
 */
public class TestPrimaryKeyOperations extends BaseTestQuery {

  protected static String finalIcebergMetadataLocation;

  @BeforeClass
  public static void setupIcebergMetadataLocation() {
    finalIcebergMetadataLocation = getDfsTestTmpSchemaLocation();
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation()));
  }

  @Test
  public void testAddAndDropPrimaryKey() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "foo";
      try (AutoCloseable ignore = enableIcebergTables()) {
        createTable(testSchema, tableName);

        final String primaryKey = "r_regionkey";
        final String addPrimaryKeySql = String.format("" +
            "alter table %s.%s add primary key (%s)",
          testSchema, tableName, primaryKey);
        runSQL(addPrimaryKeySql);

        verifyIcebergMetadataProperties(
          tableName,
          ImmutableList.of(Field.nullable(primaryKey, new ArrowType.Int(32, true))));

        final String dropPrimaryKeySql = String.format("" +
            "alter table %s.%s drop primary key",
          testSchema, tableName);
        runSQL(dropPrimaryKeySql);

        verifyIcebergMetadataProperties(
          tableName,
          ImmutableList.of());

        dropTable(testSchema, tableName);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableName));
      }
    }
  }

  @Test
  public void testAddInvalidPrimaryKey() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "foo";
      try (AutoCloseable ignore = enableIcebergTables()) {
        createTable(testSchema, tableName);

        // Try to add a non-existent primary key.
        final String primaryKey = "blah_blah";
        final String addPrimaryKeySql = String.format("" +
            "alter table %s.%s add primary key (%s)",
          testSchema, tableName, primaryKey);
        try {
          runSQL(addPrimaryKeySql);
          fail(String.format("There should have been no primary key \"%s\" to add", primaryKey));
        } catch (Exception ex) {
          assertTrue(ex.getMessage().contains(String.format("Column %s not found", primaryKey)));
        }

        dropTable(testSchema, tableName);
      } finally {
        FileUtils.deleteQuietly(new File(finalIcebergMetadataLocation, tableName));
      }
    }
  }

  @Test
  public void testDropInvalidPrimaryKey() throws Exception {
    for (String testSchema : SCHEMAS_FOR_TEST) {
      String tableName = "foo";
      try (AutoCloseable ignore = enableIcebergTables()) {
        createTable(testSchema, tableName);

        // Try to drop a primary key when there is none added.
        final String dropPrimaryKeySql = String.format("" +
            "alter table %s.%s drop primary key",
          testSchema, tableName);
        try {
          runSQL(dropPrimaryKeySql);
          fail("There should be no primary key to drop.");
        } catch (Exception ex) {
          assertTrue(ex.getMessage().contains("No primary key to drop"));
        }

        dropTable(testSchema, tableName);
      } finally {
        FileUtils.deleteQuietly(new File(finalIcebergMetadataLocation, tableName));
      }
    }
  }

  private void createTable(String testSchema, String tableName) throws Exception {
    final String createTableQuery = String.format("" +
        "CREATE TABLE %s.%s as SELECT * from cp.\"tpch/region.parquet\"",
      testSchema, tableName);
    test(createTableQuery);
  }

  private void dropTable(String testSchema, String tableName) throws Exception {
    final String dropTableQuery = String.format("DROP TABLE %s.%s", testSchema, tableName);
    test(dropTableQuery);
  }

  private void verifyIcebergMetadataProperties(String tableName, List<Field> fields) {
    Table icebergTable = getIcebergTable(new File(finalIcebergMetadataLocation, tableName),
      IcebergCatalogType.HADOOP);
    Map<String, String> properties = icebergTable.properties();
    assertTrue(properties.containsKey(DREMIO_PRIMARY_KEY));

    String serializedSchema = properties.get(DREMIO_PRIMARY_KEY);
    ObjectMapper mapper = new ObjectMapper();
    BatchSchema batchSchema;
    try {
      batchSchema = mapper.readValue(serializedSchema, BatchSchema.class);
    } catch (JsonProcessingException e) {
      String error = "Unexpected error occurred while deserializing primary keys in json string.";
      throw new RuntimeException(error);
    }
    assertEquals(batchSchema.getFields(), fields);
  }
}
