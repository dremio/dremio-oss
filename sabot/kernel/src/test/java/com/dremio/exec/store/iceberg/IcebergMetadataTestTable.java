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

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Before;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.google.common.collect.Lists;

/**
 * Abstract class for Iceberg Metadata functions tests
 */
public abstract class IcebergMetadataTestTable extends BaseTestQuery {

  protected static Table metadata_test_iceberg_table;
  protected static final String METADATA_TEST_TABLE_NAME = "iceberg_metadata_test_table";

  protected static Long FIRST_SNAPSHOT;

  @Before
  public void before() throws Exception {
    //Create a non-partitioned iceberg table,
    String createCommandSql = String.format("create table %s.%s(c1 int, c2 varchar, c3 double)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    runSQL(createCommandSql);
    Thread.sleep(1001);
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), METADATA_TEST_TABLE_NAME);
    metadata_test_iceberg_table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
    FIRST_SNAPSHOT = metadata_test_iceberg_table.currentSnapshot().snapshotId();
  }

  @After
  public void after() throws Exception {
    //Drop table
    runSQL(String.format("DROP TABLE %s.%s", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME));
    FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), METADATA_TEST_TABLE_NAME));
  }

  protected List<Snapshot> getSnapshots() {
    loadTable();
    return Lists.newArrayList(metadata_test_iceberg_table.snapshots());
  }

  protected List<ManifestFile> getManifests() {
    loadTable();
    return metadata_test_iceberg_table.currentSnapshot().allManifests(metadata_test_iceberg_table.io());
  }

  protected List<HistoryEntry> getHistory() {
    loadTable();
    return metadata_test_iceberg_table.history();
  }

  private static void loadTable() {
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), METADATA_TEST_TABLE_NAME);
    metadata_test_iceberg_table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
  }

  protected void addPartition(String partitionSpec) throws Exception {
    String insertCommandSql = String.format("ALTER TABLE %s.%s ADD PARTITION FIELD %s", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, partitionSpec);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  protected void insertOneRecord() throws Exception {
    String insertCommandSql = String.format("insert into %s.%s VALUES(1,'a', 2.0)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  protected void insertTwoRecords() throws Exception {
    String insertCommandSql = String.format("insert into %s.%s VALUES(1,'a', 2.0),(2,'b', 3.0)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  protected void insertTwoLongRecords() throws Exception {
    String insertCommandSql = String.format("insert into %s.%s VALUES(1,'abcdfg', 2.0),(2,'bcdfff', 3.0)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  protected void expectedSchema(List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema, String query, Object... args) throws Exception {
    testBuilder()
      .sqlQuery(query, args)
      .schemaBaseLine(expectedSchema)
      .build()
      .run();
  }

  protected void queryAndMatchResults(String query, String[] expectedColumns, Object[] expectedValues) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(expectedColumns)
      .baselineValues(expectedValues)
      .build()
      .run();
  }

  protected void queryAndMatchResults(String query, List<Map<String, Object>> expectedRecords) throws Exception {
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineRecords(expectedRecords)
      .build()
      .run();
  }
}
