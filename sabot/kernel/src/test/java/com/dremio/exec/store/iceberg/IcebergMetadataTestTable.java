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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_METADATA_FUNCTIONS;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

  @BeforeClass
  public static void initIcebergTable() throws Exception {
    setSystemOption(ENABLE_ICEBERG_METADATA_FUNCTIONS, "true");
    //Create a non-partitioned iceberg table,
    String createCommandSql = String.format("create table %s.%s(c1 int, c2 varchar, c3 double)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    test(createCommandSql);
    Thread.sleep(1001);
    File tableFolder = new File(getDfsTestTmpSchemaLocation(), METADATA_TEST_TABLE_NAME);
    metadata_test_iceberg_table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
    FIRST_SNAPSHOT = metadata_test_iceberg_table.currentSnapshot().snapshotId();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    setSystemOption(ENABLE_ICEBERG_METADATA_FUNCTIONS, "false");
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

  protected void insertOneRecord() throws Exception {
    String insertCommandSql = String.format("insert into %s.%s VALUES(1,'a', 2.0)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
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

}
