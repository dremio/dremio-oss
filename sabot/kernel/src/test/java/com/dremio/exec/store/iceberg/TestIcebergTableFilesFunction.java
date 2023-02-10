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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.FileContent;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.google.common.collect.Lists;

/**
 * Test class for Iceberg TABLE_FILES Function select * from table(table_files('table'))
 */
public class TestIcebergTableFilesFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableFilesColumnValues() throws Exception {
    insertOneRecord();

    String query = String.format("SELECT content FROM table(table_files('\"%s\".\"%s\"')) limit 1", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    String[] expectedColumns = {"content"};
    Object[] expectedValues = {FileContent.DATA.name()};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    //Match count of data files. -> ONE
    query = String.format("SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[]{"file_count"};
    expectedValues = new Object[]{1L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    insertOneRecord();

    //Match count of data files. -> TWO
    query = String.format("SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[]{"file_count"};
    expectedValues = new Object[]{2L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    insertTwoRecords();

    //Match count for record_count=2
    query = String.format("SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"')) where record_count=2", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[]{"file_count"};
    expectedValues = new Object[]{1L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    //Match count with snapshot
    query = String.format("SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"')) AT SNAPSHOT '%s'", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, FIRST_SNAPSHOT);
    expectedColumns = new String[]{"file_count"};
    expectedValues = new Object[]{0L};
    queryAndMatchResults(query, expectedColumns, expectedValues);
  }

  private void insertTwoRecords() throws Exception {
    String insertCommandSql = String.format("insert into %s.%s VALUES(1,'a', 2.0),(2,'b', 3.0)", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  @Test
  public void testTableFilesSchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_format"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_size_in_bytes"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("column_sizes"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("null_value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("nan_value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("lower_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("upper_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("split_offsets"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("equality_ids"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("sort_order_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema(expectedSchema,"SELECT * FROM table(table_files('\"%s\".\"%s\"')) limit 1", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
  }

  @Test
  public void testInvalidColumnTypeTableFilesSchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_format"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("file_size_in_bytes"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("column_sizes"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("null_value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("nan_value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("lower_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("upper_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("split_offsets"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("equality_ids"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("sort_order_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    assertThatThrownBy(() -> expectedSchema(expectedSchema,"SELECT * FROM table(table_files('\"%s\".\"%s\"'))", TEMP_SCHEMA_HADOOP
    , METADATA_TEST_TABLE_NAME))
      .hasMessageContaining("Schema path or type mismatch for")
      .isInstanceOf(Exception.class);
  }

  @Test
  public void incorrectTableName() {
    String query = "SELECT count(*) as k FROM table(table_files('blah'))";
    assertThatThrownBy(() -> runSQL(query))
      .hasMessageContaining("not found");
  }

}
