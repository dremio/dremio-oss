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

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Test class for iceberg table partitions functions select * from table(table_partitions('table'))
 */
public class TestIcebergPartitionsFunction extends IcebergMetadataTestTable {

  @Test
  public void testTablePartitionsSchemaOnPartitionedTable() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("file_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    addPartition("c1");
    expectedSchema(
        expectedSchema,
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME));
  }

  @Test
  public void testInvalidTablePartitionsSchemaOnPartitionedTable() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("file_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    addPartition("c1");
    assertThatThrownBy(
            () ->
                expectedSchema(
                    expectedSchema,
                    String.format(
                        "SELECT * FROM table(table_partitions('\"%s\".\"%s\"')) limit 1",
                        TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME)))
        .hasMessageContaining("Schema path or type mismatch")
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testTablePartitionsOnUnpartitionedTable() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("file_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    assertThatThrownBy(
            () ->
                expectedSchema(
                    expectedSchema,
                    String.format(
                        "SELECT * FROM table(table_partitions('\"%s\".\"%s\"')) limit 1",
                        TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME)))
        .hasMessageContaining("VALIDATION ERROR:")
        .hasMessageContaining(
            "Table %s.%s is not partitioned.", TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME)
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testTablePartitionsBeforeAddingData() throws Exception {
    insertOneRecord();
    addPartition("c1");
    String[] expectedColumns = {"partition", "record_count", "file_count", "spec_id"};
    Object[] values = {"{}", 1L, 1, 0};
    queryAndMatchResults(
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        expectedColumns,
        values);
  }

  @Test
  public void testTablePartitionsAfterAddingData() throws Exception {
    insertOneRecord();
    addPartition("c1");
    insertTwoRecords();
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 0));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{c1=1}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 1));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{c1=2}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 1));
    final List<Map<String, Object>> baselineRecord = recordBuilder.build();
    queryAndMatchResults(
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        baselineRecord);
  }

  @Test
  public void testTablePartitionsStringPartitionField() throws Exception {
    insertOneRecord();
    addPartition("c2");
    insertTwoRecords();
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 0));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{c2=a}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 1));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{c2=b}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 1));
    final List<Map<String, Object>> baselineRecord = recordBuilder.build();
    queryAndMatchResults(
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        baselineRecord);
  }

  @Test
  public void testTablePartitionsTwoPartitionField() throws Exception {
    insertOneRecord();
    addPartition("c1");
    addPartition("c2");
    insertTwoRecords();
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 0));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`",
            "{c1=1, c2=a}",
            "`record_count`",
            1L,
            "`file_count`",
            1,
            "`spec_id`",
            2));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`",
            "{c1=2, c2=b}",
            "`record_count`",
            1L,
            "`file_count`",
            1,
            "`spec_id`",
            2));
    final List<Map<String, Object>> baselineRecord = recordBuilder.build();
    queryAndMatchResults(
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        baselineRecord);
  }

  @Test
  public void testTablePartitionsPartitionTransformField() throws Exception {
    insertOneRecord();
    addPartition("truncate(2, c2)");
    insertTwoLongRecords();
    final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`", "{}", "`record_count`", 1L, "`file_count`", 1, "`spec_id`", 0));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`",
            "{c2_trunc_2=ab}",
            "`record_count`",
            1L,
            "`file_count`",
            1,
            "`spec_id`",
            1));
    recordBuilder.add(
        ImmutableMap.of(
            "`partition`",
            "{c2_trunc_2=bc}",
            "`record_count`",
            1L,
            "`file_count`",
            1,
            "`spec_id`",
            1));
    final List<Map<String, Object>> baselineRecord = recordBuilder.build();
    queryAndMatchResults(
        String.format(
            "SELECT * FROM table(table_partitions('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        baselineRecord);
  }
}
