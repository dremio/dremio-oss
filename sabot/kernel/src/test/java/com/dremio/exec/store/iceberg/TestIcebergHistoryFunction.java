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
import com.dremio.common.util.JodaDateUtility;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Snapshot;
import org.junit.Test;

/** Test class for Iceberg History Function select * from table(table_history('table')) */
public class TestIcebergHistoryFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableHistory() throws Exception {
    // match all the expected columns values.
    expectedHistoryResult(
        String.format(
            "SELECT * FROM table(table_history('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME));
  }

  @Test
  public void testTableHistoryCount() throws Exception {
    testBuilder()
        .sqlQuery(
            String.format(
                "SELECT count(*) as history_count FROM table(table_history('\"%s\".\"%s\"'))",
                TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME))
        .unOrdered()
        .baselineColumns("history_count")
        .baselineValues(1L)
        .build()
        .run();
  }

  @Test
  public void testTableHistorySchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("made_current_at"),
            Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("is_current_ancestor"),
            Types.required(TypeProtos.MinorType.BIT)));
    expectedSchema(
        expectedSchema,
        String.format(
            "SELECT * FROM table(table_history('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME));
  }

  @Test
  public void testInvalidColumnTypeTableHistorySchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("made_current_at"),
            Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("is_current_ancestor"),
            Types.required(TypeProtos.MinorType.VARCHAR))); // Invalid type , instead of BIT
    assertThatThrownBy(
            () ->
                expectedSchema(
                    expectedSchema,
                    String.format(
                        "SELECT * FROM table(table_history('\"%s\".\"%s\"')) limit 1",
                        TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME)))
        .hasMessageContaining("Schema path or type mismatch for")
        .isInstanceOf(Exception.class);
  }

  @Test
  public void incorrectName() {
    String query = "SELECT count(*) as k FROM table(table_history('blah'))";
    assertThatThrownBy(() -> runSQL(query)).hasMessageContaining("not found");
  }

  private void expectedHistoryResult(String query) throws Exception {
    Iterable<Snapshot> snapshots = getSnapshots();
    Snapshot snapshot1 = snapshots.iterator().next();
    LocalDateTime dateTime =
        Instant.ofEpochMilli(snapshot1.timestampMillis())
            .atZone(ZoneId.systemDefault()) // default zone
            .toLocalDateTime();
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("made_current_at", "snapshot_id", "parent_id", "is_current_ancestor")
        .baselineValues(
            JodaDateUtility.javaToJodaLocalDateTime(dateTime),
            snapshot1.snapshotId(),
            snapshot1.parentId(),
            true)
        .build()
        .run();
  }
}
