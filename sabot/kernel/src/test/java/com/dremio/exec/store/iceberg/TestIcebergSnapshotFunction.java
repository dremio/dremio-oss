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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Snapshot;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.JodaDateUtility;
import com.google.common.collect.Lists;

/**
 * Test class for iceberg snapshot functions select * from table(table_snapshot('table'))
 */
public class TestIcebergSnapshotFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableSnapshotSchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("committed_at"), Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("operation"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("manifest_list"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("summary"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema(expectedSchema,"SELECT * FROM table(table_snapshot('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void testInvalidColumnTypeTableSnapshotSchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("committed_at"), Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("operation"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("manifest_list"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("summary"), Types.required(TypeProtos.MinorType.STRUCT))); //Struct instead of List
    assertThatThrownBy(() -> expectedSchema(expectedSchema,"SELECT * FROM table(table_snapshot('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath()))
      .hasMessageContaining("Schema path or type mismatch for")
      .isInstanceOf(Exception.class);
  }

  @Test
  public void testTableSnapshots() throws Exception {
    expectedSnapshotsResult("SELECT committed_at,snapshot_id,parent_id,operation,manifest_list FROM table(table_snapshot('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void testTableSnapshotsCount() throws Exception {
    expectedSnapshotsCount("SELECT count(*) as k FROM table(table_snapshot('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void incorrectName() {
    String query = "SELECT count(*) as k FROM table(table_snapshot('blah'))";
    assertThatThrownBy(() -> runSQL(query))
      .hasMessageContaining("not found");
  }

  private void expectedSnapshotsResult(String query, Object... args) throws Exception {
    Iterable<Snapshot> snapshots = table.snapshots();
    Snapshot snapshot1 = snapshots.iterator().next();
    LocalDateTime dateTime = Instant.ofEpochMilli(snapshot1.timestampMillis())
      .atZone(ZoneId.systemDefault()) // default zone
      .toLocalDateTime();
    //TODO: find a way to match summary column
    testBuilder()
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("committed_at","snapshot_id","parent_id","operation","manifest_list")
      .baselineValues(
        JodaDateUtility.javaToJodaLocalDateTime(dateTime),
        snapshot1.snapshotId(),
        snapshot1.parentId(),
        snapshot1.operation(),
        snapshot1.manifestListLocation())
      .build()
      .run();
  }

  private void expectedSnapshotsCount(String query, Object... args) throws Exception {
    long count = 0;
    for (Snapshot snapshot : table.snapshots()) {
      count++;
    }
    testBuilder()
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("k")
      .baselineValues(count)
      .build()
      .run();
  }

}
