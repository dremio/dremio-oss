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
import org.apache.iceberg.ManifestFile;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.google.common.collect.Lists;

/**
 * Test class for iceberg manifest function select * from table(table_manifests('table'))
 */
public class TestIcebergManifestsFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableManifestSchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("length"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("partition_spec_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("existing_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("deleted_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("existing_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("deleted_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("partition_summaries"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema(expectedSchema,"SELECT * FROM table(table_manifests('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void testInvalidColumnCountTableManifestSchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("length"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("partition_spec_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("existing_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("deleted_data_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("added_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("existing_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("deleted_delete_files_count"), Types.required(TypeProtos.MinorType.INT)));
    assertThatThrownBy(() -> expectedSchema(expectedSchema,"SELECT * FROM table(table_manifests('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath()))
      .hasMessageContaining("Expected and actual numbers of columns do not match.");
  }

  @Test
  public void testTableManifests() throws Exception {
    expectedManifestsResult("SELECT path,length,partition_spec_id,added_snapshot_id FROM table(table_manifests('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void testTableManifestsCount() throws Exception {
    expectedManifestsCount("SELECT count(*) as k FROM table(table_manifests('dfs_hadoop.\"%s\"')) limit 1", tableFolder.toPath());
  }

  @Test
  public void incorrectName() {
    String query = "SELECT count(*) as k FROM table(table_manifests('blah'))";
    assertThatThrownBy(() -> runSQL(query))
      .hasMessageContaining("not found");
  }

  private void expectedManifestsResult(String query, Object... args) throws Exception {
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests();
    ManifestFile manifestFile1 = manifestFiles.get(0);
    testBuilder()
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("path","length","partition_spec_id","added_snapshot_id")
      .baselineValues(
        manifestFile1.path(),
        manifestFile1.length(),
        manifestFile1.partitionSpecId(),
        manifestFile1.snapshotId())
      .build()
      .run();
  }

  private void expectedManifestsCount(String query, Object... args) throws Exception {
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests();
    testBuilder()
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("k")
      .baselineValues((long) manifestFiles.size())
      .build()
      .run();
  }


}
