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
package com.dremio.exec.store.dfs.copyinto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import org.apache.iceberg.Schema;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCopyJobHistorySchemaProvider {

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetSchema(int schemaVersion) {
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      // Test for schema version 1
      assertThat(schema.columns().size()).isEqualTo(10);
    } else if (schemaVersion == 2) {
      // Test for schema version 2
      assertThat(schema.columns().size()).isEqualTo(14);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetSchemaEvolutionStep(int schemaVersion) {
    SystemIcebergTableSchemaUpdateStep schemaEvolutionStep =
        CopyJobHistoryTableSchemaProvider.getSchemaEvolutionStep(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(schemaEvolutionStep.getAddedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(1);
    } else if (schemaVersion == 2) {
      assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(4);
      assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(2);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetPartitionEvolutionStep(int schemaVersion) {
    SystemIcebergTablePartitionUpdateStep partitionEvolutionStep =
        CopyJobHistoryTableSchemaProvider.getPartitionEvolutionStep(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
    } else if (schemaVersion == 2) {
      assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(1);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {3})
  public void testGetUnsupportedVersion(int unsupportedSchemaVersion) {
    assertThrows(
        "Unsupported copy_job_history table schema version. Currently supported schema version are: 1, 2",
        UnsupportedOperationException.class,
        () -> CopyJobHistoryTableSchemaProvider.getSchema(unsupportedSchemaVersion));
    assertThrows(
        "Unsupported copy_job_history table schema version. Currently supported schema version are: 1, 2",
        UnsupportedOperationException.class,
        () ->
            CopyJobHistoryTableSchemaProvider.getPartitionEvolutionStep(unsupportedSchemaVersion));
  }

  @Test
  public void testGetUserNameCol() {
    String userNameColName = CopyJobHistoryTableSchemaProvider.getUserNameColName();
    assertThat(userNameColName).isEqualTo("user_name");
  }

  @Test
  public void testGetJobIdCol() {
    String jobIdColName = CopyJobHistoryTableSchemaProvider.getJobIdColName();
    assertThat(jobIdColName).isEqualTo("job_id");
  }

  @Test
  public void testGetTableNameCol() {
    String tableNameColName = CopyJobHistoryTableSchemaProvider.getTableNameColName();
    assertThat(tableNameColName).isEqualTo("table_name");
  }

  @Test
  public void testGetCopyOptionsCol() {
    String copyOptionsColName = CopyJobHistoryTableSchemaProvider.getCopyOptionsColName();
    assertThat(copyOptionsColName).isEqualTo("copy_options");
  }

  @Test
  public void testGetStorageLocationCol() {
    String storageLocationColName = CopyJobHistoryTableSchemaProvider.getStorageLocationColName();
    assertThat(storageLocationColName).isEqualTo("storage_location");
  }

  @Test
  public void testGetFileFormatCol() {
    String fileFormatColName = CopyJobHistoryTableSchemaProvider.getFileFormatColName();
    assertThat(fileFormatColName).isEqualTo("file_format");
  }

  @Test
  public void testGetBaseSnapshotCol() {
    String baseSnapshotIdColName = CopyJobHistoryTableSchemaProvider.getBaseSnapshotIdColName();
    assertThat(baseSnapshotIdColName).isEqualTo("base_snapshot_id");
  }

  @Test
  public void testGetExecutedAtCol() {
    String executedAtColName = CopyJobHistoryTableSchemaProvider.getExecutedAtColName();
    assertThat(executedAtColName).isEqualTo("executed_at");
  }
}
