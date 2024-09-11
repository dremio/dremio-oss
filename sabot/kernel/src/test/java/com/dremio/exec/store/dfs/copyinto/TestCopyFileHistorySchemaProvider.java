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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCopyFileHistorySchemaProvider {

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetSchema(int schemaVersion) {
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(schema.columns().size()).isEqualTo(6);
    } else if (schemaVersion == 2) {
      assertThat(schema.columns().size()).isEqualTo(12);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetSchemaEvolutionStep(int schemaVersion) {
    SystemIcebergTableSchemaUpdateStep schemaEvolutionStep =
        CopyFileHistoryTableSchemaProvider.getSchemaEvolutionStep(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(schemaEvolutionStep.getAddedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(1);
    } else if (schemaVersion == 2) {
      assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(6);
      assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
      assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(2);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testGetPartitionTransforms(int schemaVersion) {
    SystemIcebergTablePartitionUpdateStep partitionEvolutionStep =
        CopyFileHistoryTableSchemaProvider.getPartitionEvolutionStep(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
      assertThat(partitionEvolutionStep.getSchemaVersion()).isEqualTo(1);
    } else if (schemaVersion == 2) {
      assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(1);
      assertThat(partitionEvolutionStep.getSchemaVersion()).isEqualTo(2);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {3})
  public void testGetUnsupportedVersion(int unsupportedSchemaVersion) {
    assertThrows(
        "Unsupported copy_file_history table schema version. Currently supported schema version are: 1",
        UnsupportedOperationException.class,
        () -> CopyFileHistoryTableSchemaProvider.getSchema(unsupportedSchemaVersion));
  }
}
