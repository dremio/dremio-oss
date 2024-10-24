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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import org.apache.iceberg.Schema;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TestCopyFileHistorySchemaProvider {

  @Test
  public void testGetSchema() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      Schema schema = new CopyFileHistoryTableSchemaProvider(i).getSchema();
      if (i == 1) {
        assertThat(schema.columns().size()).isEqualTo(6);
      } else if (i == 2) {
        assertThat(schema.columns().size()).isEqualTo(12);
      } else if (i == 3) {
        assertThat(schema.columns().size()).isEqualTo(12);
      } else {
        Assertions.fail("Unknown schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetSchemaEvolutionStep() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      SystemIcebergTableSchemaUpdateStep schemaEvolutionStep =
          new CopyFileHistoryTableSchemaProvider(i).getSchemaEvolutionStep();
      if (i == 1) {
        assertThat(schemaEvolutionStep.getAddedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 2) {
        assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(6);
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 3) {
        assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(0);
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else {
        Assertions.fail("Unknown schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetPartitionTransforms() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      SystemIcebergTablePartitionUpdateStep partitionEvolutionStep =
          new CopyFileHistoryTableSchemaProvider(i).getPartitionEvolutionStep();
      if (i == 1) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
        assertThat(partitionEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 2) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(1);
        assertThat(partitionEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 3) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
        assertThat(partitionEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else {
        Assertions.fail("Unknown schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetUnsupportedVersion() {
    long unsupportedSchemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal() + 1;
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> new CopyFileHistoryTableSchemaProvider(unsupportedSchemaVersion).getSchema());
    assertThat(exception.getMessage())
        .contains(
            "Unsupported copy_file_history schema version: "
                + unsupportedSchemaVersion
                + ". Currently supported schema versions are: [1, 2, 3]");
  }
}
