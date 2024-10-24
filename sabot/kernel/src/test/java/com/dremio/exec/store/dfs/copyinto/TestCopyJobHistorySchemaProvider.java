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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCopyJobHistorySchemaProvider {

  @Test
  public void testGetSchema() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      Schema schema = new CopyJobHistoryTableSchemaProvider(i).getSchema();
      if (i == 1) {
        // Test for schema version 1
        assertThat(schema.columns().size()).isEqualTo(10);
      } else if (i == 2) {
        // Test for schema version 2
        assertThat(schema.columns().size()).isEqualTo(14);
      } else if (i == 3) {
        // Test for schema version 3
        assertThat(schema.columns().size()).isEqualTo(15);
      } else {
        Assertions.fail("Unexpected schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetSchemaEvolutionStep() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      SystemIcebergTableSchemaUpdateStep schemaEvolutionStep =
          new CopyJobHistoryTableSchemaProvider(i).getSchemaEvolutionStep();
      if (i == 1) {
        assertThat(schemaEvolutionStep.getAddedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 2) {
        assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(4);
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else if (i == 3) {
        assertThat(schemaEvolutionStep.getAddedColumns().size()).isEqualTo(1);
        assertThat(schemaEvolutionStep.getChangedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getDeletedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getRenamedColumns()).isEmpty();
        assertThat(schemaEvolutionStep.getSchemaVersion()).isEqualTo(i);
      } else {
        Assertions.fail("Unexpected schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetPartitionSpec() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      PartitionSpec partitionSpec = new CopyJobHistoryTableSchemaProvider(i).getPartitionSpec();
      if (i == 1) {
        assertThat(partitionSpec.isUnpartitioned()).isTrue();
      } else if (i == 2) {
        assertThat(partitionSpec.fields().size()).isEqualTo(1);
      } else if (i == 3) {
        assertThat(partitionSpec.fields().size()).isEqualTo(1);
      } else {
        Assertions.fail("Unexpected schema version: " + schemaVersion);
      }
    }
  }

  @Test
  public void testGetPartitionEvolutionStep() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      SystemIcebergTablePartitionUpdateStep partitionEvolutionStep =
          new CopyJobHistoryTableSchemaProvider(i).getPartitionEvolutionStep();
      if (i == 1) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
      } else if (i == 2) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(1);
      } else if (i == 3) {
        assertThat(partitionEvolutionStep.getPartitionTransforms().size()).isEqualTo(0);
      } else {
        Assertions.fail("Unexpected schema version: " + schemaVersion);
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
            () -> new CopyJobHistoryTableSchemaProvider(unsupportedSchemaVersion));
    assertThat(exception.getMessage())
        .contains(
            "Unsupported copy_job_history schema version: "
                + unsupportedSchemaVersion
                + ". Currently supported schema versions are: [1, 2, 3]");
  }
}
