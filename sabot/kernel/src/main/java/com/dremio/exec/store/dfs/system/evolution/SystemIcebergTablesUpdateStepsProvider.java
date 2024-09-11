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
package com.dremio.exec.store.dfs.system.evolution;

import com.dremio.exec.store.dfs.copyinto.CopyFileHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.copyinto.CopyJobHistoryTableSchemaProvider;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.store.dfs.system.evolution.SystemIcebergTablesUpdateStepsProvider.UpdateSteps;
import com.dremio.exec.store.dfs.system.evolution.step.ImmutableSystemIcebergTablePropertyUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePropertyUpdateStep;
import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class provides methods to generate schema evolution steps for system Iceberg tables. It
 * retrieves update steps for a given table name and schema versions.
 */
public class SystemIcebergTablesUpdateStepsProvider implements Iterator<UpdateSteps> {

  private final List<SystemIcebergTableSchemaUpdateStep> schemaEvolutionSteps = new ArrayList<>();
  private final List<SystemIcebergTablePartitionUpdateStep> partitionEvolutionSteps =
      new ArrayList<>();
  private final List<SystemIcebergTablePropertyUpdateStep> propertyUpdateSteps = new ArrayList<>();
  private final long numberOfSteps;
  private int idx = 0;

  public SystemIcebergTablesUpdateStepsProvider(
      String tableName, long currentSchemaVersion, long updateSchemaVersion) {
    numberOfSteps = updateSchemaVersion - currentSchemaVersion;

    if (SystemIcebergTableMetadataFactory.COPY_JOB_HISTORY_TABLE_NAME.equals(tableName)) {
      for (long i = currentSchemaVersion + 1; i <= updateSchemaVersion; i++) {
        schemaEvolutionSteps.add(CopyJobHistoryTableSchemaProvider.getSchemaEvolutionStep(i));
        partitionEvolutionSteps.add(CopyJobHistoryTableSchemaProvider.getPartitionEvolutionStep(i));
        propertyUpdateSteps.add(
            new ImmutableSystemIcebergTablePropertyUpdateStep.Builder()
                .putProperties(
                    SystemIcebergTableMetadata.SCHEMA_VERSION_PROPERTY, String.valueOf(i))
                .build());
      }
    } else if (SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME.equals(tableName)) {
      for (long i = currentSchemaVersion + 1; i <= updateSchemaVersion; i++) {
        schemaEvolutionSteps.add(CopyFileHistoryTableSchemaProvider.getSchemaEvolutionStep(i));
        partitionEvolutionSteps.add(
            CopyFileHistoryTableSchemaProvider.getPartitionEvolutionStep(i));
        propertyUpdateSteps.add(
            new ImmutableSystemIcebergTablePropertyUpdateStep.Builder()
                .putProperties(
                    SystemIcebergTableMetadata.SCHEMA_VERSION_PROPERTY, String.valueOf(i))
                .build());
      }
    }
  }

  @Override
  public boolean hasNext() {
    return idx < numberOfSteps;
  }

  @Override
  public UpdateSteps next() {
    UpdateSteps updateSteps =
        new UpdateSteps(
            schemaEvolutionSteps.get(idx),
            partitionEvolutionSteps.get(idx),
            propertyUpdateSteps.get(idx));
    idx++;
    return updateSteps;
  }

  public static class UpdateSteps {
    private final SystemIcebergTableSchemaUpdateStep schemaUpdateStep;
    private final SystemIcebergTablePartitionUpdateStep partitionUpdateStep;
    private final SystemIcebergTablePropertyUpdateStep propertyUpdateStep;

    private UpdateSteps(
        SystemIcebergTableSchemaUpdateStep schemaUpdateStep,
        SystemIcebergTablePartitionUpdateStep partitionUpdateStep,
        SystemIcebergTablePropertyUpdateStep propertyUpdateStep) {
      this.schemaUpdateStep = schemaUpdateStep;
      this.partitionUpdateStep = partitionUpdateStep;
      this.propertyUpdateStep = propertyUpdateStep;
    }

    public SystemIcebergTableSchemaUpdateStep getSchemaUpdateStep() {
      return schemaUpdateStep;
    }

    public SystemIcebergTablePartitionUpdateStep getPartitionUpdateStep() {
      return partitionUpdateStep;
    }

    public SystemIcebergTablePropertyUpdateStep getPropertyUpdateStep() {
      return propertyUpdateStep;
    }
  }
}
