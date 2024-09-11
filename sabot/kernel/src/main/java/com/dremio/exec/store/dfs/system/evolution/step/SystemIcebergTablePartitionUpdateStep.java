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
package com.dremio.exec.store.dfs.system.evolution.step;

import com.dremio.exec.planner.sql.PartitionTransform;
import java.util.Collections;
import java.util.List;
import org.immutables.value.Value;

/**
 * Represents a partition evolution step for system Iceberg tables. This class encapsulates the
 * changes made to the table partitioning during a partition evolution.
 */
@Value.Immutable
public abstract class SystemIcebergTablePartitionUpdateStep
    implements SystemIcebergTableUpdateStep {

  /**
   * Retrieves the list of partition transforms associated with this partition update step.
   *
   * @return The list of partition transforms.
   */
  @Value.Default
  public List<PartitionTransform> getPartitionTransforms() {
    return Collections.emptyList();
  }

  /**
   * Retrieves the schema version associated with this partition update step.
   *
   * @return The schema version.
   */
  @Value.Parameter
  public abstract long getSchemaVersion();
}
