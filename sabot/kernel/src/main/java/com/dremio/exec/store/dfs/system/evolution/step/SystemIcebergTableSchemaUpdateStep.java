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

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.immutables.value.Value;

/**
 * Represents a schema evolution step for a system Iceberg table. This class encapsulates the
 * changes made to the table schema during a schema evolution.
 */
@Value.Immutable
public abstract class SystemIcebergTableSchemaUpdateStep implements SystemIcebergTableUpdateStep {

  /**
   * Returns the list of columns that were added in this schema evolution step.
   *
   * @return The list of added columns. One entry in the list contains the name, the type and an
   *     isOptional flag of the column
   */
  @Value.Default
  public List<Triple<String, Type, Boolean>> getAddedColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the list of columns that were deleted in this schema evolution step.
   *
   * @return The list of deleted columns.
   */
  @Value.Default
  public List<String> getDeletedColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the list of columns that were changed in this schema evolution step.
   *
   * @return The list of changed columns.
   */
  @Value.Default
  public List<Pair<String, PrimitiveType>> getChangedColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the list of columns that were renamed in this schema evolution step.
   *
   * @return The list of renamed columns.
   */
  @Value.Default
  public List<Pair<String, String>> getRenamedColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the schema version associated with this schema evolution step.
   *
   * @return The schema version.
   */
  @Value.Parameter
  public abstract long getSchemaVersion();
}
