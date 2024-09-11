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
import java.util.Map;
import org.immutables.value.Value;

/**
 * Represents a property update step for a system Iceberg table. This class encapsulates the changes
 * made to the properties of the table.
 */
@Value.Immutable
public abstract class SystemIcebergTablePropertyUpdateStep implements SystemIcebergTableUpdateStep {

  /**
   * Retrieves the properties associated with this property update step.
   *
   * @return The properties.
   */
  @Value.Default
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  /**
   * Retrieves the schema version associated with this property update step.
   *
   * @return The schema version.
   */
  @Value.Parameter
  public abstract long getSchemaVersion();
}
