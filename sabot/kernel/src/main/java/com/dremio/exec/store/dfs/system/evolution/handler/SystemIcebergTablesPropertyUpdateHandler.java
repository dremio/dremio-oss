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
package com.dremio.exec.store.dfs.system.evolution.handler;

import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePropertyUpdateStep;
import java.util.stream.Collectors;
import org.apache.iceberg.UpdateProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles property updates for system Iceberg tables. This class implements the {@link
 * SystemIcebergTablesUpdateHandler} interface for updating property-related changes.
 */
public class SystemIcebergTablesPropertyUpdateHandler
    implements SystemIcebergTablesUpdateHandler<
        UpdateProperties, SystemIcebergTablePropertyUpdateStep> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SystemIcebergTablesPropertyUpdateHandler.class);

  @Override
  public void update(UpdateProperties update, SystemIcebergTablePropertyUpdateStep step) {
    LOGGER.debug(
        "Applying property update step for schema version {}. Adding properties: [{}]",
        step.getSchemaVersion(),
        step.getProperties().entrySet().stream()
            .map(e -> String.format("%s : %s", e.getKey(), e.getValue()))
            .collect(Collectors.joining(", ")));
    step.getProperties().forEach(update::set);
    update.commit();
  }
}
