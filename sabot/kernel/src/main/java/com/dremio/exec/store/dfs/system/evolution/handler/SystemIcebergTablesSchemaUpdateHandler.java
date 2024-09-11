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

import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTableSchemaUpdateStep;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.UpdateSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles schema updates for system Iceberg tables. This class implements the {@link
 * SystemIcebergTablesUpdateHandler} interface for updating schema-related changes.
 */
public class SystemIcebergTablesSchemaUpdateHandler
    implements SystemIcebergTablesUpdateHandler<UpdateSchema, SystemIcebergTableSchemaUpdateStep> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SystemIcebergTablesSchemaUpdateHandler.class);

  @Override
  public void update(UpdateSchema update, SystemIcebergTableSchemaUpdateStep step) {
    LOGGER.debug(
        "Applying schema update step for schema version {}.\nAdding columns: [{}]\nRemoving columns: [{}]\nChanging column types: [{}]\nRenaming columns: [{}]",
        step.getSchemaVersion(),
        step.getAddedColumns().stream().map(Pair::getLeft).collect(Collectors.joining(", ")),
        String.join(", ", step.getDeletedColumns()),
        step.getChangedColumns().stream()
            .map(p -> String.format("%s: %s", p.getLeft(), p.getRight()))
            .collect(Collectors.joining(", ")),
        step.getRenamedColumns().stream()
            .map(p -> String.format(" %s -> %s", p.getLeft(), p.getRight()))
            .collect(Collectors.joining(", ")));
    step.getAddedColumns().forEach(p -> update.addColumn(p.getLeft(), p.getRight()));
    step.getDeletedColumns().forEach(update::deleteColumn);
    step.getChangedColumns().forEach(p -> update.updateColumn(p.getLeft(), p.getRight()));
    step.getRenamedColumns().forEach(p -> update.renameColumn(p.getLeft(), p.getRight()));
    update.commit();
  }
}
