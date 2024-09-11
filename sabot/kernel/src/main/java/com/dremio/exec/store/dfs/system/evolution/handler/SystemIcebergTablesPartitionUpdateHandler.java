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

import com.dremio.exec.store.dfs.system.evolution.step.SystemIcebergTablePartitionUpdateStep;
import com.dremio.exec.store.iceberg.IcebergUtils;
import java.util.stream.Collectors;
import org.apache.iceberg.UpdatePartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles partition updates for system Iceberg tables. This class implements the {@link
 * SystemIcebergTablesUpdateHandler} interface for updating partition-related changes.
 */
public class SystemIcebergTablesPartitionUpdateHandler
    implements SystemIcebergTablesUpdateHandler<
        UpdatePartitionSpec, SystemIcebergTablePartitionUpdateStep> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SystemIcebergTablesPartitionUpdateHandler.class);

  @Override
  public void update(UpdatePartitionSpec update, SystemIcebergTablePartitionUpdateStep step) {
    LOGGER.debug(
        "Applying partition update step for schema version {}. Partition transforms: {}",
        step.getSchemaVersion(),
        step.getPartitionTransforms().stream()
            .map(t -> String.format("[%s]", t))
            .collect(Collectors.joining(",")));

    step.getPartitionTransforms().stream()
        .map(IcebergUtils::getIcebergTerm)
        .forEach(update::addField);
    update.commit();
  }
}
