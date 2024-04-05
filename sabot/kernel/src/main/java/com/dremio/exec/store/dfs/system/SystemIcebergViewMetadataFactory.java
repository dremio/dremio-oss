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
package com.dremio.exec.store.dfs.system;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.dfs.copyinto.CopyErrorsHistoryViewMetadata;
import com.dremio.exec.store.dfs.copyinto.CopyErrorsHistoryViewSchemaProvider;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.iceberg.Schema;

/**
 * {@code SystemIcebergViewMetadataFactory} is a factory class responsible for creating system
 * Iceberg view metadata and checking the supportability of such views.
 */
public final class SystemIcebergViewMetadataFactory {

  /** The name of the "copy errors history" system Iceberg view. */
  public static final String COPY_ERRORS_HISTORY_VIEW_NAME = "copy_errors_history";

  private static final List<String> SUPPORTED_VIEWS =
      ImmutableList.of(COPY_ERRORS_HISTORY_VIEW_NAME);

  /**
   * Constructs a new instance of the factory class. (Private constructor to prevent instantiation)
   */
  private SystemIcebergViewMetadataFactory() {}

  /**
   * Get the metadata for a system Iceberg view based on the provided option manager and view schema
   * path.
   *
   * @param optionManager The {@link OptionManager} containing system Iceberg tables schema version.
   * @param viewSchemaPath The path to the system Iceberg view.
   * @return The {@link SystemIcebergViewMetadata} instance for the provided view.
   * @throws IllegalArgumentException If the view schema path is not supported.
   */
  public static SystemIcebergViewMetadata getViewMetadata(
      OptionManager optionManager, List<String> viewSchemaPath) {
    long schemaVersion =
        optionManager.getOption(ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION);
    if (viewSchemaPath.stream().anyMatch(COPY_ERRORS_HISTORY_VIEW_NAME::equalsIgnoreCase)) {
      Schema schema = CopyErrorsHistoryViewSchemaProvider.getSchema(schemaVersion);
      return new CopyErrorsHistoryViewMetadata(
          schema, schemaVersion, COPY_ERRORS_HISTORY_VIEW_NAME);
    }
    throw new IllegalArgumentException("Invalid system Iceberg view: " + viewSchemaPath);
  }

  /**
   * Check if the provided view schema path represents a supported system Iceberg view.
   *
   * @param viewSchemaPath The path to the system Iceberg view.
   * @return {@code true} if the view is supported; otherwise, {@code false}.
   */
  public static boolean isSupportedViewPath(List<String> viewSchemaPath) {
    return SUPPORTED_VIEWS.stream().anyMatch(v -> viewSchemaPath.stream().anyMatch(v::equals));
  }
}
