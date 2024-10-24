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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;

/**
 * {@code SystemIcebergViewMetadataFactory} is a factory class responsible for creating system
 * Iceberg view metadata and checking the supportability of such views.
 */
public final class SystemIcebergViewMetadataFactory {

  public enum SupportedSystemIcebergView {
    COPY_ERRORS_HISTORY("copy_errors_history");

    private final String viewName;

    SupportedSystemIcebergView(String viewName) {
      this.viewName = viewName;
    }

    public String getViewName() {
      return viewName;
    }
  }

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
    if (viewSchemaPath.stream()
        .anyMatch(
            p ->
                p.equalsIgnoreCase(SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName()))) {
      Schema schema = new CopyErrorsHistoryViewSchemaProvider(schemaVersion).getSchema();
      return new CopyErrorsHistoryViewMetadata(
          schema, schemaVersion, SupportedSystemIcebergView.COPY_ERRORS_HISTORY.getViewName());
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
    return Arrays.stream(SupportedSystemIcebergView.values())
        .anyMatch(v -> viewSchemaPath.stream().anyMatch(p -> p.equalsIgnoreCase(v.getViewName())));
  }
}
