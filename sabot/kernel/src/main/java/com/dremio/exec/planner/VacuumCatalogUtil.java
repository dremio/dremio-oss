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
package com.dremio.exec.planner;

import static org.apache.iceberg.DremioTableProperties.NESSIE_GC_ENABLED;

import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;

/** Utility methods related to VACUUM CATALOG. */
public class VacuumCatalogUtil {
  private VacuumCatalogUtil() {}

  /**
   * Checks if garbage collection is enabled for an Iceberg table. Checks for the Dremio specific
   * nessie.gc.enabled property.
   *
   * @param icebergTableProperties A map containing Iceberg table properties.
   * @return True if garbage collection is enabled for the Iceberg table, false otherwise.
   */
  public static boolean isGCEnabledForIcebergTable(Map<String, String> icebergTableProperties) {
    return PropertyUtil.propertyAsBoolean(icebergTableProperties, NESSIE_GC_ENABLED, true);
  }
}
