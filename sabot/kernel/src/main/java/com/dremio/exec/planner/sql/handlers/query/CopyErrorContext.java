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

package com.dremio.exec.planner.sql.handlers.query;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.ops.DelegatingPlannerCatalog;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import java.util.List;
import java.util.UUID;

/**
 * Internal structure to hold input parameters/options for 'COPY_ERRORS' table function. Also
 * validates user input for target table name and original COPY INTO jobId.
 */
public final class CopyErrorContext {

  private final SimpleCatalog<?> catalog;
  private final String targetTableName;
  private final String copyIntoJobId;
  private final boolean strictConsistency;
  private DremioPrepareTable resolvedTargetTable;

  public CopyErrorContext(
      SimpleCatalog<?> catalog,
      String targetTableName,
      String copyIntoJobId,
      boolean strictConsistency) {
    this.catalog = catalog;
    validateTargetTableName(targetTableName);
    this.targetTableName = targetTableName;
    validateJobId(copyIntoJobId);
    this.copyIntoJobId = copyIntoJobId;
    this.strictConsistency = strictConsistency;
  }

  private void validateTargetTableName(String targetTableName) {
    DremioCatalogReader catalogReader =
        new DremioCatalogReader(DelegatingPlannerCatalog.newInstance(catalog));
    try {
      List<String> tablePath = VersionedTableMacro.splitTableIdentifier(targetTableName);
      resolvedTargetTable = catalogReader.getTable(tablePath);
      if (resolvedTargetTable == null) {
        throw UserException.resourceError()
            .message("Unable to find target table %s", targetTableName)
            .buildSilently();
      }
    } catch (IllegalArgumentException e) {
      throw UserException.parseError(e)
          .message("Invalid table identifier %s", targetTableName)
          .buildSilently();
    }
  }

  private void validateJobId(String copyIntoJobId) {
    if (copyIntoJobId == null) {
      return;
    }
    try {
      UUID.fromString(copyIntoJobId);
    } catch (IllegalArgumentException e) {
      throw UserException.parseError()
          .message("JobID must be a UUID " + e.getMessage())
          .buildSilently();
    }
  }

  public String getTargetTableName() {
    return targetTableName;
  }

  public String getCopyIntoJobId() {
    return copyIntoJobId;
  }

  public DremioPrepareTable getResolvedTargetTable() {
    return resolvedTargetTable;
  }

  public boolean isStrictConsistency() {
    return strictConsistency;
  }
}
