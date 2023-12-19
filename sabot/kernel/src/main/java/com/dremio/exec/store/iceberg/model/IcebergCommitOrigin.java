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
package com.dremio.exec.store.iceberg.model;

import java.util.Locale;

import com.dremio.exec.catalog.VersionedPlugin.EntityType;
import com.google.common.base.Preconditions;

/**
 * Info about the origin of an iceberg commit i.e. CREATE_VIEW, INSERT_TABLE. Will be used to
 * generate an informative commit message in nessie. Use READ_ONLY to indicate no intention of
 * performing a commit.
 */
public enum IcebergCommitOrigin {
  /**
   * indicates that the operation is not supposed to commit anything and will cause an error if
   * tried anyway
   */
  READ_ONLY,
  CREATE_TABLE,
  DROP_TABLE,
  ALTER_TABLE,
  INSERT_TABLE,
  /** deletes data from table, do not confuse with DROP_TABLE */
  DELETE_TABLE,
  MERGE_TABLE,
  UPDATE_TABLE,
  TRUNCATE_TABLE,
  ROLLBACK_TABLE,
  EXPIRE_SNAPSHOTS,
  OPTIMIZE_REWRITE_DATA_TABLE,
  OPTIMIZE_REWRITE_MANIFESTS_TABLE,
  FULL_METADATA_REFRESH,
  INCREMENTAL_METADATA_REFRESH,
  CREATE_VIEW,
  ALTER_VIEW,
  DROP_VIEW,
  ;

  public static IcebergCommitOrigin fromCommandType(IcebergCommandType commandType) {
    switch (commandType) {
    case CREATE:
      return CREATE_TABLE;
    case INSERT:
      return INSERT_TABLE;
    case MERGE:
      return MERGE_TABLE;
    case UPDATE:
      return UPDATE_TABLE;
    case DELETE:
      return DELETE_TABLE;
    case TRUNCATE:
      return TRUNCATE_TABLE;
    case METADATA:
      return ALTER_TABLE;
    case FULL_METADATA_REFRESH:
      return FULL_METADATA_REFRESH;
    case INCREMENTAL_METADATA_REFRESH:
    case PARTIAL_METADATA_REFRESH:
      return INCREMENTAL_METADATA_REFRESH;
    case OPTIMIZE:
      return OPTIMIZE_REWRITE_DATA_TABLE;
    case ROLLBACK:
      return ROLLBACK_TABLE;
    case VACUUM:
    default:
      throw new IllegalArgumentException("Unable to translate commandType: " + commandType);
    }
  }

  private EntityType getEntityType() {
    switch (this) {
    case CREATE_VIEW:
    case ALTER_VIEW:
    case DROP_VIEW:
      return EntityType.ICEBERG_VIEW;
    default:
      return EntityType.ICEBERG_TABLE;
    }
  }

  private boolean isCreateOrDrop() {
    switch (this) {
    case CREATE_TABLE:
    case DROP_TABLE:
    case CREATE_VIEW:
    case DROP_VIEW:
      return true;
    default:
      return false;
    }
  }

  public String createCommitMessage(String catalogKey, EntityType entityType) {
    Preconditions.checkState(this != READ_ONLY);
    Preconditions.checkArgument(
      entityType == EntityType.ICEBERG_TABLE || entityType == EntityType.ICEBERG_VIEW,
      "Unsupported entity type %s for commit origin %s", entityType, this
    );
    Preconditions.checkArgument(
      entityType.equals(getEntityType()),
      "Mismatched entity type %s for commit origin %s", entityType, this
    );
    String operation = name().toUpperCase(Locale.ROOT).replace('_', ' ');
    if (!isCreateOrDrop()) {
      if (entityType == EntityType.ICEBERG_TABLE) {
        if (operation.contains("TABLE")) {
          operation = operation.replaceFirst("TABLE", "on TABLE");
        } else {
          operation += " on TABLE";
        }
      } else {
        if (operation.contains("VIEW")) {
          operation = operation.replaceFirst("VIEW", "on VIEW");
        } else {
          operation += " on VIEW";
        }
      }
    }
    return operation + " " + catalogKey;
  }
}
