/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult.successful;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin2.UpdateStatus;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.collect.ImmutableSet;

/**
 * Handler for <code>REFRESH TABLE tblname</code> command.
 */
public class RefreshTableHandler extends SimpleDirectHandler {

  static final Set<DatasetType> ALTER_METADATA_TYPES = ImmutableSet.of(
      DatasetType.PHYSICAL_DATASET,
      DatasetType.PHYSICAL_DATASET_SOURCE_FILE,
      DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER
      );

  private final SchemaPlus defaultSchema;
  private final CatalogService catalog;

  public RefreshTableHandler(SchemaPlus defaultSchema, CatalogService catalog) {
    this.defaultSchema = defaultSchema;
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlRefreshTable sqlRefreshTable = SqlNodeUtil.unwrap(sqlNode, SqlRefreshTable.class);

    final NamespaceKey tableNSKey = new NamespaceKey(sqlRefreshTable.getTable().names);

    UpdateStatus status = catalog.refreshDataset(tableNSKey);

    final String message;
    switch(status){
    case CHANGED:
      message = "Metadata for table '%s' refreshed.";
      break;
    case DELETED:
      message = "Table '%s' no longer exists, metadata removed.";
      break;
    case UNCHANGED:
      message = "Table '%s' read signature reviewed but source stated metadata is unchanged, no refresh occurred.";
      break;
    default:
      throw new IllegalStateException();
    }

    return singletonList(successful(String.format(message, sqlRefreshTable.getTable().toString())));
  }
}
