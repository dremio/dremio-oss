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

package com.dremio.exec.planner.sql.handlers.direct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlShowTableProperties;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.TableProperties;

/**
 * Handler for show table properties.
 * SHOW TBLPROPERTIES tblName
 */
public class ShowTablePropertiesHandler implements SqlDirectHandler<ShowTablePropertiesHandler.ShowTablePropertiesResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowTablePropertiesHandler.class);

  private final EntityExplorer catalog;
  private final QueryContext context;

  public ShowTablePropertiesHandler(EntityExplorer catalog, QueryContext context){
    super();
    this.catalog = catalog;
    this.context = context;
  }

  @Override
  public List<ShowTablePropertiesResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlShowTableProperties sqlShowTableProperties = SqlNodeUtil.unwrap(sqlNode, SqlShowTableProperties.class);

    IcebergUtils.validateTablePropertiesRequest(context.getOptions());
    try {
      final SqlIdentifier tableId = sqlShowTableProperties.getTableName();
      final NamespaceKey path = new NamespaceKey(tableId.names);
      final DremioTable table = catalog.getTable(path);
      final RelDataType type;
      if (table == null) {
        throw UserException.validationError()
          .message("Unknown table [%s]", path)
          .build(logger);
      } else {
        type = table.getRowType(JavaTypeFactoryImpl.INSTANCE);
      }

      List<TableProperties> tablePropertiesList = fillTablePropertiesList(table);

      List<ShowTablePropertiesHandler.ShowTablePropertiesResult> columns = new ArrayList<>();
      String columnName = null;

      if (tablePropertiesList == null || tablePropertiesList.isEmpty()) {
        return Collections.emptyList();
      }
      for (TableProperties properties : tablePropertiesList) {
        ShowTablePropertiesHandler.ShowTablePropertiesResult result = new ShowTablePropertiesHandler.ShowTablePropertiesResult(properties.getTablePropertyName(),
          properties.getTablePropertyValue());
        columns.add(result);
      }
      return columns;
    } catch (Exception ex) {
      throw UserException.invalidMetadataError(ex)
        .message("Error while getting metadata via SHOW TABLEPROPERTIES query: %s", ex.getMessage())
        .build(logger);
    }
  }

  private List<TableProperties> fillTablePropertiesList(DremioTable table) {
    TableMetadata dataset = table.getDataset();
    if (dataset == null) {
      return Collections.emptyList();
    }

    DatasetConfig datasetConfig = dataset.getDatasetConfig();
    if (datasetConfig == null) {
      return Collections.emptyList();
    }

    PhysicalDataset physicalDataset = datasetConfig.getPhysicalDataset();
    if (physicalDataset == null) {
      return Collections.emptyList();
    }

    IcebergMetadata icebergMetadata = physicalDataset.getIcebergMetadata();
    if (icebergMetadata == null || icebergMetadata.getTablePropertiesList() == null) {
      return Collections.emptyList();
    } else {
      return icebergMetadata.getTablePropertiesList();
    }
  }

  @Override
  public Class<ShowTablePropertiesResult> getResultType() {
    return ShowTablePropertiesResult.class;
  }

  public static class ShowTablePropertiesResult {
    public final String TABLE_PROPERTY_NAME;
    public final String TABLE_PROPERTY_VALUE;

    public ShowTablePropertiesResult(String tablePropertyName, String tablePropertyValue) {
      super();
      TABLE_PROPERTY_NAME = tablePropertyName;
      TABLE_PROPERTY_VALUE = tablePropertyValue;
    }
  }
}
