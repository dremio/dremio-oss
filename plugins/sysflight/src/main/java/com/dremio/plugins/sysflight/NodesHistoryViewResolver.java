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
package com.dremio.plugins.sysflight;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.Views;
import com.dremio.exec.store.easy.text.compliant.TextColumnNameGenerator;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.options.OptionManager;
import com.dremio.plugins.nodeshistory.NodesHistoryTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Provider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;

/**
 * Supplies the view exposed by the "sys" source to enable viewing historical information on cluster
 * nodes.
 */
public class NodesHistoryViewResolver implements ViewResolver {
  private static final NodesHistoryTable TABLE = NodesHistoryTable.METRICS;
  private static final List<String> VIEW_PATH = List.of("sys", "nodes_recent");

  private final Provider<OptionManager> optionManagerProvider;

  public NodesHistoryViewResolver(Provider<OptionManager> optionManagerProvider) {
    this.optionManagerProvider = optionManagerProvider;
  }

  @Override
  public boolean isSupportedView(List<String> path) {
    if (!optionManagerProvider.get().getOption(ExecConstants.NODE_HISTORY_ENABLED)) {
      return false;
    }
    List<String> pathComponents = getNamespaceKey().getPathComponents();
    return path.size() == pathComponents.size()
        && IntStream.range(0, pathComponents.size())
            .boxed()
            .allMatch(i -> pathComponents.get(i).equalsIgnoreCase(path.get(i)));
  }

  @Override
  public ViewTable getViewTable() {
    BatchSchema schema = buildSchema();
    View view =
        Views.fieldTypesToView(
            getViewName(), prepareViewQuery(), ViewFieldsHelper.getBatchSchemaFields(schema), null);
    return new ViewTable(
        getNamespaceKey(), view, CatalogUser.from(SystemUser.SYSTEM_USERNAME), schema);
  }

  public NamespaceKey getNamespaceKey() {
    return new NamespaceKey(VIEW_PATH);
  }

  public String getViewName() {
    return getNamespaceKey().getName();
  }

  private static BatchSchema buildSchema() {
    return BatchSchema.newBuilder().addFields(TABLE.getSchema()).build();
  }

  private static String prepareViewQuery() {
    List<String> projections =
        IntStream.range(0, TABLE.getSchema().size())
            .boxed()
            .map(NodesHistoryViewResolver::createProjection)
            .collect(Collectors.toList());
    // We need to select DISTINCT because the compaction implementation has a race condition in that
    // for a brief period, records may be duplicated between the compacted CSV and the
    // not-yet-deleted raw CSVs
    return String.format(
        "SELECT DISTINCT %s FROM %s",
        StringUtils.join(projections, ", "), StringUtils.join(TABLE.getPath(), "."));
  }

  private static String createProjection(int columnIndex) {
    String originalFieldName = TextColumnNameGenerator.columnNameForIndex(columnIndex);
    Field field = TABLE.getSchema().get(columnIndex);
    String outputFieldName = field.getName();
    ArrowType type = field.getType();
    String sqlType;
    switch (type.getTypeID()) {
      case Timestamp:
        sqlType = "TIMESTAMP";
        break;
      case Utf8:
        sqlType = "VARCHAR";
        break;
      case Bool:
        sqlType = "BOOLEAN";
        break;
      case FloatingPoint:
        {
          FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
          if (precision != FloatingPointPrecision.SINGLE) {
            throw new IllegalStateException(
                String.format(
                    "Unsupported floating point precision for %s column: %s",
                    outputFieldName, precision.name()));
          }
          sqlType = "FLOAT";
          break;
        }
      default:
        throw new IllegalStateException(
            String.format("Unsupported column type for %s column: %s", outputFieldName, type));
    }
    // Quote output field names since they may be reserved keywords (e.g. "timestamp")
    String quotedOutputField = String.format("\"%s\"", outputFieldName);
    // All projected fields need to be cast to the expected type since CSV by default is all VARCHAR
    // CAST doesn't handle converting string "null" to another type so it must be explicitly handled
    String fieldExpression =
        field.isNullable()
            ? String.format(
                "CASE WHEN UPPER(TRIM(%s)) = 'NULL' THEN NULL ELSE %s END",
                originalFieldName, originalFieldName)
            : originalFieldName;
    return String.format("CAST(%s AS %s) AS %s", fieldExpression, sqlType, quotedOutputField);
  }
}
