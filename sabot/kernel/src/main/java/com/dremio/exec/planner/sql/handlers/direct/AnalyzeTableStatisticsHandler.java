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

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlAnalyzeTableStatistics;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.service.namespace.NamespaceKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.sql.SqlNode;

/** Handler for <code>ANALYZE TABLE</code> command. */
public class AnalyzeTableStatisticsHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AnalyzeTableStatisticsHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;
  private final StatisticsAdministrationService.Factory statisticsAdministrationFactory;

  public AnalyzeTableStatisticsHandler(
      Catalog catalog,
      SqlHandlerConfig config,
      StatisticsAdministrationService.Factory statisticsAdministrationFactory) {
    this.catalog = catalog;
    this.config = config;
    this.statisticsAdministrationFactory = statisticsAdministrationFactory;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAnalyzeTableStatistics sqlAnalyzeTableStatistics =
        SqlNodeUtil.unwrap(sqlNode, SqlAnalyzeTableStatistics.class);
    final NamespaceKey path =
        catalog.resolveSingle(new NamespaceKey(sqlAnalyzeTableStatistics.getTable().names));
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    final StatisticsAdministrationService statisticsAdministrationService =
        statisticsAdministrationFactory.get(config.getContext().getQueryUserName());
    statisticsAdministrationService.validate(path);

    boolean isAnalysis = sqlAnalyzeTableStatistics.isAnalyze().booleanValue();
    final DremioTable table = catalog.getTable(path);
    if (table == null) {
      throw UserException.validationError()
          .message("Cannot find the requested table: %s", path.toString())
          .build(logger);
    }

    final List<Field> fields = table.getSchema().getFields();
    final Set<Field> supportedFields = new LinkedHashSet<>();
    final Map<String, Field> fieldMap = new HashMap<>();
    fields.forEach(f -> fieldMap.put(f.getName().toLowerCase(), f));
    boolean isAllColumns = false;
    final Set<String> columns = new LinkedHashSet<>();
    if (sqlAnalyzeTableStatistics.getColumns().getList().isEmpty()) { // all columns
      isAllColumns = true;
      for (Field field : fields) {
        if (!field.getName().equals(UPDATE_COLUMN) && isSupportedType(field.getFieldType())) {
          supportedFields.add(field);
        }
      }
    } else {
      Set<String> invalidColumns = new LinkedHashSet<>();
      for (SqlNode node : sqlAnalyzeTableStatistics.getColumns().getList()) {
        if (!fieldMap.containsKey(node.toString().toLowerCase())) {
          invalidColumns.add(node.toString().toLowerCase());
        } else {
          supportedFields.add(fieldMap.get(node.toString().toLowerCase()));
        }
      }
      if (!invalidColumns.isEmpty()) {
        throw UserException.validationError()
            .message(
                "The following column(s) were not found in the table %s: %s.",
                path.toString(), invalidColumns.toString())
            .build(logger);
      }
    }
    if (isAnalysis) { // compute statistics
      long maxColumnLimit = plannerSettings.getStatisticsMaxColumnLimit();
      if (supportedFields.size() > maxColumnLimit) {
        return Collections.singletonList(
            SimpleCommandResult.successful(
                "The number of columns requested exceeds the the limit set by the administrator, "
                    + maxColumnLimit
                    + "."));
      }

      Double samplingRate =
          Optional.ofNullable(table.getStatistic().getRowCount())
              .filter(rowCount -> rowCount > plannerSettings.getStatisticsSamplingThreshold())
              .map(rowCount -> plannerSettings.getStatisticsSamplingRate())
              .orElse(null);

      String id =
          statisticsAdministrationService.requestStatistics(
              new ArrayList<>(supportedFields), path, samplingRate);
      return Collections.singletonList(
          SimpleCommandResult.successful(String.format("Requested with a job ID: %s", id)));
    } else { // delete statistics
      final List<String> failed =
          statisticsAdministrationFactory
              .get(config.getContext().getQueryUserName())
              .deleteStatistics(
                  supportedFields.stream()
                      .map(f -> f.getName().toLowerCase())
                      .collect(Collectors.toList()),
                  path);
      final StringBuilder sb =
          new StringBuilder("Statistics Removed. Requested columns: ").append(columns);
      if (failed != null && failed.size() != 0) {
        sb.append(", Non-existent columns: ").append(failed);
      }
      if (isAllColumns) {
        // deleteRowCount Statistics
        boolean succeed =
            statisticsAdministrationFactory
                .get(config.getContext().getQueryUserName())
                .deleteRowCountStatistics(path);
        if (!succeed) {
          logger.warn("Deletion of RowCount Statistics Failed for table {}", path.toString());
        }
      }
      return Collections.singletonList(SimpleCommandResult.successful(sb.toString()));
    }
  }

  private boolean isSupportedType(FieldType fieldType) {
    switch (fieldType.getType().getTypeID()) {
      case Struct:
      case List:
      case LargeList:
      case FixedSizeList:
      case Union:
      case Map:
        return false;
      default:
        return true;
    }
  }
}
