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

package com.dremio.service.reflection;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Visitor that checks if a logical plan can support incremental update. The supported pattern right
 * now is a plan that contains only ExpansionNode, Filters, Projects, Scans, Sorts and Aggregates.
 * There can only be one Aggregate in the plan, the Sort must not have any FETCH and OFFSET, and the
 * Scan must support incremental update.
 */
public class IncrementalChecker extends RoutingShuttle {
  private static final Logger logger = LoggerFactory.getLogger(IncrementalChecker.class);

  private final ReflectionSettings reflectionSettings;
  private final ReflectionService service;
  private final OptionManager optionManager;

  private RelNode unsupportedOperator = null;
  private final List<SqlAggFunction> unsupportedAggregates = new ArrayList<>();
  private boolean isIncremental = false;
  private int aggCount = 0;
  private String fullRefreshReason = "";

  IncrementalChecker(
      ReflectionSettings reflectionSettings,
      ReflectionService service,
      OptionManager optionManager) {
    this.reflectionSettings =
        Preconditions.checkNotNull(reflectionSettings, "reflection settings required");
    this.service = Preconditions.checkNotNull(service, "reflection service required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
  }

  protected void setFullRefreshReason(final String fullRefreshReason) {
    this.fullRefreshReason = fullRefreshReason;
  }

  protected Logger getLogger() {
    return logger;
  }

  public boolean isIncremental() {
    if (unsupportedOperator != null) {
      fullRefreshReason =
          String.format(
              "Cannot do incremental update because %s does not support incremental update",
              unsupportedOperator.getRelTypeName());
      logger.debug(fullRefreshReason);
      return false;
    }

    if (!unsupportedAggregates.isEmpty()) {
      fullRefreshReason =
          String.format(
              "Cannot do incremental update because Aggregate operator has unsupported aggregate functions: %s",
              unsupportedAggregates);
      logger.debug(fullRefreshReason);
      return false;
    }

    if (aggCount > 1) {
      fullRefreshReason =
          "Cannot do incremental update because the reflection has multiple aggregate operators";
      logger.debug(fullRefreshReason);
      return false;
    }

    if (!isIncremental) {
      fullRefreshReason =
          "Cannot do incremental update because the table is not incrementally updatable";
      logger.debug(fullRefreshReason);
    }

    return isIncremental;
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof ExpansionNode) {
      return visitChild(other, 0, other.getInput(0));
    }
    if (unsupportedOperator == null) {
      unsupportedOperator = other;
    }
    return other;
  }

  @Override
  public RelNode visit(TableScan tableScan) {
    List<String> tablePath = tableScan.getTable().getQualifiedName();
    NamespaceKey tableKey = new NamespaceKey(tablePath);
    // If the scan is over a reflection inherit its refresh method.
    // Search the ReflectionService using the ReflectionId.
    if (tableKey.getRoot().equals(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME)) {
      Optional<ReflectionEntry> entry = service.getEntry(new ReflectionId(tablePath.get(1)));
      isIncremental = entry.get().getRefreshMethod() == RefreshMethod.INCREMENTAL;
    } else {
      DremioTable table = tableScan.getTable().unwrap(DremioTable.class);
      final CatalogEntityKey.Builder builder =
          CatalogEntityKey.newBuilder().keyComponents(table.getPath().getPathComponents());
      if (table.getDataset().getVersionContext() != null) {
        builder.tableVersionContext(table.getDataset().getVersionContext());
      }
      if (IncrementalUpdateServiceUtils.isIncrementalRefreshBySnapshotEnabled(
          table.getDatasetConfig(), optionManager)) {
        isIncremental = true;
      } else {
        final CatalogEntityKey catalogEntityKey = builder.build();
        final AccelerationSettings settings =
            reflectionSettings.getReflectionSettings(catalogEntityKey);
        isIncremental = settings.getMethod() == RefreshMethod.INCREMENTAL;
      }
    }
    return tableScan;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    aggCount++;
    aggregate
        .getAggCallList()
        .forEach(
            a -> {
              if (!canRollUp(a.getAggregation())) {
                unsupportedAggregates.add(a.getAggregation());
              }
            });
    return visitChild(aggregate, 0, aggregate.getInput());
  }

  private static boolean canRollUp(final SqlAggFunction aggregation) {
    final SqlKind kind = aggregation.getKind();
    return kind == SqlKind.SUM
        || kind == SqlKind.SUM0
        || kind == SqlKind.MIN
        || kind == SqlKind.MAX
        || kind == SqlKind.COUNT
        || DremioSqlOperatorTable.HLL.getName().equals(aggregation.getName());
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    if (sort.fetch == null && sort.offset == null) {
      return visitChild(sort, 0, sort.getInput());
    }
    if (unsupportedOperator == null) {
      unsupportedOperator = sort;
    }
    return sort;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    return visitChild(project, 0, project.getInput());
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    return visitChild(filter, 0, filter.getInput());
  }

  public String getFullRefreshReason() {
    return fullRefreshReason;
  }

  protected OptionManager getOptionManager() {
    return optionManager;
  }

  protected RelNode getUnsupportedOperator() {
    return unsupportedOperator;
  }

  protected void setUnsupportedOperator(RelNode unsupportedOperator) {
    this.unsupportedOperator = unsupportedOperator;
  }

  protected ReflectionService getReflectionService() {
    return service;
  }

  protected ReflectionSettings getReflectionSettings() {
    return reflectionSettings;
  }
}
