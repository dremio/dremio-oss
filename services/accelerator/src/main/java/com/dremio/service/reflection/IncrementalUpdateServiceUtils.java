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

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL;

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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.OptionManager;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Preconditions;

/**
 * Contains various utilities for acceleration incremental updates
 */
public class IncrementalUpdateServiceUtils {

  public static final class RefreshDetails {
    private RefreshMethod refreshMethod;
    private String refreshField;
    private boolean snapshotBased;
    private TableMetadata baseTableMetadata;
    private String baseTableSnapshotId;
    private final String fullRefreshReason;
    public RefreshDetails(
      RefreshMethod refreshMethod,
      String refreshField,
      boolean snapshotBased,
      TableMetadata baseTableMetadata,
      String baseTableSnapshotId,
      final String fullRefreshReason) {
      this.refreshMethod = refreshMethod;
      this.refreshField = refreshField;
      this.snapshotBased = snapshotBased;
      this.baseTableMetadata = baseTableMetadata;
      this.baseTableSnapshotId = baseTableSnapshotId;
      this.fullRefreshReason = fullRefreshReason;
    }

    public RefreshMethod getRefreshMethod() {
      return refreshMethod;
    }

    public String getRefreshField() {
      return refreshField;
    }

    public boolean getSnapshotBased() {
      return snapshotBased;
    }

    public TableMetadata getBaseTableMetadata() {
      return baseTableMetadata;
    }
    public String getBaseTableSnapshotId() {
      return baseTableSnapshotId;
    }

    public String getFullRefreshReason(){
      return fullRefreshReason;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(IncrementalUpdateServiceUtils.class);

  /**
   * compute acceleration settings from refresh details
   */
  public static AccelerationSettings extractRefreshSettings(
    RefreshDetails refreshDetails) {
    return new AccelerationSettings()
      .setMethod(refreshDetails.getRefreshMethod())
      .setRefreshField(refreshDetails.getRefreshField())
      .setSnapshotBased(refreshDetails.getSnapshotBased());
  }

  /**
   * compute refresh details from the plan
   */
 public static RefreshDetails extractRefreshDetails(
   final RelNode normalizedPlan,
   ReflectionSettings reflectionSettings,
   ReflectionService service,
   OptionManager optionManager,
   boolean isRebuildPlan,
   ReflectionEntry entry) {
    final Pointer<String> fullRefreshReason = new Pointer<>();
    final boolean incremental = getIncremental(normalizedPlan, reflectionSettings, service, optionManager, fullRefreshReason);

    // When rebuilding the logical plan during upgrade, use the same refresh method of existing materialization.
    if (isRebuildPlan) {
      return new RefreshDetails(entry.getRefreshMethod(), entry.getRefreshField(), entry.getSnapshotBased(), null, null, null);
    }
    if (!incremental) {
      return new RefreshDetails(RefreshMethod.FULL, null, false, null, null, fullRefreshReason.value);
    } else {
      return findIncrementalRefreshDetails(normalizedPlan, reflectionSettings, service, optionManager);
    }
  }

  private static RefreshDetails findIncrementalRefreshDetails(
      RelNode plan,
      final ReflectionSettings reflectionSettings,
      ReflectionService service,
      OptionManager optionManager) {
    final Pointer<String> refreshField = new Pointer<>();
    final Pointer<Boolean> snapshotBased = new Pointer<>(false);
    final Pointer<TableMetadata> baseTableMetadata = new Pointer<>();
    final Pointer<String> baseTableSnapshotId = new Pointer<>();
    plan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(TableScan tableScan) {
        List<String> tablePath = tableScan.getTable().getQualifiedName();
        NamespaceKey tableKey = new NamespaceKey(tablePath);
        // If the scan is over a reflection inherit its refresh field.
        // Search the ReflectionService using the ReflectionId.
        if (tableKey.getRoot().equals(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME)) {
          Optional<ReflectionEntry> entry = service.getEntry(new ReflectionId(tablePath.get(1)));
          refreshField.value = entry.get().getRefreshField();
        } else {
          DremioTable table = tableScan.getTable().unwrap(DremioTable.class);
          final CatalogEntityKey.Builder builder =
            CatalogEntityKey.newBuilder().keyComponents(table.getPath().getPathComponents());
          if (table.getDataset().getVersionContext() != null) {
            builder.tableVersionContext(table.getDataset().getVersionContext());
          }
          final CatalogEntityKey catalogEntityKey = builder.build();
          final AccelerationSettings settings = reflectionSettings.getReflectionSettings(catalogEntityKey);

          Optional.ofNullable(table.getDatasetConfig())
            .filter(c -> isIncrementalRefreshBySnapshotEnabled(c, optionManager))
            .map(DatasetConfig::getPhysicalDataset)
            .map(PhysicalDataset::getIcebergMetadata)
            .map(IcebergMetadata::getSnapshotId)
            .ifPresent(snapshotId -> {
              snapshotBased.value = true;
              baseTableMetadata.value = table.getDataset();
              baseTableSnapshotId.value = snapshotId.toString();
            });

          refreshField.value = settings.getRefreshField();
        }
        return tableScan;
      }
    });
    return new RefreshDetails(RefreshMethod.INCREMENTAL, refreshField.value, snapshotBased.value, baseTableMetadata.value, baseTableSnapshotId.value, "");
  }

  /**
   * Check if a plan can support incremental update
   */
  private static boolean getIncremental(final RelNode plan,
                                        final ReflectionSettings reflectionSettings,
                                        final ReflectionService service,
                                        final OptionManager optionManager,
                                        final Pointer<String> fullRefreshReason) {
    IncrementalChecker checker = new IncrementalChecker(reflectionSettings, service, optionManager);
    plan.accept(checker);
    boolean result = checker.isIncremental();
    fullRefreshReason.value =  checker.getFullRefreshReason();
    return result;
  }

  public static boolean isIncrementalRefreshBySnapshotEnabled(DatasetConfig datasetConfig, OptionManager optionManager){
    // Incremental refresh is not available for file-based datasets.
    if (datasetConfig.getType() == PHYSICAL_DATASET_SOURCE_FILE) {
      return false;
    }

    // REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED and REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL
    // override the dataset refresh method settings for their appropriate datasets
    return (optionManager.getOption(REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED) &&
      DatasetHelper.isIcebergDataset(datasetConfig))
      || (optionManager.getOption(REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL) &&
      DatasetHelper.isInternalIcebergTable(datasetConfig));
  }

  /**
   * Visitor that checks if a logical plan can support incremental update. The supported pattern right now is a plan
   * that contains only ExpansionNode, Filters, Projects, Scans, Sorts and Aggregates.
   * There can only be one Aggregate in the plan, the Sort must not have any FETCH and OFFSET, and the
   * Scan most support incremental update.
   */
  private static class IncrementalChecker extends RoutingShuttle {
    private final ReflectionSettings reflectionSettings;
    private final ReflectionService service;
    private final OptionManager optionManager;

    private RelNode unsupportedOperator = null;
    private List<SqlAggFunction> unsupportedAggregates = new ArrayList<>();
    private boolean isIncremental = false;
    private int aggCount = 0;
    private String fullRefreshReason = "";

    IncrementalChecker(ReflectionSettings reflectionSettings, ReflectionService service, OptionManager optionManager) {
      this.reflectionSettings = Preconditions.checkNotNull(reflectionSettings, "reflection settings required");
      this.service = Preconditions.checkNotNull(service,"reflection service required");
      this.optionManager = Preconditions.checkNotNull(optionManager,"option manager required");
    }

    public boolean isIncremental() {
      if (unsupportedOperator != null) {
        fullRefreshReason = String.format("Cannot do incremental update because %s does not support incremental update", unsupportedOperator.getRelTypeName());
        logger.debug(fullRefreshReason);
        return false;
      }

      if (!unsupportedAggregates.isEmpty()) {
        fullRefreshReason = String.format("Cannot do incremental update because Aggregate operator has unsupported aggregate functions: %s", unsupportedAggregates);
        logger.debug(fullRefreshReason);
        return false;
      }

      if (aggCount > 1) {
        fullRefreshReason = "Cannot do incremental update because the reflection has multiple aggregate operators";
        logger.debug(fullRefreshReason);
        return false;
      }

      if (!isIncremental) {
        fullRefreshReason = "Cannot do incremental update because the table is not incrementally updatable";
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
        if (isIncrementalRefreshBySnapshotEnabled(table.getDatasetConfig(), optionManager)) {
            isIncremental = true;
        } else {
          final CatalogEntityKey catalogEntityKey = builder.build();
          final AccelerationSettings settings = reflectionSettings.getReflectionSettings(catalogEntityKey);
          isIncremental = settings.getMethod() == RefreshMethod.INCREMENTAL;
        }
      }
      return tableScan;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
      aggCount++;
      aggregate.getAggCallList().forEach(a -> {
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
      if(sort.fetch == null && sort.offset == null) {
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
  }

}
