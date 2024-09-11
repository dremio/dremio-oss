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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlOperator;

/** Contains various utilities for acceleration incremental updates */
public class IncrementalUpdateServiceUtils {
  private static final String INCREMENTAL_CHECKER =
      "dremio.reflection.refresh.incremental-checker.class";

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

    public String getFullRefreshReason() {
      return fullRefreshReason;
    }
  }

  /** compute acceleration settings from refresh details */
  public static AccelerationSettings extractRefreshSettings(RefreshDetails refreshDetails) {
    return new AccelerationSettings()
        .setMethod(refreshDetails.getRefreshMethod())
        .setRefreshField(refreshDetails.getRefreshField())
        .setSnapshotBased(refreshDetails.getSnapshotBased());
  }

  /** compute refresh details from the plan */
  public static RefreshDetails extractRefreshDetails(
      final RelNode normalizedPlan,
      ReflectionSettings reflectionSettings,
      ReflectionService service,
      OptionManager optionManager,
      final SabotConfig sabotConfig,
      List<SqlOperator> nonIncrementalRefreshFunctions) {

    final Pointer<String> fullRefreshReason = new Pointer<>();
    final boolean incremental;
    if (!nonIncrementalRefreshFunctions.isEmpty()) {
      incremental = false;
      fullRefreshReason.value =
          "Cannot do incremental update because the reflection has dynamic function(s): "
              + nonIncrementalRefreshFunctions.stream()
                  .map(SqlOperator::getName)
                  .collect(Collectors.joining(", "));
    } else {
      incremental =
          getIncremental(
              normalizedPlan,
              reflectionSettings,
              service,
              optionManager,
              fullRefreshReason,
              sabotConfig);
    }

    if (!incremental) {
      return new RefreshDetails(
          RefreshMethod.FULL, null, false, null, null, fullRefreshReason.value);
    } else {
      return findIncrementalRefreshDetails(
          normalizedPlan, reflectionSettings, service, optionManager);
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
    plan.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(TableScan tableScan) {
            List<String> tablePath = tableScan.getTable().getQualifiedName();
            NamespaceKey tableKey = new NamespaceKey(tablePath);
            // If the scan is over a reflection inherit its refresh field.
            // Search the ReflectionService using the ReflectionId.
            if (tableKey.getRoot().equals(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME)) {
              Optional<ReflectionEntry> entry =
                  service.getEntry(new ReflectionId(tablePath.get(1)));
              refreshField.value = entry.get().getRefreshField();
            } else {
              DremioTable table = tableScan.getTable().unwrap(DremioTable.class);
              final CatalogEntityKey.Builder builder =
                  CatalogEntityKey.newBuilder().keyComponents(table.getPath().getPathComponents());
              if (table.getDataset().getVersionContext() != null) {
                builder.tableVersionContext(table.getDataset().getVersionContext());
              }
              final CatalogEntityKey catalogEntityKey = builder.build();
              final AccelerationSettings settings =
                  reflectionSettings.getReflectionSettings(catalogEntityKey);

              Optional.ofNullable(table.getDatasetConfig())
                  .filter(c -> isIncrementalRefreshBySnapshotEnabled(c, optionManager))
                  .map(DatasetConfig::getPhysicalDataset)
                  .map(PhysicalDataset::getIcebergMetadata)
                  .map(IcebergMetadata::getSnapshotId)
                  .ifPresent(
                      snapshotId -> {
                        snapshotBased.value = true;
                        baseTableMetadata.value = table.getDataset();
                        baseTableSnapshotId.value = snapshotId.toString();
                      });

              refreshField.value = settings.getRefreshField();
            }
            return tableScan;
          }
        });
    return new RefreshDetails(
        RefreshMethod.INCREMENTAL,
        refreshField.value,
        snapshotBased.value,
        baseTableMetadata.value,
        baseTableSnapshotId.value,
        "");
  }

  /** Check if a plan can support incremental update */
  private static boolean getIncremental(
      final RelNode plan,
      final ReflectionSettings reflectionSettings,
      final ReflectionService service,
      final OptionManager optionManager,
      final Pointer<String> fullRefreshReason,
      final SabotConfig sabotConfig) {
    IncrementalChecker defaultChecker =
        new IncrementalChecker(reflectionSettings, service, optionManager);
    IncrementalChecker checker =
        sabotConfig.getInstance(
            INCREMENTAL_CHECKER, IncrementalChecker.class, defaultChecker, defaultChecker);
    plan.accept(checker);
    boolean result = checker.isIncremental();
    fullRefreshReason.value = checker.getFullRefreshReason();
    return result;
  }

  public static boolean isIncrementalRefreshBySnapshotEnabled(
      DatasetConfig datasetConfig, OptionManager optionManager) {
    // Incremental refresh is not available for file-based datasets.
    if (datasetConfig.getType() == PHYSICAL_DATASET_SOURCE_FILE) {
      return false;
    }

    // REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED and
    // REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL
    // override the dataset refresh method settings for their appropriate datasets
    return (optionManager.getOption(REFLECTION_ICEBERG_SNAPSHOT_BASED_INCREMENTAL_ENABLED)
            && DatasetHelper.isIcebergDataset(datasetConfig))
        || (optionManager.getOption(REFLECTION_UNLIMITED_SPLITS_SNAPSHOT_BASED_INCREMENTAL)
            && DatasetHelper.isInternalIcebergTable(datasetConfig));
  }
}
