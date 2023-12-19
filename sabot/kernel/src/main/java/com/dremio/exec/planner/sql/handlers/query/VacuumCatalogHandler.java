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

import static com.dremio.exec.ExecConstants.ICEBERG_VACUUM_CATALOG_RETENTION_PERIOD_MINUTES;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.projectnessie.client.api.NessieApiV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.calcite.logical.VacuumCatalogCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.NessieGCPolicy;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlVacuumCatalog;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NessieApiProvider;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;

/**
 * Handler for {@link com.dremio.exec.planner.sql.parser.SqlVacuumCatalog} command.
 */
public class VacuumCatalogHandler implements SqlToPlanHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(VacuumCatalogHandler.class);
  private String textPlan;
  private Rel drel;

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    final Catalog catalog = config.getContext().getCatalog();
    SqlVacuumCatalog sqlVacuumCatalog = (SqlVacuumCatalog) sqlNode;
    NamespaceKey path = SqlNodeUtil.unwrap(sqlVacuumCatalog, SqlVacuumCatalog.class).getPath();

    validate(catalog, config, path);

    try {
      final Prel prel = getNonPhysicalPlan(config.getContext().getCatalogService(), config, sqlVacuumCatalog, path);
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);

      return PrelTransformer.convertToPlan(config, pop);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(LOGGER, sql, e, true);
    }
  }

  private void validate(Catalog catalog, SqlHandlerConfig config, NamespaceKey path) {
    catalog.validateOwnership(path);
    validateFeatureEnabled(config);
    validateCompatibleCatalog(catalog, path);
  }

  private void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG)) {
      throw UserException.unsupportedError().message("VACUUM CATALOG command is not supported.").buildSilently();
    }
  }

  private void validateCompatibleCatalog(Catalog catalog, NamespaceKey path) {
    if (!CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)) {
      throw UserException.unsupportedError()
        .message("VACUUM CATALOG is supported only on versioned sources.")
        .buildSilently();
    }
  }

  private Prel getNonPhysicalPlan(CatalogService catalogService, SqlHandlerConfig config, SqlVacuumCatalog sqlNode, NamespaceKey path) throws Exception {
    final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvert(config, sqlNode);
    final RelNode relNode = convertedRelNode.getConvertedNode();
    ManagedStoragePlugin managedStoragePlugin = catalogService.getManagedSource(path.getRoot());
    StoragePlugin sourcePlugin = catalogService.getSource(path.getRoot());
    VacuumCatalogCompatibilityChecker.getInstance(config.getScanResult())
      .checkCompatibility(sourcePlugin, config.getContext().getOptions());

    NessieApiV2 nessieApi = getNessieApi(sourcePlugin);
    IcebergCostEstimates costEstimates = VacuumCatalogCostEstimates.find(nessieApi, sqlNode.getVacuumOptions());

    drel = convertToDrel(config, relNode, managedStoragePlugin, nessieApi, costEstimates, config.getContext().getQueryUserName());
    final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);
    textPlan = prelAndTextPlan.getValue();
    return prelAndTextPlan.getKey();
  }

  private Rel convertToDrel(SqlHandlerConfig config, RelNode relNode, ManagedStoragePlugin managedStoragePlugin,
                            NessieApiV2 nessieApi, IcebergCostEstimates costEstimates, String user) throws Exception {
    NessieGCPolicy nessieGCPolicy = new NessieGCPolicy(nessieApi,
      config.getContext().getOptions().getOption(ICEBERG_VACUUM_CATALOG_RETENTION_PERIOD_MINUTES));
    VacuumOptions vacuumOptions = new VacuumOptions(nessieGCPolicy);
    Rel convertedRelNode = DrelTransformer.convertToDrel(config,
        rewriteCrel(relNode, managedStoragePlugin.getId(), vacuumOptions, costEstimates, user, nessieGCPolicy.isDefaultRetention()));
    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  private static NessieApiV2 getNessieApi(StoragePlugin nessiePlugin) {
    Preconditions.checkState(nessiePlugin instanceof NessieApiProvider, "VACUUM CATALOG is supported only on versioned sources.");
    return ((NessieApiProvider) nessiePlugin).getNessieApi();
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }

  protected static RelNode rewriteCrel(RelNode relNode, StoragePluginId storagePluginId,
      VacuumOptions vacuumOptions, IcebergCostEstimates costEstimates, String user, boolean isDefaultRetention) {
    return TableManagementHandler.ScanCrelSubstitutionRewriter.disableScanCrelSubstitution(
        CrelStoragePluginApplier.apply(relNode, storagePluginId, vacuumOptions, costEstimates, user, isDefaultRetention));
  }

  private static class CrelStoragePluginApplier extends StatelessRelShuttleImpl {
    private final StoragePluginId storagePluginId;
    private final String user;
    private final VacuumOptions vacuumOptions;
    private final IcebergCostEstimates costEstimates;
    private final boolean isDefaultRetention;

    private CrelStoragePluginApplier(StoragePluginId storagePluginId, String user, IcebergCostEstimates costEstimates,
        VacuumOptions vacuumOptions, boolean isDefaultRetention) {
      this.storagePluginId = storagePluginId;
      this.user = user;
      this.vacuumOptions = vacuumOptions;
      this.costEstimates = costEstimates;
      this.isDefaultRetention = isDefaultRetention;
    }

    public static RelNode apply(RelNode relNode,
        StoragePluginId storagePluginId, VacuumOptions vacuumOptions, IcebergCostEstimates costEstimates, String user, boolean isDefaultRetention) {
      CrelStoragePluginApplier applier = new CrelStoragePluginApplier(storagePluginId, user, costEstimates,
          vacuumOptions, isDefaultRetention);
      return applier.visit(relNode);
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof VacuumCatalogCrel) {
        other = ((VacuumCatalogCrel) other).createWith(storagePluginId, vacuumOptions, costEstimates, user, isDefaultRetention);
      }

      return super.visit(other);
    }
  }
}
