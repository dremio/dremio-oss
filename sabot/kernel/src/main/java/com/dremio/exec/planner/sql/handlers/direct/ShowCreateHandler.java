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

import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlShowCreate;
import com.dremio.exec.planner.sql.parser.TableDefinitionGenerator;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;

public class ShowCreateHandler implements SqlDirectHandler<ShowCreateHandler.DefinitionResult> {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ShowCreateHandler.class);

  private final Catalog catalog;
  protected final QueryContext queryContext;

  public ShowCreateHandler(Catalog catalog, QueryContext queryContext) {
    super();
    this.catalog = Preconditions.checkNotNull(catalog);
    this.queryContext = Preconditions.checkNotNull(queryContext);
  }

  @Override
  public List<ShowCreateHandler.DefinitionResult> toResult(String sql, SqlNode sqlNode)
      throws RelConversionException, ForemanSetupException {
    OptionManager optionManager = Preconditions.checkNotNull(queryContext.getOptions());
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlNode, optionManager);

    final SqlShowCreate sqlShowCreate = SqlNodeUtil.unwrap(sqlNode, SqlShowCreate.class);
    NamespaceKey resolvedPath = catalog.resolveSingle(sqlShowCreate.getPath());
    final String sourceName = resolvedPath.getRoot();
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(sqlShowCreate.getRefType(), sqlShowCreate.getRefValue(), null);
    final VersionContext sessionVersion =
        queryContext.getSession().getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    final ResolvedVersionContext resolvedVersionContext =
        getResolvedVersionContext(sourceName, sourceVersion);
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(resolvedPath.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    validateCommand(catalogEntityKey, sqlShowCreate.getIsView());

    final DremioTable table = getTable(catalogEntityKey);
    validateTable(table, resolvedPath, resolvedVersionContext, sqlShowCreate.getIsView());

    if (sqlShowCreate.getIsView()) {
      return getViewDefinition(table.getDatasetConfig(), resolvedPath, resolvedVersionContext);
    } else {
      return getTableDefinition(
          table.getDatasetConfig(),
          table.getRowType(JavaTypeFactoryImpl.INSTANCE),
          resolvedPath,
          resolvedVersionContext,
          optionManager);
    }
  }

  protected ResolvedVersionContext getResolvedVersionContext(
      String sourceName, VersionContext version) {
    return CatalogUtil.resolveVersionContext(catalog, sourceName, version);
  }

  protected void validateCommand(CatalogEntityKey key, boolean isView) {
    // For the default implementation, don't do anything.
  }

  private void validateTable(
      DremioTable table,
      NamespaceKey resolvedPath,
      ResolvedVersionContext resolvedVersionContext,
      boolean isView) {
    if (table == null) {
      throw UserException.validationError()
          .message(
              "Unknown %s [%s]%s",
              isView ? "view" : "table",
              resolvedPath,
              resolvedVersionContext == null
                  ? ""
                  : String.format(" for %s", resolvedVersionContext))
          .build(LOGGER);
    }

    DatasetConfig datasetConfig = table.getDatasetConfig();
    if (isView) {
      if (datasetConfig == null || !isDatasetTypeAView(datasetConfig.getType())) {
        throw UserException.validationError()
            .message("[%s] is not a view", resolvedPath)
            .build(LOGGER);
      }
    } else {
      if (datasetConfig == null || !CatalogUtil.isDatasetTypeATable(datasetConfig.getType())) {
        throw UserException.validationError()
            .message("[%s] is not a table", resolvedPath)
            .build(LOGGER);
      }
    }
  }

  private boolean isDatasetTypeAView(DatasetType type) {
    return type == VIRTUAL_DATASET;
  }

  protected DremioTable getTable(CatalogEntityKey catalogEntityKey) {
    return catalog.getTable(catalogEntityKey);
  }

  private List<ShowCreateHandler.DefinitionResult> getViewDefinition(
      DatasetConfig datasetConfig,
      NamespaceKey resolvedPath,
      ResolvedVersionContext resolvedVersionContext) {
    final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();
    if (virtualDataset == null) {
      throw UserException.validationError()
          .message("View at [%s] is corrupted", resolvedPath)
          .build(LOGGER);
    }

    return Collections.singletonList(
        new DefinitionResult(
            String.format(
                "[%s]%s",
                resolvedPath,
                resolvedVersionContext == null
                    ? ""
                    : String.format(
                        " at %s",
                        ResolvedVersionContext.convertToVersionContext(resolvedVersionContext))),
            virtualDataset.getSql()));
  }

  private List<ShowCreateHandler.DefinitionResult> getTableDefinition(
      DatasetConfig datasetConfig,
      RelDataType type,
      NamespaceKey resolvedPath,
      ResolvedVersionContext resolvedVersionContext,
      OptionManager optionManager) {
    boolean isVersioned = isVersioned(resolvedPath);
    Preconditions.checkArgument(
        !isVersioned || resolvedVersionContext != null,
        String.format("Can't resolve the version of table [%s].", resolvedPath));
    Preconditions.checkArgument(
        type != null, String.format("The row type of table [%s] is corrupted.", resolvedPath));

    String tableDefinition =
        getTableDefinition(
            datasetConfig,
            resolvedPath,
            type.getFieldList(),
            resolvedVersionContext == null ? null : resolvedVersionContext.getType().toString(),
            resolvedVersionContext == null ? null : resolvedVersionContext.getRefName(),
            isVersioned);

    if (tableDefinition != null) {
      return Collections.singletonList(
          new DefinitionResult(
              String.format(
                  "[%s]%s",
                  resolvedPath,
                  resolvedVersionContext == null
                      ? ""
                      : String.format(
                          " at %s",
                          ResolvedVersionContext.convertToVersionContext(resolvedVersionContext))),
              tableDefinition));
    } else {
      return Collections.singletonList(
          new DefinitionResult(
              String.format("[%s]", resolvedPath),
              String.format("SELECT * FROM %s", resolvedPath)));
    }
  }

  protected boolean isVersioned(NamespaceKey resolvedPath) {
    return CatalogUtil.requestedPluginSupportsVersionedTables(resolvedPath, catalog);
  }

  protected String getTableDefinition(
      DatasetConfig datasetConfig,
      NamespaceKey resolvedPath,
      List<RelDataTypeField> fields,
      String refType,
      String refValue,
      boolean isVersioned) {
    return new TableDefinitionGenerator(
            datasetConfig,
            resolvedPath,
            fields,
            refType,
            refValue,
            isVersioned,
            queryContext.getOptions())
        .generateTableDefinition();
  }

  public static class DefinitionResult {
    public final String path;
    public final String sql_definition;

    public DefinitionResult(String path, String sql_definition) {
      super();
      this.path = path;
      this.sql_definition = sql_definition;
    }
  }

  @Override
  public Class<ShowCreateHandler.DefinitionResult> getResultType() {
    return ShowCreateHandler.DefinitionResult.class;
  }
}
