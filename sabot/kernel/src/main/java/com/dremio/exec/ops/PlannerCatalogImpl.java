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
package com.dremio.exec.ops;

import static com.dremio.exec.planner.physical.PlannerSettings.VDS_CACHE_ENABLED;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils.VersionedPath;
import com.dremio.exec.planner.logical.ConvertedViewTable;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.ViewExpander;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.tablefunctions.TimeTravelTableMacro;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.util.Pair;

/**
 * PlannerCatalogImpl validates and converts views as they are accessed so that we can guarantee
 * that the view's row type matches its converted type. See {@link
 * PlannerCatalog#getValidatedTableWithSchema(NamespaceKey)}
 */
public class PlannerCatalogImpl implements PlannerCatalog {

  private Map<Pair<CatalogIdentity, VersionedPath>, ConvertedViewTable> convertedViewTables;

  private OptionResolver optionResolver;
  private ViewExpander viewExpander;
  private Catalog metadataCatalog;
  private QueryContext context;

  PlannerCatalogImpl(
      QueryContext context, ViewExpander viewExpander, OptionResolver optionResolver) {
    this.viewExpander = viewExpander;
    this.optionResolver = optionResolver;
    this.context = context;
    this.metadataCatalog = context.getCatalog();
    this.convertedViewTables = new ConcurrentHashMap();
  }

  protected PlannerCatalogImpl(Catalog metadataCatalog, PlannerCatalogImpl plannerCatalogImpl) {
    this.metadataCatalog = metadataCatalog;
    this.optionResolver = plannerCatalogImpl.optionResolver;
    this.viewExpander = plannerCatalogImpl.viewExpander;
    this.context = plannerCatalogImpl.context;
    this.convertedViewTables = plannerCatalogImpl.convertedViewTables;
  }

  public Catalog getCatalog() {
    return metadataCatalog;
  }

  @Override
  public DremioTable getTableWithSchema(NamespaceKey key) {
    return metadataCatalog.getTable(key);
  }

  @Override
  public DremioTable getTableIgnoreSchema(NamespaceKey key) {
    return metadataCatalog.getTableNoResolve(key);
  }

  @Override
  public DremioTable getTableWithSchema(CatalogEntityKey catalogEntityKey) {
    return metadataCatalog.getTable(catalogEntityKey);
  }

  @Override
  public DremioTable getTableIgnoreSchema(CatalogEntityKey catalogEntityKey) {
    return metadataCatalog.getTableNoResolve(catalogEntityKey);
  }

  @Override
  public DremioTable getValidatedTableWithSchema(NamespaceKey key) {
    return convertView(metadataCatalog.getTableForQuery(key));
  }

  @Override
  public DremioTable getValidatedTableIgnoreSchema(NamespaceKey key) {
    return convertView(metadataCatalog.getTableNoResolve(key));
  }

  @Override
  public DremioTable getValidatedTableWithSchema(CatalogEntityKey catalogEntityKey) {
    return convertView(metadataCatalog.getTableSnapshotForQuery(catalogEntityKey));
  }

  @Override
  public Collection<Function> getFunctions(
      NamespaceKey path, SimpleCatalog.FunctionType functionType) {
    Collection<Function> f =
        metadataCatalog.getFunctions(path, functionType).stream()
            .map(
                function -> {
                  if (function instanceof TimeTravelTableMacro) {
                    return new TimeTravelTableMacro(
                        (tablePath, versionContext) ->
                            getValidatedTableWithSchema(
                                CatalogEntityKey.newBuilder()
                                    .keyComponents(tablePath)
                                    .tableVersionContext(versionContext)
                                    .build()));
                  }
                  return function;
                })
            .collect(Collectors.toList());
    return f;
  }

  /**
   * convertView is called during validation to immediately convert a {@link ViewTable} and
   * determine its converted row type. The converted view's rel node tree is cached and re-used
   * during conversion of the query. If the view has field names (i.e. aliases the fields in the
   * view's SQL), then update the validated row type with the field names.
   *
   * @param table - unconverted ViewTable
   * @return - {@link ConvertedViewTable}
   */
  protected DremioTable convertView(DremioTable table) {
    if (table == null || !(table instanceof ViewTable)) {
      return table;
    }

    ViewTable viewTable = (ViewTable) table;
    Pair<CatalogIdentity, VersionedPath> viewCacheKey =
        Pair.of(
            viewTable.getViewOwner(),
            VersionedPath.of(
                viewTable.getPath().getPathComponents(), viewTable.getVersionContext()));
    ConvertedViewTable convertedViewTable = convertedViewTables.get(viewCacheKey);
    if (convertedViewTable == null) {
      RelRoot root = viewExpander.expandView(viewTable);

      final RelDataType validatedRowTypeWithNames;
      if (viewTable.getView().hasDeclaredFieldNames()) {
        final List<String> fieldNames = viewTable.getView().getFieldNames();
        final List<RelDataTypeField> validatedFields = root.validatedRowType.getFieldList();
        Preconditions.checkState(
            fieldNames.size() == validatedFields.size(),
            String.format(
                "Cardinality mismatch between view field names %s and converted field list %s",
                fieldNames, root.validatedRowType.getFullTypeString()));
        List<RelDataTypeField> fields = new ArrayList<>();
        for (int i = 0; i < validatedFields.size(); i++) {
          final RelDataTypeField field = validatedFields.get(i);
          fields.add(
              new RelDataTypeFieldImpl(fieldNames.get(i), field.getIndex(), field.getType()));
        }
        validatedRowTypeWithNames = new RelRecordType(fields);
      } else {
        validatedRowTypeWithNames = root.validatedRowType;
      }
      convertedViewTable = ConvertedViewTable.of(viewTable, validatedRowTypeWithNames, root.rel);
      if (optionResolver.getOption(VDS_CACHE_ENABLED)) {
        convertedViewTables.put(viewCacheKey, convertedViewTable);
      }
    }
    return convertedViewTable;
  }

  @Override
  public void clearDatasetCache(NamespaceKey dataset, TableVersionContext versionContext) {
    metadataCatalog.clearDatasetCache(dataset, versionContext);
    convertedViewTables.values().removeIf(t -> t.getPath().equals(dataset));
  }

  @Override
  public void clearConvertedCache() {
    convertedViewTables.clear();
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    return metadataCatalog.getAllRequestedTables();
  }

  @Override
  public void validateSelection() {
    if (!optionResolver.getOption(CatalogOptions.DISABLE_CROSS_SOURCE_SELECT)) {
      return;
    }
    Set<String> selectedSources = new HashSet<>();
    getAllRequestedTables().forEach(t -> selectedSources.add(t.getPath().getRoot()));
    final List<String> disallowedSources =
        selectedSources.stream()
            .filter(s -> !s.equalsIgnoreCase("sys") && !s.equalsIgnoreCase("INFORMATION_SCHEMA"))
            .filter(
                s ->
                    Optional.ofNullable(context.getCatalogService().getManagedSource(s))
                        .map(x -> !x.getConfig().getAllowCrossSourceSelection())
                        .orElse(false))
            .collect(Collectors.toList());
    if (disallowedSources.size() > 1) {
      Collections.sort(disallowedSources);
      final String str =
          disallowedSources.stream()
              .collect(
                  Collectors.joining("', '", "Cross select is disabled between sources '", "'."));
      throw UserException.validationError().message(str).buildSilently();
    }
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return metadataCatalog.getDefaultSchema();
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return JavaTypeFactoryImpl.INSTANCE;
  }

  @Override
  public boolean containerExists(CatalogEntityKey path) {
    return metadataCatalog.containerExists(path);
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(NamespaceKey newDefaultSchema) {
    return new PlannerCatalogImpl(metadataCatalog.resolveCatalog(newDefaultSchema), this);
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(CatalogIdentity subject) {
    return new PlannerCatalogImpl(metadataCatalog.resolveCatalog(subject), this);
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return new PlannerCatalogImpl(metadataCatalog.resolveCatalog(sourceVersionMapping), this);
  }

  @Override
  public PlannerCatalog resolvePlannerCatalog(
      java.util.function.Function<Catalog, Catalog> catalogTransformer) {
    return new PlannerCatalogImpl(metadataCatalog.visit(catalogTransformer), this);
  }

  @Override
  public Catalog getMetadataCatalog() {
    return metadataCatalog;
  }

  @Override
  public void dispose() {
    optionResolver = null;
    viewExpander = null;
    context = null;
    metadataCatalog = null;
    convertedViewTables = null;
  }
}
