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

import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacroCall;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Throwables;

public class CreateViewHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateViewHandler.class);

  private final SqlHandlerConfig config;
  private final Catalog catalog;
  private BatchSchema viewSchema;

  public CreateViewHandler(SqlHandlerConfig config) {
    this.config = config;
    this.catalog = config.getContext().getCatalog();
    this.viewSchema = null;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
      SqlCreateView createView = SqlNodeUtil.unwrap(sqlNode, SqlCreateView.class);
      final NamespaceKey path = catalog.resolveSingle(createView.getPath());

      if (isVersioned(path)){
        return createVersionedView(createView);
      } else {
        return createView(createView);
      }
  }

  private List<SimpleCommandResult> createVersionedView(SqlCreateView createView) throws IOException, ValidationException {
    if(!config.getContext().getOptions().getOption(VERSIONED_VIEW_ENABLED)){
      throw UserException.unsupportedError().message("Currently do not support create versioned view").buildSilently();
    }

    final String newViewName = createView.getFullName();
    View view = getView(createView);

    boolean isUpdate = createView.getReplace();
    NamespaceKey viewPath = catalog.resolveSingle(createView.getPath());

    boolean exists = checkViewExistence(viewPath, newViewName, isUpdate);
    isUpdate &= exists;
    final ViewOptions viewOptions = getViewOptions(viewPath, isUpdate);
    if (isUpdate) {
      catalog.updateView(viewPath, view, viewOptions);
    } else {
      catalog.createView(viewPath, view, viewOptions);
    }
    return Collections.singletonList(SimpleCommandResult.successful("View '%s' %s successfully",
      viewPath, isUpdate ? "replaced" : "created"));
  }

  private List<SimpleCommandResult> createView(SqlCreateView createView) throws ValidationException, RelConversionException, ForemanSetupException, IOException, NamespaceException {
    final String newViewName = createView.getName();
    boolean isUpdate = createView.getReplace();
    NamespaceKey viewPath = catalog.resolveSingle(createView.getPath());
    boolean exists = checkViewExistence(viewPath, newViewName, isUpdate);
    isUpdate &= exists;
    final View view = getView(createView, exists);

    if (isUpdate) {
      catalog.updateView(viewPath, view, null);
    } else {
      createView(config.getContext(), viewPath, view, null, createView);
    }
    return Collections.singletonList(SimpleCommandResult.successful("View '%s' %s successfully",
      viewPath, isUpdate ? "replaced" : "created"));
  }

  protected void createView(QueryContext queryContext, NamespaceKey key, View view, ViewOptions viewOptions, SqlCreateView sqlCreateView) throws IOException {
    catalog.createView(key, view, viewOptions);
  }

  protected void updateView(QueryContext queryContext, NamespaceKey key, View view, ViewOptions viewOptions, SqlCreateView sqlCreateView) throws IOException, NamespaceException {
    catalog.updateView(key, view, viewOptions);
  }

  protected boolean isVersioned(NamespaceKey path){
    return CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog);
  }

  protected View getView(SqlCreateView createView) throws ValidationException {
    return getView(createView, false);
  }

  protected View getView(SqlCreateView createView, boolean allowRenaming) throws ValidationException {
    final String newViewName = createView.getName();
    final String viewSql = createView.getQuery().toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final RelNode newViewRelNode = getViewRelNode(createView, allowRenaming);

    NamespaceKey defaultSchema = catalog.getDefaultSchema();

    List<String> viewContext = defaultSchema == null ? null : defaultSchema.getPathComponents();
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (RelDataTypeField f : newViewRelNode.getRowType().getFieldList()) {
      CalciteArrowHelper.fieldFromCalciteRowType(f.getKey(), f.getValue()).ifPresent(schemaBuilder::addField);
    }
    viewSchema = schemaBuilder.build();
    return new View(newViewName, viewSql, newViewRelNode.getRowType(),
      allowRenaming ?
        createView.getFieldNames()
        : createView.getFieldNamesWithoutColumnMasking(),
      viewContext);
  }

  protected RelNode getViewRelNode(SqlCreateView createView, boolean allowRenaming) throws ValidationException {
    ConvertedRelNode convertedRelNode;
    try {
      convertedRelNode = PrelTransformer.validateAndConvert(config, createView.getQuery());
    } catch (Exception e) {
      throw UserException.resourceError().message("Cannot find table to create view from").buildSilently();
    }

    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();
    return SqlHandlerUtil.resolveNewTableRel(true, allowRenaming ? createView.getFieldNames() : createView.getFieldNamesWithoutColumnMasking(), validatedRowType, queryRelNode);
  }

  protected ViewOptions getViewOptions(NamespaceKey viewPath, boolean isUpdate){
    final String sourceName = viewPath.getRoot();

    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext version = CatalogUtil.resolveVersionContext(catalog, viewPath.getRoot(), sessionVersion);

    ViewOptions viewOptions = new ViewOptions.ViewOptionsBuilder()
      .version(version)
      .batchSchema(viewSchema)
      .viewUpdate(isUpdate)
      .build();

    return viewOptions;
  }

  private boolean isTimeTravelQuery(SqlNode sqlNode) {
    Pointer<Boolean> timeTravel = new Pointer<>(false);
    SqlVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
      @Override
      public Void visit(SqlCall call) {
        if (call instanceof SqlVersionedTableMacroCall) {
          timeTravel.value = true;
          return null;
        }

        return super.visit(call);
      }
    };

    sqlNode.accept(visitor);
    return timeTravel.value;
  }

  public static CreateViewHandler create(SqlHandlerConfig config) throws SqlParseException {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseCreateViewHandler");
      final Constructor<?> ctor = cl.getConstructor(SqlHandlerConfig.class);
      return (CreateViewHandler) ctor.newInstance(config);
    } catch (ClassNotFoundException e) {
      return new CreateViewHandler(config);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  public boolean  checkViewExistence(NamespaceKey viewPath, String newViewName, boolean isUpdate) {
    final DremioTable existingTable = catalog.getTableNoResolve(viewPath);
    if (existingTable != null) {
      if (existingTable.getJdbcTableType() != Schema.TableType.VIEW) {
        // existing table is not a view
        throw UserException.validationError()
          .message("A non-view table with given name [%s] already exists in schema [%s]",
            newViewName, viewPath.getParent()
          )
          .build(logger);
      }

      if (existingTable.getJdbcTableType() == Schema.TableType.VIEW && !isUpdate) {
        // existing table is a view and create view has no "REPLACE" clause
        throw UserException.validationError()
          .message("A view with given name [%s] already exists in schema [%s]",
            newViewName, viewPath.getParent()
          )
          .build(logger);
      }
    }
    return (existingTable != null) ;
  }

}
