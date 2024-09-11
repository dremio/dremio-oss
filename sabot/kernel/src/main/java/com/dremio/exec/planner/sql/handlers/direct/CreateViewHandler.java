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
import static com.dremio.exec.planner.sql.parser.ParserUtil.isTimeTravelQuery;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.ParserUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.util.QueryVersionUtils;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class CreateViewHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CreateViewHandler.class);

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
    ParserUtil.validateParsedViewQuery(createView.getQuery());
    final NamespaceKey resolvedViewPath = catalog.resolveSingle(createView.getPath());
    Preconditions.checkState(
        resolvedViewPath.size() > 1, "View path " + resolvedViewPath + " is not valid ");
    String formattedViewSql = getViewSql(createView, sql);
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(createView.getRefType(), createView.getRefValue(), null);
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(resolvedViewPath.getRoot());
    ConvertedRelNode convertedRelNode =
        validateTablesAndVersionContext(
            createView.getQuery(), resolvedViewPath, statementSourceVersion.orElse(sessionVersion));
    validateNonCyclicView(convertedRelNode, resolvedViewPath);
    catalog.validatePrivilege(resolvedViewPath, SqlGrant.Privilege.ALTER);
    if (isVersioned(resolvedViewPath)) {
      return createVersionedView(createView, formattedViewSql, convertedRelNode, resolvedViewPath);
    } else {
      return createView(createView, formattedViewSql, convertedRelNode, resolvedViewPath);
    }
  }

  private List<SimpleCommandResult> createVersionedView(
      SqlCreateView createView,
      String sql,
      ConvertedRelNode convertedRelNode,
      NamespaceKey viewPath)
      throws IOException, ValidationException {
    if (!config.getContext().getOptions().getOption(VERSIONED_VIEW_ENABLED)) {
      throw UserException.unsupportedError()
          .message("Currently do not support create versioned view")
          .buildSilently();
    }

    final String newViewName = createView.getFullName();
    View view = getView(createView, sql, convertedRelNode);

    boolean isUpdate = createView.getReplace();

    final String sourceName = viewPath.getRoot();
    VersionContext statementVersion =
        ReferenceTypeUtils.map(createView.getRefType(), createView.getRefValue(), null);
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementVersion.orElse(sessionVersion);
    final ResolvedVersionContext resolvedVersionContext =
        getResolvedVersionContext(sourceName, sourceVersion);
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(viewPath.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    boolean exists = checkViewExistence(newViewName, isUpdate, catalogEntityKey);
    isUpdate &= exists;
    final ViewOptions viewOptions = getViewOptions(isUpdate, resolvedVersionContext);
    CatalogUtil.validateResolvedVersionIsBranch(viewOptions.getVersion());
    if (isUpdate) {
      catalog.updateView(viewPath, view, viewOptions);
    } else {
      catalog.createView(viewPath, view, viewOptions);
    }
    return Collections.singletonList(
        SimpleCommandResult.successful(
            "View '%s' %s successfully", viewPath, isUpdate ? "replaced" : "created"));
  }

  private List<SimpleCommandResult> createView(
      SqlCreateView createView,
      String sql,
      ConvertedRelNode convertedRelNode,
      NamespaceKey viewPath)
      throws ValidationException,
          RelConversionException,
          ForemanSetupException,
          IOException,
          NamespaceException {
    final String newViewName = createView.getName();
    boolean isUpdate = createView.getReplace();
    boolean exists =
        checkViewExistence(newViewName, isUpdate, CatalogEntityKey.fromNamespaceKey(viewPath));
    isUpdate &= exists;
    final View view = getView(createView, sql, exists, convertedRelNode);

    if (isUpdate) {
      catalog.updateView(viewPath, view, null);
    } else {
      createView(config.getContext(), viewPath, view, null, createView);
    }
    return Collections.singletonList(
        SimpleCommandResult.successful(
            "View '%s' %s successfully", viewPath, isUpdate ? "replaced" : "created"));
  }

  protected void createView(
      QueryContext queryContext,
      NamespaceKey key,
      View view,
      ViewOptions viewOptions,
      SqlCreateView sqlCreateView)
      throws IOException {
    catalog.createView(key, view, viewOptions);
  }

  protected void updateView(
      QueryContext queryContext,
      NamespaceKey key,
      View view,
      ViewOptions viewOptions,
      SqlCreateView sqlCreateView)
      throws IOException, NamespaceException {
    catalog.updateView(key, view, viewOptions);
  }

  protected boolean isVersioned(NamespaceKey path) {
    return CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog);
  }

  protected ResolvedVersionContext getResolvedVersionContext(
      String sourceName, VersionContext version) {
    return CatalogUtil.resolveVersionContext(catalog, sourceName, version);
  }

  protected View getView(SqlCreateView createView, String sql, ConvertedRelNode relNode)
      throws ValidationException {
    return getView(createView, sql, false, relNode);
  }

  protected View getView(
      SqlCreateView createView,
      String viewSql,
      boolean allowRenaming,
      ConvertedRelNode convertedRelNode)
      throws ValidationException {
    final String newViewName = createView.getName();
    final RelNode newViewRelNode = getViewRelNode(convertedRelNode, createView, allowRenaming);

    NamespaceKey defaultSchema = catalog.getDefaultSchema();

    List<String> viewContext = defaultSchema == null ? null : defaultSchema.getPathComponents();
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (RelDataTypeField f : newViewRelNode.getRowType().getFieldList()) {
      CalciteArrowHelper.fieldFromCalciteRowType(f.getKey(), f.getValue())
          .ifPresent(schemaBuilder::addField);
    }
    viewSchema = schemaBuilder.build();
    return new View(
        newViewName,
        viewSql,
        newViewRelNode.getRowType(),
        allowRenaming ? createView.getFieldNames() : createView.getFieldNamesWithoutColumnMasking(),
        viewContext);
  }

  private List<SqlIdentifier> getIdentifiers(SqlNode query, Comparator<SqlParserPos> comparator) {
    List<SqlIdentifier> identifiers = new ArrayList<>();
    SqlVisitor<Void> createViewVisitor =
        new SqlBasicVisitor<Void>() {
          @Override
          public Void visit(SqlIdentifier id) {
            identifiers.add(id);
            return null;
          }

          @Override
          public Void visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
              SqlNode node = nodeList.get(i);
              if (node != null) {
                node.accept(this);
              }
            }
            return null;
          }
        };

    query.accept(createViewVisitor);

    // Sort identifiers by their positions
    Collections.sort(
        identifiers,
        (SqlIdentifier left, SqlIdentifier right) -> {
          SqlParserPos lp = left.getParserPosition();
          SqlParserPos rp = right.getParserPosition();
          return comparator.compare(lp, rp);
        });

    // Remove duplicated identifiers
    List<SqlIdentifier> uniqueIdentifiers = new ArrayList<>();
    for (int i = 0; i < identifiers.size(); i++) {
      if (!uniqueIdentifiers.isEmpty()
          && comparator.compare(
                  uniqueIdentifiers.get(uniqueIdentifiers.size() - 1).getParserPosition(),
                  identifiers.get(i).getParserPosition())
              == 0) {
        continue;
      }
      uniqueIdentifiers.add(identifiers.get(i));
    }

    return uniqueIdentifiers;
  }

  /*
   * Convert ParserPos row and col number to the index in the sql string. Note ParserPos is 1-base indexed.
   * For instance, given sql = "CREATE VIEW test AS\nSELECT *\nFROM table" and row = 3, col = 6, it should return 34.
   * Because row 3 is "FROM table" and col 6 is letter `t`, whose index in the original sql string is 34 (0-base indexed).
   * The returned value should be in range [0, sql.length()).
   */
  private int convertParserPosToStringPos(String sql, int row, int col) {
    // fromIndex is the index of the 1st letter after a new line
    int fromIndex = 0;
    while (row > 1 && fromIndex < sql.length()) {
      fromIndex = sql.indexOf('\n', fromIndex) + 1;
      row--;
    }
    // At this point, fromIndex is the index of the 1st letter in row #row.
    // The index of #col letter in this row would be added by col-1.
    return fromIndex + col - 1;
  }

  private int appendIdentifierAndSql(
      SqlIdentifier identifier, String sql, StringBuilder viewSql, int beginIndex)
      throws IndexOutOfBoundsException {
    SqlDialect dialect = DremioSqlDialect.DEFAULT;
    int nonStarComponentSize = identifier.names.size();
    if (identifier.isStar()) {
      // The last component is *
      nonStarComponentSize--;
    }
    // Process non-star identifier components
    for (int j = 0; j < nonStarComponentSize; j++) {
      SqlParserPos pos = identifier.getComponentParserPosition(j);
      int endIndex = convertParserPosToStringPos(sql, pos.getLineNum(), pos.getColumnNum());
      viewSql.append(sql, beginIndex, endIndex);
      String componentName = identifier.names.get(j);
      if (pos.isQuoted()) {
        viewSql.append(dialect.quoteIdentifier(componentName));
      } else {
        viewSql.append(componentName);
      }
      beginIndex = 1 + convertParserPosToStringPos(sql, pos.getEndLineNum(), pos.getEndColumnNum());
    }
    // Process the star component, i.e. the last component
    if (identifier.isStar()) {
      SqlParserPos pos = identifier.getComponentParserPosition(nonStarComponentSize);
      int endIndex = convertParserPosToStringPos(sql, pos.getLineNum(), pos.getColumnNum());
      viewSql.append(sql, beginIndex, endIndex);
      viewSql.append('*');
      beginIndex = 1 + endIndex;
    }

    return beginIndex;
  }

  private String getViewSql(String sql, SqlParserPos asTokenPos, List<SqlIdentifier> identifiers)
      throws IndexOutOfBoundsException {
    // Start from the index one position past the `AS` token
    int beginIndex =
        1
            + convertParserPosToStringPos(
                sql, asTokenPos.getEndLineNum(), asTokenPos.getEndColumnNum());
    // Skip any whitespaces after the `AS` token
    while (Character.isWhitespace(sql.charAt(beginIndex)) && beginIndex < sql.length()) {
      beginIndex++;
    }

    StringBuilder viewSql = new StringBuilder();
    for (int i = 0; i < identifiers.size(); i++) {
      beginIndex = appendIdentifierAndSql(identifiers.get(i), sql, viewSql, beginIndex);
    }
    viewSql.append(sql.substring(beginIndex));

    return viewSql.toString();
  }

  protected String getViewSql(SqlCreateView createView, String sql) throws UserException {
    // Get the list of identifiers in the view definition query in ascending order of their
    // positions
    Comparator<SqlParserPos> comparator =
        (lp, rp) -> {
          if (lp.getLineNum() != rp.getLineNum()) {
            return lp.getLineNum() - rp.getLineNum();
          } else if (lp.getColumnNum() != rp.getColumnNum()) {
            return lp.getColumnNum() - rp.getColumnNum();
          } else if (lp.getEndLineNum() != rp.getEndLineNum()) {
            return lp.getEndLineNum() - rp.getEndLineNum();
          } else {
            return lp.getEndColumnNum() - rp.getEndColumnNum();
          }
        };
    List<SqlIdentifier> identifiers = getIdentifiers(createView.getQuery(), comparator);

    // The `AS` token must be BEFORE any identifier in the view definition query. The exception
    // should never be thrown.
    SqlParserPos asTokenPos = createView.getParserPosition();
    if (identifiers.size() > 0
        && comparator.compare(asTokenPos, identifiers.get(0).getParserPosition()) >= 0) {
      throw UserException.validationError()
          .message("Invalid create view statement. AS token after view definition")
          .buildSilently();
    }

    return getViewSql(sql, asTokenPos, identifiers);
  }

  protected RelNode getViewRelNode(
      ConvertedRelNode convertedRelNode, SqlCreateView createView, boolean allowRenaming)
      throws UserException, ValidationException {
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode queryRelNode = convertedRelNode.getConvertedNode();
    return SqlHandlerUtil.resolveNewTableRel(
        true,
        allowRenaming ? createView.getFieldNames() : createView.getFieldNamesWithoutColumnMasking(),
        validatedRowType,
        queryRelNode);
  }

  protected ViewOptions getViewOptions(boolean isUpdate, ResolvedVersionContext version) {
    ViewOptions viewOptions =
        new ViewOptions.ViewOptionsBuilder()
            .version(version)
            .batchSchema(viewSchema)
            .actionType(
                isUpdate ? ViewOptions.ActionType.UPDATE_VIEW : ViewOptions.ActionType.CREATE_VIEW)
            .icebergViewVersion(config.getContext().getOptions())
            .build();

    return viewOptions;
  }

  public static CreateViewHandler create(SqlHandlerConfig config) throws SqlParseException {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseCreateViewHandler");
      final Constructor<?> ctor = cl.getConstructor(SqlHandlerConfig.class);
      return (CreateViewHandler) ctor.newInstance(config);
    } catch (ClassNotFoundException e) {
      return new CreateViewHandler(config);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  public boolean checkViewExistence(
      String newViewName, boolean isUpdate, CatalogEntityKey catalogEntityKey) {
    DremioTable existingTable = catalog.getTableNoResolve(catalogEntityKey);
    NamespaceKey viewPath = catalogEntityKey.toNamespaceKey();
    if (existingTable != null) {
      if (existingTable.getJdbcTableType() != Schema.TableType.VIEW) {
        // existing table is not a view
        throw UserException.validationError()
            .message(
                "A non-view table with given name [%s] already exists in schema [%s]",
                newViewName, viewPath.getParent())
            .build(logger);
      }

      if (existingTable.getJdbcTableType() == Schema.TableType.VIEW && !isUpdate) {
        // existing table is a view and create view has no "REPLACE" clause
        throw UserException.validationError()
            .message(
                "A view with given name [%s] already exists in schema [%s]",
                newViewName, viewPath.getParent())
            .build(logger);
      }
    }
    return (existingTable != null);
  }

  protected ConvertedRelNode validateTablesAndVersionContext(
      SqlNode sqlNode, NamespaceKey targetPath, VersionContext targetVersion) {
    Map<String, VersionContext> targetSourceVersionMapping = CaseInsensitiveMap.newHashMap();
    if (targetPath != null) {
      String targetSource = targetPath.getRoot();
      targetSourceVersionMapping.put(
          targetSource,
          ((targetVersion != null) && targetVersion.isSpecified()
              ? targetVersion
              : VersionContext.NOT_SPECIFIED));
      if (isVersioned(targetPath) && isTimeTravelQuery(sqlNode)) {
        throw UserException.unsupportedError()
            .message("Versioned views do not support AT SNAPSHOT or AT TIMESTAMP")
            .buildSilently();
      }
      try {
        List<String> pathContext =
            catalog.getDefaultSchema() == null
                ? null
                : catalog.getDefaultSchema().getPathComponents();
        return QueryVersionUtils.checkForUnspecifiedVersionsAndReturnRelNode(
            sqlNode,
            pathContext,
            config.getContext().getSabotQueryContext(),
            targetSourceVersionMapping,
            Optional.ofNullable(config.getContext().getSession()));
      } catch (Exception e) {
        throw UserException.validationError(e)
            .message("Validation of view sql failed. %s ", e.getMessage())
            .buildSilently();
      }
    }
    return null;
  }

  /** To verify that the view does not contain any reference to itself. */
  private void validateNonCyclicView(ConvertedRelNode convertedRelNode, NamespaceKey viewPath) {
    if (convertedRelNode != null && convertedRelNode.getViewIdentifiers() != null) {
      for (NamespaceKey identifier : convertedRelNode.getViewIdentifiers()) {
        if (viewPath.equals(identifier)) {
          throw UserException.validationError()
              .message(
                  "Invalid create view statement. "
                      + "A view with given name [%s] cannot reference itself.",
                  viewPath)
              .build(logger);
        }
      }
    }
  }

  protected Catalog getCatalog() {
    return catalog;
  }
}
