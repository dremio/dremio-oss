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
package com.dremio.exec.planner.sql;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql2rel.DremioRelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Scope for validation and SQL to Rel.
 */
public class SqlValidatorAndToRelContext {
  private static final org.slf4j.Logger logger = getLogger(SqlValidatorAndToRelContext.class);

  private final SqlConverter sqlConverter;
  private final DremioCatalogReader dremioCatalogReader;
  private final SqlValidatorImpl validator;
  private final boolean isInnerQuery;

  public SqlValidatorAndToRelContext(SqlConverter sqlConverter,
    DremioCatalogReader dremioCatalogReader,
    @Nullable SqlOperatorTable contextualSqlOperatorTable,
    boolean isInnerQuery) {
    this.sqlConverter = sqlConverter;
    this.dremioCatalogReader = dremioCatalogReader;
    this.validator = createValidator(sqlConverter, dremioCatalogReader, contextualSqlOperatorTable);
    this.isInnerQuery = isInnerQuery;
  }

  public SqlNode validate(final SqlNode parsedNode) {
    resolveVersionedTableExpressions(parsedNode);
    SqlNode node = validator.validate(parsedNode);
    dremioCatalogReader.validateSelection();
    return node;
  }

  public SqlNode validateParameterized(final SqlNode parsedNode, Map<String, RelDataType> nameToTypeMap) {
    resolveVersionedTableExpressions(parsedNode);
    SqlNode node = validator.validateParameterizedExpression(parsedNode, nameToTypeMap);
    dremioCatalogReader.validateSelection();
    return node;
  }

  public RexNode validateAndConvertFunction(final SqlNode parsedNode, Map<String, RexNode> nameToNodeMap) {
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().build();
    SqlSelect selectNode = toSelect(parsedNode);
    final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
    for (Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet()) {
      nameToTypeMap.put(entry.getKey(), entry.getValue().getType());
    }
    SqlNode validatedNode = validateParameterized(selectNode, nameToTypeMap);
    SqlNode expressionNode = extractOnlyExpression(validatedNode);
    final SqlToRelConverter sqlToRelConverter =
      new DremioSqlToRelConverter(sqlConverter, dremioCatalogReader, validator,
        sqlConverter.getConvertletTable(), config);

    RexNode rexNode =  sqlToRelConverter.convertExpression(expressionNode, nameToNodeMap);

    return replaceArgs(rexNode, nameToNodeMap);
  }


  /**
   * Returns a rel root that defers materialization of scans via {@link com.dremio.exec.planner.logical.ConvertibleScan}
   *
   * Used for serialization.
   */
  public SqlConverter.RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean expand, boolean flatten) {
    return toConvertibleRelRoot(validatedNode, expand, flatten, true);
  }

  public SqlConverter.RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean expand, boolean flatten, boolean withConvertTableAccess) {

    final OptionResolver o = sqlConverter.getSettings().options;
    ReflectionAllowedMonitoringConvertletTable.ConvertletTableNotes convertletTableNotes =
      sqlConverter.getConvertletTableNotes();
    SqlRexConvertletTable convertletTable = sqlConverter.getConvertletTable();
    final long inSubQueryThreshold =  o.getOption(ExecConstants.FAST_OR_ENABLE) ? o.getOption(ExecConstants.FAST_OR_MAX_THRESHOLD) : o.getOption(ExecConstants.PLANNER_IN_SUBQUERY_THRESHOLD);
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
      .withInSubQueryThreshold((int) inSubQueryThreshold)
      .withTrimUnusedFields(true)
      .withConvertTableAccess(withConvertTableAccess && o.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT))
      .withExpand(expand)
      .build();
    final SqlToRelConverter sqlToRelConverter = new DremioSqlToRelConverter(sqlConverter, dremioCatalogReader, validator, convertletTable, config);
    final boolean isComplexTypeSupport = o.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT);
    // Previously we had "top" = !innerQuery, but calcite only adds project if it is not a top query.
    final RelRoot rel = sqlToRelConverter.convertQuery(validatedNode, false /* needs validate */, false /* top */);
    if (!flatten) {
      return SqlConverter.RelRootPlus.of(rel.rel, rel.validatedRowType, rel.kind, convertletTableNotes.isReflectionDisallowed(), convertletTableNotes.isPlanCacheable());
    }
    final RelNode rel2;
    if (isComplexTypeSupport) {
      rel2 = MoreRelOptUtil.StructuredConditionRewriter.rewrite(rel.rel);
    } else {
      rel2 = sqlToRelConverter.flattenTypes(rel.rel, true);
    }

    final RelNode rel3;
    if (expand && !isComplexTypeSupport) {
      rel3 = rel2.accept(new RexSubQueryUtils.RelsWithRexSubQueryFlattener(sqlToRelConverter));
    } else {
      rel3 = rel2;
    }

    final RelNode converted;
    if (sqlConverter.getSettings().isRelPlanningEnabled()) {
      converted = rel3;
    } else {
      converted = DremioRelDecorrelator.decorrelateQuery(rel3, DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(rel3.getCluster(), null), false, false);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ConvertQuery with expand = {}:\n{}", expand, RelOptUtil.toString(converted, SqlExplainLevel.ALL_ATTRIBUTES));
    }
    return SqlConverter.RelRootPlus.of(converted, rel.validatedRowType, rel.kind, convertletTableNotes.isReflectionDisallowed(), convertletTableNotes.isPlanCacheable());
  }

  /**
   * This performs a special pass over the SqlNode AST to resolve any versioned table references containing constant
   * expressions to an equivalent form with those expressions resolved to a SqlLiteral.  This is necessary so that
   * catalog lookups performed during validation can be provided with the resolved version context.
   */
  private void resolveVersionedTableExpressions(final SqlNode parsedNode) {
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().build();
    final SqlToRelConverter sqlToRelConverter = new DremioSqlToRelConverter(sqlConverter, dremioCatalogReader, validator, sqlConverter.getConvertletTable(), config);
    VersionedTableExpressionResolver resolver = new VersionedTableExpressionResolver(validator, sqlConverter.getCluster().getRexBuilder());
    resolver.resolve(sqlToRelConverter, parsedNode);
  }

  public RelDataType getValidatedRowType(String sql) {
    SqlNode sqlNode = sqlConverter.parse(sql);
    SqlNode validatedNode = validate(sqlNode);
    return validator.getValidatedNodeType(validatedNode);
  }

  public RelDataType getOutputType(SqlNode validatedNode) {
    return validator.getValidatedNodeType(validatedNode);
  }

  public DremioCatalogReader getDremioCatalogReader() {
    return dremioCatalogReader;
  }

  public static Builder builder(SqlConverter sqlConverter) {
    return new Builder(sqlConverter,
      sqlConverter.getCatalog(),
      null,
      false);
  }

  public static Builder builder(SqlValidatorAndToRelContext sqlValidatorAndToRelContext) {
    return new Builder(sqlValidatorAndToRelContext.sqlConverter,
      sqlValidatorAndToRelContext.sqlConverter.getCatalog(),
      null,
      sqlValidatorAndToRelContext.isInnerQuery);
  }

  public SqlConverter getSqlConverter() {
    return sqlConverter;
  }

  public static class Builder {
    final SqlConverter sqlConverter;
    final Catalog catalog;
    final boolean isSubQuery;
    @Nullable final SqlOperatorTable contextualSqlOperatorTable;

    public Builder(SqlConverter sqlConverter,
        Catalog catalog,
        SqlOperatorTable contextualSqlOperatorTable,
        boolean isSubQuery) {
      this.sqlConverter = sqlConverter;
      this.catalog = catalog;
      this.contextualSqlOperatorTable = contextualSqlOperatorTable;
      this.isSubQuery = isSubQuery;
    }

    public Builder withSchemaPath(List<String> schemaPath) {
      NamespaceKey withSchemaPath = schemaPath == null ? null : new NamespaceKey(schemaPath);

      return new Builder(
        sqlConverter,
        catalog.resolveCatalog(withSchemaPath),
        contextualSqlOperatorTable,
        isSubQuery);
    }

    public Builder withCatalog(final Function<Catalog, Catalog> catalogTransformer) {
      return new Builder(
        sqlConverter,
        catalogTransformer.apply(catalog),
        contextualSqlOperatorTable,
        isSubQuery);
    }

    public Builder withUser(CatalogIdentity user) {
      return new Builder(
        sqlConverter,
        catalog.resolveCatalog(user),
        contextualSqlOperatorTable,
        isSubQuery);
    }

    public Builder withVersionContext(String source, VersionContext versionContext) {
      sqlConverter.setViewExpansionVersionContext(versionContext);
      final Map<String, VersionContext> sourceVersionMapping = new HashMap<>();
      sourceVersionMapping.put(source, versionContext);
      return new Builder(
        sqlConverter,
        catalog.resolveCatalog(sourceVersionMapping),
        contextualSqlOperatorTable,
        isSubQuery);
    }

    public Builder withContextualSqlOperatorTable(SqlOperatorTable contextualSqlOperatorTable) {
      return new Builder(sqlConverter, catalog, contextualSqlOperatorTable, isSubQuery);
    }

    public Builder withSystemDefaultParserConfig() {
      return new Builder(sqlConverter.withSystemDefaultParserConfig(), catalog, contextualSqlOperatorTable, isSubQuery);
    }

    public SqlValidatorAndToRelContext build() {
      return new SqlValidatorAndToRelContext(sqlConverter,
        new DremioCatalogReader(catalog, sqlConverter.getTypeFactory()),
        contextualSqlOperatorTable,
        isSubQuery);
    }

  }

  private static RexNode replaceArgs(RexNode rexNode, Map<String, RexNode> namesToNodeMap) {
    return rexNode.accept(new RexShuttle() {
      @Override public RexNode visitCall(RexCall call) {
        if(call.getOperands().isEmpty()) {
          return namesToNodeMap.getOrDefault(call.getOperator().getName(), call);
        }
        return super.visitCall(call);
      }
    });
  }

  private static SqlNode extractOnlyExpression(SqlNode sqlNode) {
    if (sqlNode instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlNode;
      Preconditions.checkState(null == sqlSelect.getFrom());
      Preconditions.checkState(sqlSelect.getSelectList().size() == 1);
      return sqlSelect.getSelectList().get(0);
    } else {
      throw new RuntimeException();
    }
  }

  private static SqlValidatorImpl createValidator(SqlConverter sqlConverter,
      DremioCatalogReader catalogReader,
      @Nullable SqlOperatorTable contextualSqlOperatorTable) {
    SqlValidatorImpl validator =  new SqlValidatorImpl(sqlConverter.getFlattenCounter(),
      createOperatorTable(
        sqlConverter,
        catalogReader,
        contextualSqlOperatorTable),
      catalogReader, sqlConverter.getTypeFactory(),
      DremioSqlConformance.INSTANCE,
      sqlConverter.getSettings().getOptions());
    validator.setIdentifierExpansion(true);
    return validator;
  }

  private static SqlOperatorTable createOperatorTable(SqlConverter sqlConverter,
      DremioCatalogReader catalogReader,
      @Nullable SqlOperatorTable contextualSqlOperatorTable) {
    if (null == contextualSqlOperatorTable) {
      return SqlOperatorTables.chain(
        sqlConverter.getOpTab(),
        catalogReader
      );
    } else {
      return SqlOperatorTables.chain(
        contextualSqlOperatorTable,
        sqlConverter.getOpTab(),
        catalogReader
      );
    }
  }

  private static SqlSelect toSelect(SqlNode sqlNode) {
    if(sqlNode instanceof SqlSelect) {
      return (SqlSelect) sqlNode;
    } else {
      return new SqlSelect(SqlParserPos.ZERO, null,
        new SqlNodeList(ImmutableList.of(sqlNode), SqlParserPos.ZERO),
        null, null, null, null, null, null, null, null, null, null);
    }
  }
}
