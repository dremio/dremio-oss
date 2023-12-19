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
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;
import com.dremio.exec.planner.sql.parser.DremioHint;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * Scope for validation and SQL to Rel.
 */
public class SqlValidatorAndToRelContext {
  private static final org.slf4j.Logger logger = getLogger(SqlValidatorAndToRelContext.class);
  private final SqlConverter sqlConverter;
  private final DremioCatalogReader dremioCatalogReader;
  private final SqlValidatorImpl validator;
  private final ExpansionType expansionType;

  public SqlValidatorAndToRelContext(
    SqlConverter sqlConverter,
    DremioCatalogReader dremioCatalogReader,
    @Nullable SqlOperatorTable contextualSqlOperatorTable,
    ExpansionType expansionType) {
    this.sqlConverter = sqlConverter;
    this.dremioCatalogReader = dremioCatalogReader;
    this.validator = createValidator(sqlConverter, dremioCatalogReader, contextualSqlOperatorTable);
    this.expansionType = expansionType;
  }

  public SqlNode validate(final SqlNode parsedNode) {
    resolveVersionedTableExpressions(parsedNode);
    SqlNode node = validator.validate(parsedNode);
    dremioCatalogReader.validateSelection();
    return node;
  }

  public RelNode validateAndConvertForExpression(final SqlNode sqlNode) {
    SqlToRelConverter.ConfigBuilder configBuilder = createDefaultSqlToRelConfigBuilder();

    final SqlToRelConverter.Config config = configBuilder.build();
    final DremioSqlToRelConverter sqlToRelConverter = new DremioSqlToRelConverter(
      sqlConverter,
      dremioCatalogReader,
      validator,
      sqlConverter.getConvertletTable(),
      config);

    final SqlNode query = toQuery(sqlNode);
    return sqlToRelConverter.convertQuery(query, true, true).rel;
  }

  /**
   * Returns a rel root that defers materialization of scans via {@link com.dremio.exec.planner.logical.ConvertibleScan}
   *
   * Used for serialization.
   */
  public SqlConverter.RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean flatten) {
    return toConvertibleRelRoot(validatedNode, flatten, true);
  }

  public SqlConverter.RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean flatten, boolean withConvertTableAccess) {

    final OptionResolver o = sqlConverter.getSettings().options;
    ReflectionAllowedMonitoringConvertletTable.ConvertletTableNotes convertletTableNotes =
      sqlConverter.getConvertletTableNotes();
    SqlRexConvertletTable convertletTable = sqlConverter.getConvertletTable();
    final long inSubQueryThreshold =  o.getOption(ExecConstants.FAST_OR_ENABLE) ? o.getOption(ExecConstants.FAST_OR_MAX_THRESHOLD) : o.getOption(ExecConstants.PLANNER_IN_SUBQUERY_THRESHOLD);
    final SqlToRelConverter.Config config = createDefaultSqlToRelConfigBuilder()
      .withInSubQueryThreshold((int) inSubQueryThreshold)
      .withConvertTableAccess(withConvertTableAccess && o.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT))
      .withHintStrategyTable(DremioHint.buildHintStrategyTable())
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
    if (!isComplexTypeSupport) {
      rel3 = rel2.accept(new RexSubQueryUtils.RelsWithRexSubQueryFlattener(sqlToRelConverter));
    } else {
      rel3 = rel2;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ConvertQuery:\n{}", RelOptUtil.toString(rel3, SqlExplainLevel.ALL_ATTRIBUTES));
    }
    return SqlConverter.RelRootPlus.of(rel3, rel.validatedRowType, rel.kind, convertletTableNotes.isReflectionDisallowed(), convertletTableNotes.isPlanCacheable());
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


  /**
   * This performs a special pass over the SqlNode AST to resolve any versioned table references containing constant
   * expressions to an equivalent form with those expressions resolved to a SqlLiteral.  This is necessary so that
   * catalog lookups performed during validation can be provided with the resolved version context.
   */
  private void resolveVersionedTableExpressions(final SqlNode parsedNode) {
    final SqlToRelConverter.Config config = createDefaultSqlToRelConfigBuilder().build();
    final SqlToRelConverter sqlToRelConverter = new DremioSqlToRelConverter(sqlConverter, dremioCatalogReader, validator, sqlConverter.getConvertletTable(), config);
    VersionedTableExpressionResolver resolver = new VersionedTableExpressionResolver(validator, sqlConverter.getCluster().getRexBuilder());
    resolver.resolve(sqlToRelConverter, parsedNode);
  }

  private SqlToRelConverter.ConfigBuilder createDefaultSqlToRelConfigBuilder() {
    final OptionResolver options = sqlConverter.getSettings().options;

    SqlToRelConverter.ConfigBuilder configBuilder = SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(true);

    switch (expansionType) {
      case DEFAULT:
        configBuilder =
          configBuilder.withExpand(options.getOption(PlannerSettings.USE_SQL_TO_REL_SUB_QUERY_EXPANSION));
        break;
      case SQL_TO_REL:
        configBuilder = configBuilder.withExpand(true);
        break;
      case REX_SUB_QUERY:
        configBuilder = configBuilder.withExpand(false);
        break;
    }
    return configBuilder;
  }

  public static Builder builder(SqlConverter sqlConverter) {
    return new Builder(sqlConverter,
      sqlConverter.getPlannerCatalog(),
      null,
      ExpansionType.DEFAULT);
  }

  public SqlConverter getSqlConverter() {
    return sqlConverter;
  }

  public static class Builder {
    final SqlConverter sqlConverter;
    final PlannerCatalog catalog;
    final ExpansionType expansionType;
    @Nullable final SqlOperatorTable contextualSqlOperatorTable;

    public Builder(
      SqlConverter sqlConverter,
      PlannerCatalog catalog,
      SqlOperatorTable contextualSqlOperatorTable,
      ExpansionType expansionType) {
      this.sqlConverter = sqlConverter;
      this.catalog = catalog;
      this.contextualSqlOperatorTable = contextualSqlOperatorTable;
      this.expansionType = expansionType;
    }

    public Builder withSchemaPath(List<String> schemaPath) {
      NamespaceKey withSchemaPath = schemaPath == null ? null : new NamespaceKey(schemaPath);

      return new Builder(
        sqlConverter,
        catalog.resolvePlannerCatalog(withSchemaPath),
        contextualSqlOperatorTable,
        expansionType);
    }

    public Builder withCatalog(final Function<Catalog, Catalog> catalogTransformer) {
      return new Builder(
        sqlConverter,
        catalog.resolvePlannerCatalog(catalogTransformer),
        contextualSqlOperatorTable,
        expansionType);
    }

    public Builder withUser(CatalogIdentity user) {
      return new Builder(
        sqlConverter,
        catalog.resolvePlannerCatalog(user),
        contextualSqlOperatorTable,
        expansionType);
    }

    public Builder withVersionContext(String source, TableVersionContext versionContext) {
      final Map<String, VersionContext> sourceVersionMapping = new HashMap<>();
      sourceVersionMapping.put(source, versionContext.asVersionContext());
      return new Builder(
        sqlConverter,
        catalog.resolvePlannerCatalog(sourceVersionMapping),
        contextualSqlOperatorTable,
        expansionType);
    }
    public Builder withContextualSqlOperatorTable(SqlOperatorTable contextualSqlOperatorTable) {
      return new Builder(
        sqlConverter,
        catalog,
        contextualSqlOperatorTable,
        expansionType);
    }

    public Builder withSystemDefaultParserConfig() {
      return new Builder(
        sqlConverter.withSystemDefaultParserConfig(),
        catalog,
        contextualSqlOperatorTable,
        expansionType);
    }

    public Builder disallowSubqueryExpansion() {
      return new Builder(
        sqlConverter.withSystemDefaultParserConfig(),
        catalog,
        contextualSqlOperatorTable,
        ExpansionType.REX_SUB_QUERY);
    }

    public Builder requireSubqueryExpansion() {
      return new Builder(
        sqlConverter.withSystemDefaultParserConfig(),
        catalog,
        contextualSqlOperatorTable,
        ExpansionType.SQL_TO_REL);
    }

    public SqlValidatorAndToRelContext build() {
      return new SqlValidatorAndToRelContext(sqlConverter,
        new DremioCatalogReader(catalog),
        contextualSqlOperatorTable,
        expansionType);
    }

    public Catalog getMetadataCatalog() {
      return catalog.getMetadataCatalog();
    }
  }

  private static SqlValidatorImpl createValidator(
    SqlConverter sqlConverter,
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

  private static SqlNode toQuery(SqlNode node) {
    // This is here since we might have some old UDFs that are not in a normalized format
    // But the main logic should happen now in CreateFunctionHandler
    final SqlKind kind = node.getKind();
    switch (kind) {
      // These are the node types that we know are already a query.
      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case WITH:
      case VALUES:
        return node;
      default:
        // We need to convert scalar values into a select statement
        return new SqlSelect(
          SqlParserPos.ZERO,
          null,
          new SqlNodeList(ImmutableList.of(node),
            SqlParserPos.ZERO),
          null, null, null, null, null, null, null, null, null, null);
    }
  }

  public enum ExpansionType {
    DEFAULT,
    SQL_TO_REL,
    REX_SUB_QUERY;
  }
}
