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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.DremioHint;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/** Scope for validation and SQL to Rel. */
public class SqlValidatorAndToRelContext {
  private static final org.slf4j.Logger LOGGER = getLogger(SqlValidatorAndToRelContext.class);
  private final RelOptCluster relOptCluster;
  private final DremioStringToSqlNode dremioStringToSqlNode;
  private final DremioCatalogReader dremioCatalogReader;
  private final SqlValidatorImpl validator;
  private final ExpansionType expansionType;
  private final OptionResolver optionResolver;
  private final RelOptTable.ToRelContext toRelContext;
  private final SqlRexConvertletTable sqlRexConvertletTable;
  private final ContextInformation contextInformation;
  private final Meter.MeterProvider<Counter> ambiguousColumnCounter;

  public SqlValidatorAndToRelContext(
      SqlValidatorImpl sqlValidator,
      DremioCatalogReader dremioCatalogReader,
      ExpansionType expansionType,
      DremioStringToSqlNode dremioStringToSqlNode,
      RelOptTable.ToRelContext toRelContext,
      SqlRexConvertletTable sqlRexConvertletTable,
      OptionResolver optionResolver,
      RelOptCluster relOptCluster,
      ContextInformation contextInformation) {
    this.relOptCluster = checkNotNull(relOptCluster, "relOptCluster");
    this.dremioCatalogReader = checkNotNull(dremioCatalogReader, "dremioCatalogReader");
    this.toRelContext = checkNotNull(toRelContext, "toRelContext");
    this.sqlRexConvertletTable = checkNotNull(sqlRexConvertletTable, "sqlRexConvertletTable");
    this.validator = checkNotNull(sqlValidator, "sqlValidator");
    this.expansionType = checkNotNull(expansionType, "expansionType");
    this.dremioStringToSqlNode = checkNotNull(dremioStringToSqlNode, "dremioStringToSqlNode");
    this.optionResolver = checkNotNull(optionResolver, "optionResolver");
    this.contextInformation = checkNotNull(contextInformation, "contextInformation");
    this.ambiguousColumnCounter =
        Counter.builder(
                PlannerMetrics.createName(PlannerMetrics.PREFIX, PlannerMetrics.AMBIGUOUS_COLUMN))
            .description("Tracks how queries with ambiguous columns are found")
            .withRegistry(Metrics.globalRegistry);
  }

  public SqlNode validate(final SqlNode parsedNode) {
    resolveVersionedTableExpressions(parsedNode);
    String sqlQuery = parsedNode.toString();
    SqlNode node = validator.validate(parsedNode);
    checkAmbiguousColumn(validator, sqlQuery);
    dremioCatalogReader.validateSelection();
    return node;
  }

  public void checkAmbiguousColumn(SqlValidator validator, String sqlQuery) {
    if (optionResolver.getOption(PlannerSettings.ALLOW_AMBIGUOUS_COLUMN)
        && optionResolver.getOption(PlannerSettings.WARN_IF_AMBIGUOUS_COLUMN)) {
      // Check only if configured to allow ambiguous columns
      // Otherwise we throw an exception through Calcite.
      if (validator.hasValidationMessage()
          && validator.getValidationMessage().contains("ambiguous")) {
        String msg = validator.getValidationMessage() + " in statement " + sqlQuery;
        LOGGER.warn(msg);
        ambiguousColumnCounter.withTags().increment();
      }
    }
  }

  public RelNode validateAndConvertForExpression(final SqlNode sqlNode) {
    SqlToRelConverter.ConfigBuilder configBuilder = createDefaultSqlToRelConfigBuilder();

    final SqlToRelConverter.Config config = configBuilder.build();
    final DremioSqlToRelConverter sqlToRelConverter =
        new DremioSqlToRelConverter(
            toRelContext,
            relOptCluster,
            dremioCatalogReader,
            validator,
            sqlRexConvertletTable,
            config);

    final SqlNode query = toQuery(sqlNode);
    return sqlToRelConverter.convertQuery(query, true, true).rel;
  }

  /**
   * Returns a rel root that defers materialization of scans via {@link
   * com.dremio.exec.planner.logical.ConvertibleScan}
   *
   * <p>Used for serialization.
   */
  public RelRoot toConvertibleRelRoot(final SqlNode validatedNode, boolean flatten) {
    return toConvertibleRelRoot(validatedNode, flatten, true);
  }

  public RelRoot toConvertibleRelRoot(
      final SqlNode validatedNode, boolean flatten, boolean withConvertTableAccess) {

    final OptionResolver o = optionResolver;

    final long inSubQueryThreshold =
        o.getOption(ExecConstants.FAST_OR_ENABLE)
            ? o.getOption(ExecConstants.FAST_OR_MAX_THRESHOLD)
            : o.getOption(ExecConstants.PLANNER_IN_SUBQUERY_THRESHOLD);
    final SqlToRelConverter.Config config =
        createDefaultSqlToRelConfigBuilder()
            .withInSubQueryThreshold((int) inSubQueryThreshold)
            .withConvertTableAccess(withConvertTableAccess)
            .withHintStrategyTable(DremioHint.buildHintStrategyTable())
            .build();
    final SqlToRelConverter sqlToRelConverter =
        new DremioSqlToRelConverter(
            toRelContext,
            relOptCluster,
            dremioCatalogReader,
            validator,
            sqlRexConvertletTable,
            config);
    // Previously we had "top" = !innerQuery, but calcite only adds project if it is not a top
    // query.
    final RelRoot rel =
        sqlToRelConverter.convertQuery(validatedNode, false /* needs validate */, true /* top */);
    if (!flatten) {
      return rel;
    }

    final RelNode rel2 = MoreRelOptUtil.StructuredConditionRewriter.rewrite(rel.rel);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("ConvertQuery:\n{}", RelOptUtil.toString(rel2, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    return RelRoot.of(rel2, rel.validatedRowType, rel.kind);
  }

  public SqlNode parse(String sqlString) {
    return dremioStringToSqlNode.parse(sqlString);
  }

  public SqlValidatorImpl getValidator() {
    return validator;
  }

  public DremioCatalogReader getDremioCatalogReader() {
    return dremioCatalogReader;
  }

  /**
   * This performs a special pass over the SqlNode AST to resolve any versioned table references
   * containing constant expressions to an equivalent form with those expressions resolved to a
   * SqlLiteral. This is necessary so that catalog lookups performed during validation can be
   * provided with the resolved version context.
   */
  private void resolveVersionedTableExpressions(final SqlNode parsedNode) {
    final SqlToRelConverter.Config config = createDefaultSqlToRelConfigBuilder().build();
    final SqlToRelConverter sqlToRelConverter =
        new DremioSqlToRelConverter(
            toRelContext,
            relOptCluster,
            dremioCatalogReader,
            validator,
            sqlRexConvertletTable,
            config);
    VersionedTableExpressionResolver resolver =
        new VersionedTableExpressionResolver(
            validator, relOptCluster.getRexBuilder(), contextInformation);
    resolver.resolve(sqlToRelConverter, parsedNode);
  }

  private SqlToRelConverter.ConfigBuilder createDefaultSqlToRelConfigBuilder() {
    SqlToRelConverter.ConfigBuilder configBuilder =
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true);

    switch (expansionType) {
      case DEFAULT:
        configBuilder =
            configBuilder.withExpand(
                optionResolver.getOption(PlannerSettings.USE_SQL_TO_REL_SUB_QUERY_EXPANSION));
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

  public static Builder builder(
      PlannerCatalog catalog,
      DremioStringToSqlNode dremioStringToSqlNode,
      SqlOperatorTable globalSqlOperatorTable,
      SqlValidatorImpl.FlattenOpCounter flattenOpCounter,
      RelOptTable.ToRelContext toRelContext,
      SqlRexConvertletTable sqlRexConvertletTable,
      RelOptCluster relOptCluster,
      OptionResolver optionResolver,
      ContextInformation contextInformation) {
    return new Builder(
        catalog,
        ExpansionType.DEFAULT,
        dremioStringToSqlNode,
        globalSqlOperatorTable,
        flattenOpCounter,
        toRelContext,
        sqlRexConvertletTable,
        relOptCluster,
        optionResolver,
        contextInformation,
        Function.identity());
  }

  public interface BuilderFactory {
    Builder builder();
  }

  public static class Builder {
    private final OptionResolver optionResolver;
    private final PlannerCatalog catalog;
    private final ExpansionType expansionType;

    private final Function<SqlOperatorTable, SqlOperatorTable> sqlOperatorTableTransformer;
    private final DremioStringToSqlNode dremioStringToSqlNode;
    private final SqlOperatorTable globalSqlOperatorTable;
    private final SqlValidatorImpl.FlattenOpCounter flattenOpCounter;
    private final RelOptTable.ToRelContext toRelContext;
    private final SqlRexConvertletTable sqlRexConvertletTable;
    private final RelOptCluster relOptCluster;
    private final ContextInformation contextInformation;

    public Builder(
        PlannerCatalog catalog,
        ExpansionType expansionType,
        DremioStringToSqlNode dremioStringToSqlNode,
        SqlOperatorTable globalSqlOperatorTable,
        SqlValidatorImpl.FlattenOpCounter flattenOpCounter,
        RelOptTable.ToRelContext toRelContext,
        SqlRexConvertletTable sqlRexConvertletTable,
        RelOptCluster relOptCluster,
        OptionResolver optionResolver,
        ContextInformation contextInformation,
        Function<SqlOperatorTable, SqlOperatorTable> sqlOperatorTableTransformer) {
      this.catalog = catalog;
      this.expansionType = expansionType;
      this.dremioStringToSqlNode = dremioStringToSqlNode;
      this.globalSqlOperatorTable = globalSqlOperatorTable;
      this.flattenOpCounter = flattenOpCounter;
      this.toRelContext = toRelContext;
      this.sqlRexConvertletTable = sqlRexConvertletTable;
      this.relOptCluster = relOptCluster;
      this.optionResolver = optionResolver;
      this.contextInformation = contextInformation;
      this.sqlOperatorTableTransformer = sqlOperatorTableTransformer;
    }

    public Builder(Builder parent, PlannerCatalog plannerCatalog) {
      this.catalog = plannerCatalog;
      this.expansionType = parent.expansionType;
      this.dremioStringToSqlNode = parent.dremioStringToSqlNode;
      this.globalSqlOperatorTable = parent.globalSqlOperatorTable;
      this.flattenOpCounter = parent.flattenOpCounter;
      this.toRelContext = parent.toRelContext;
      this.sqlRexConvertletTable = parent.sqlRexConvertletTable;
      this.relOptCluster = parent.relOptCluster;
      this.optionResolver = parent.optionResolver;
      this.contextInformation = parent.contextInformation;
      this.sqlOperatorTableTransformer = parent.sqlOperatorTableTransformer;
    }

    public Builder withSchemaPath(List<String> schemaPath) {
      NamespaceKey withSchemaPath = schemaPath == null ? null : new NamespaceKey(schemaPath);
      return new Builder(this, catalog.resolvePlannerCatalog(withSchemaPath));
    }

    public Builder withCatalog(final Function<Catalog, Catalog> catalogTransformer) {
      return new Builder(this, catalog.resolvePlannerCatalog(catalogTransformer));
    }

    public Builder withUser(CatalogIdentity user) {
      return new Builder(this, catalog.resolvePlannerCatalog(user));
    }

    public Builder withVersionContext(String source, TableVersionContext versionContext) {
      final Map<String, VersionContext> sourceVersionMapping = new HashMap<>();
      sourceVersionMapping.put(source, versionContext.asVersionContext());
      return new Builder(this, catalog.resolvePlannerCatalog(sourceVersionMapping));
    }

    public Builder withContextualSqlOperatorTable(SqlOperatorTable contextualSqlOperatorTable) {
      return new Builder(
          catalog,
          expansionType,
          dremioStringToSqlNode,
          ChainedSqlOperatorTable.of(globalSqlOperatorTable, contextualSqlOperatorTable),
          flattenOpCounter,
          toRelContext,
          sqlRexConvertletTable,
          relOptCluster,
          optionResolver,
          contextInformation,
          sqlOperatorTableTransformer);
    }

    public Builder withSqlOperatorTableTransformer(
        Function<SqlOperatorTable, SqlOperatorTable> sqlOperatorTableTransformer) {
      return new Builder(
          catalog,
          expansionType,
          dremioStringToSqlNode,
          globalSqlOperatorTable,
          flattenOpCounter,
          toRelContext,
          sqlRexConvertletTable,
          relOptCluster,
          optionResolver,
          contextInformation,
          this.sqlOperatorTableTransformer.andThen(sqlOperatorTableTransformer));
    }

    public Builder disallowSubqueryExpansion() {
      return new Builder(
          catalog,
          ExpansionType.REX_SUB_QUERY,
          dremioStringToSqlNode,
          globalSqlOperatorTable,
          flattenOpCounter,
          toRelContext,
          sqlRexConvertletTable,
          relOptCluster,
          optionResolver,
          contextInformation,
          sqlOperatorTableTransformer);
    }

    public Builder requireSubqueryExpansion() {
      return new Builder(
          catalog,
          ExpansionType.SQL_TO_REL,
          dremioStringToSqlNode,
          globalSqlOperatorTable,
          flattenOpCounter,
          toRelContext,
          sqlRexConvertletTable,
          relOptCluster,
          optionResolver,
          contextInformation,
          sqlOperatorTableTransformer);
    }

    public SqlValidatorAndToRelContext build() {
      DremioCatalogReader dremioCatalogReader = new DremioCatalogReader(catalog);
      SqlOperatorTable contextualAndFixedSqlOperatorTable =
          sqlOperatorTableTransformer.apply(
              ChainedSqlOperatorTable.of(globalSqlOperatorTable, dremioCatalogReader));
      SqlValidatorImpl sqlValidatorImpl =
          createValidator(
              contextualAndFixedSqlOperatorTable,
              optionResolver,
              flattenOpCounter,
              dremioCatalogReader);
      return new SqlValidatorAndToRelContext(
          sqlValidatorImpl,
          dremioCatalogReader,
          expansionType,
          dremioStringToSqlNode,
          toRelContext,
          sqlRexConvertletTable,
          optionResolver,
          relOptCluster,
          contextInformation);
    }
  }

  private static SqlValidatorImpl createValidator(
      SqlOperatorTable operatorTableWithContext,
      OptionResolver optionResolver,
      SqlValidatorImpl.FlattenOpCounter flattenOpCounter,
      DremioCatalogReader catalogReader) {

    SqlValidator.Config config =
        SqlValidator.Config.DEFAULT
            .withSqlConformance(DremioSqlConformance.INSTANCE)
            .withAmbiguousColumnAllowed(
                optionResolver.getOption(PlannerSettings.ALLOW_AMBIGUOUS_COLUMN));
    SqlValidatorImpl validator =
        new SqlValidatorImpl(
            flattenOpCounter,
            operatorTableWithContext,
            catalogReader,
            JavaTypeFactoryImpl.INSTANCE,
            config,
            optionResolver);
    validator.setIdentifierExpansion(true);
    return validator;
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
            new SqlNodeList(ImmutableList.of(node), SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
  }

  private enum ExpansionType {
    DEFAULT,
    SQL_TO_REL,
    REX_SUB_QUERY;
  }
}
