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

import java.util.List;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.AccelerationAwareSubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.DremioRelDecorrelator;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.SqlValidatorImpl.FlattenOpCounter;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Preconditions;

/**
 * Class responsible for managing parsing, validation and toRel conversion for sql statements.
 */
public class SqlConverter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlConverter.class);

  private final JavaTypeFactory typeFactory;
  private final ParserConfig parserConfig;
  private final DremioCatalogReader catalogReader;
  private final PlannerSettings settings;
  private final SqlOperatorTable opTab;
  private final RelOptCostFactory costFactory;
  private final SqlValidatorImpl validator;
  private final boolean isInnerQuery;
  private final FunctionContext functionContext;
  private final FunctionImplementationRegistry functions;
  private final RelOptPlanner planner;
  private final RelOptCluster cluster;
  private final AttemptObserver observer;
  private final int nestingLevel;
  private final AccelerationAwareSubstitutionProvider substitutions;
  private final MaterializationList materializations;
  private final UserSession session;
  private final ViewExpansionContext viewExpansionContext;
  private final FlattenOpCounter flattenCounter;
  private final ScanResult scanResult;
  private final SabotConfig config;

  public SqlConverter(
      final PlannerSettings settings,
      final SqlOperatorTable operatorTable,
      final FunctionContext functionContext,
      final MaterializationDescriptorProvider materializationProvider,
      final FunctionImplementationRegistry functions,
      final UserSession session,
      final AttemptObserver observer,
      final Catalog catalog,
      final SubstitutionProviderFactory factory,
      final SabotConfig config,
      final ScanResult scanResult
      ) {
    this.nestingLevel = 0;
    this.flattenCounter = new FlattenOpCounter();
    this.observer = observer;
    this.settings = settings;
    this.functionContext = functionContext;
    this.functions = functions;
    this.session = Preconditions.checkNotNull(session, "user session is required");
    this.parserConfig = ParserConfig.newInstance(session, settings);
    this.isInnerQuery = false;
    this.typeFactory = JavaTypeFactoryImpl.INSTANCE;
    this.catalogReader = new DremioCatalogReader(catalog, typeFactory);
    this.opTab = operatorTable;
    this.costFactory = (settings.useDefaultCosting()) ? null : new DremioCost.Factory();
    this.validator = new SqlValidatorImpl(flattenCounter, ChainedSqlOperatorTable.of(opTab, catalogReader), this.catalogReader, typeFactory, DremioSqlConformance.INSTANCE);
    validator.setIdentifierExpansion(true);
    this.materializations = new MaterializationList(this, session, materializationProvider);
    this.substitutions = AccelerationAwareSubstitutionProvider.of(factory.getSubstitutionProvider(config,  materializations, this.settings.options));
    this.planner = DremioVolcanoPlanner.of(this);
    this.cluster = RelOptCluster.create(planner, new DremioRexBuilder(typeFactory));
    this.cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    this.viewExpansionContext = new ViewExpansionContext(session.getCredentials().getUserName());
    this.config = config;
    this.scanResult = scanResult;
  }

  public SqlConverter(SqlConverter parent, DremioCatalogReader catalog) {
    this.nestingLevel = parent.nestingLevel + 1;
    // since this is level 1 or deeper, we need to use system defaults instead of any overridden edge parser.
    this.parserConfig = parent.parserConfig.cloneWithSystemDefault();
    this.substitutions = parent.substitutions;
    this.functions = parent.functions;
    this.session = parent.session;
    this.functionContext = parent.functionContext;
    this.isInnerQuery = true;
    this.observer = parent.observer;
    this.typeFactory = parent.typeFactory;
    this.costFactory = parent.costFactory;
    this.settings = parent.settings;
    this.flattenCounter = parent.flattenCounter;
    this.cluster = parent.cluster;
    this.catalogReader = catalog;
    this.opTab = parent.opTab;
    this.planner = parent.planner;
    this.materializations = parent.materializations;
    // Note: Do not use the parent SqlConverter's catalog's operator table to validate user-defined table functions.
    // They may be inaccessible and will cause validation errors before checking if the functions are valid within the local context.
    this.validator = new SqlValidatorImpl(parent.flattenCounter, ChainedSqlOperatorTable.of(opTab, catalog), catalog, typeFactory, DremioSqlConformance.INSTANCE);
    validator.setIdentifierExpansion(true);
    this.viewExpansionContext = parent.viewExpansionContext;
    this.config = parent.config;
    this.scanResult = parent.scanResult;
  }

  public static final SqlShuttle STRING_LITERAL_CONVERTER = new SqlShuttle() {
    @Override
    public SqlNode visit(SqlLiteral literal) {
      if (literal instanceof SqlCharStringLiteral) {
        return SqlVarCharStringLiteral.create((SqlCharStringLiteral) literal);
      }
      return literal;
    }
  };

  private static SqlNodeList parseMultipleStatementsImpl(String sql, ParserConfig parserConfig, boolean isInnerQuery) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      return parser.parseStmtList();
    } catch (SqlParseException e) {
      UserException.Builder builder = SqlExceptionHelper.parseError(sql, e);

      builder.message(isInnerQuery ? SqlExceptionHelper.INNER_QUERY_PARSING_ERROR : SqlExceptionHelper.QUERY_PARSING_ERROR);
      throw builder.build(logger);
    }
  }

  public SqlNodeList parseMultipleStatements(String sql) {
    return parseMultipleStatementsImpl(sql, parserConfig, isInnerQuery);
  }

  @VisibleForTesting
  static SqlNode parseSingleStatementImpl(String sql, ParserConfig parserConfig, boolean isInnerQuery) {
    SqlNodeList list = parseMultipleStatementsImpl(sql, parserConfig, isInnerQuery);
    if (list.size() > 1) {
      SqlParserPos pos = list.get(1).getParserPosition();
      int col = pos.getColumnNum();
      String first = sql.substring(0, col);
      String second = sql.substring(col-1);

      UserException.Builder builder = UserException.parseError();
      builder.message("Unable to parse multiple queries. First query is %s. Rest of submission is %s", first, second);
      throw builder.buildSilently();
    }
    SqlNode newNode = list.get(0).accept(STRING_LITERAL_CONVERTER);
    return newNode;
  }

  public SqlNode parse(String sql) {
    return parseSingleStatementImpl(sql, parserConfig, isInnerQuery);
  }

  public ViewExpansionContext getViewExpansionContext() {
    return viewExpansionContext;
  }

  public UserSession getSession() {
    return session;
  }

  public SqlNode validate(final SqlNode parsedNode) {
    SqlNode node = validator.validate(parsedNode);
    catalogReader.validateSelection();
    return node;
  }

  public RelDataType getValidatedRowType(String sql) {
    SqlNode sqlNode = parse(sql);
    SqlNode validatedNode = validate(sqlNode);
    return validator.getValidatedNodeType(validatedNode);
  }

  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return functions;
  }

  public PlannerSettings getSettings() {
    return settings;
  }

  public RelDataType getOutputType(SqlNode validatedNode) {
    return validator.getValidatedNodeType(validatedNode);
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public SqlOperatorTable getOpTab() {
    return opTab;
  }

  public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  public FunctionContext getFunctionContext() {
    return functionContext;
  }

  public DremioCatalogReader getCatalogReader() {
    return catalogReader;
  }

  public SqlParser.Config getParserConfig() {
    return parserConfig;
  }

  public AttemptObserver getObserver() {
    return observer;
  }

  public MaterializationList getMaterializations() {
    return materializations;
  }

  public int getNestingLevel() {
    return nestingLevel;
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  public AccelerationAwareSubstitutionProvider getSubstitutionProvider() {
    return substitutions;
  }

  public RelSerializerFactory getSerializerFactory() {
    return RelSerializerFactory.getPlanningFactory(config, scanResult);
  }

  public SabotConfig getConfig() {
    return config;
  }

  /**
   * Returns a rel root that defers materialization of scans via {@link com.dremio.exec.planner.logical.ConvertibleScan}
   *
   * Used for serialization.
   */
  public RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean expand, boolean flatten) {
    return toConvertibleRelRoot(validatedNode, expand, flatten, true);
  }

  public RelRootPlus toConvertibleRelRoot(final SqlNode validatedNode, boolean expand, boolean flatten, boolean withConvertTableAccess) {

    final OptionManager o = settings.getOptions();
    final long inSubQueryThreshold =  o.getOption(ExecConstants.FAST_OR_ENABLE) ? o.getOption(ExecConstants.FAST_OR_MAX_THRESHOLD) : settings.getOptions().getOption(ExecConstants.PLANNER_IN_SUBQUERY_THRESHOLD);
    final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
      .withInSubQueryThreshold((int) inSubQueryThreshold)
      .withTrimUnusedFields(true)
      .withConvertTableAccess(withConvertTableAccess && o.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT))
      .withExpand(expand)
      .build();
    final ReflectionAllowedMonitoringConvertletTable convertletTable = new ReflectionAllowedMonitoringConvertletTable(new ConvertletTable(functionContext.getContextInformation()));
    final SqlToRelConverter sqlToRelConverter = new DremioSqlToRelConverter(this, validator, convertletTable, config);
    final boolean isComplexTypeSupport = o.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT);
    // Previously we had "top" = !innerQuery, but calcite only adds project if it is not a top query.
    final RelRoot rel = sqlToRelConverter.convertQuery(validatedNode, false /* needs validate */, false /* top */);
    if (!flatten) {
      return RelRootPlus.of(rel.rel, rel.validatedRowType, rel.kind, convertletTable.isReflectionDisallowed());
    }
    RelNode rel2 = rel.rel;
    if (!isComplexTypeSupport) {
      rel2 = sqlToRelConverter.flattenTypes(rel.rel, true);
    } else {
      rel2 = MoreRelOptUtil.StructuredConditionRewriter.rewrite(rel.rel);
    }

    RelNode converted;
    RelNode rel3 = rel2;
    if (expand && !isComplexTypeSupport) {
      rel3 = rel2.accept(new RexSubQueryUtils.RelsWithRexSubQueryFlattener(sqlToRelConverter));
    }
    if (settings.isRelPlanningEnabled()) {
      converted = rel3;
    } else {
      converted = DremioRelDecorrelator.decorrelateQuery(rel3, DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(rel3.getCluster(), null), false, false);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("ConvertQuery with expand = {}:\n{}", expand, RelOptUtil.toString(converted, SqlExplainLevel.ALL_ATTRIBUTES));
    }
    return RelRootPlus.of(converted, rel.validatedRowType, rel.kind, convertletTable.isReflectionDisallowed());
  }

  /**
   * A RelRoot that carries additional conversion information.
   */
  public static class RelRootPlus extends RelRoot {

    private final boolean contextSensitive;

    private RelRootPlus(RelNode rel, RelDataType validatedRowType, SqlKind kind, List<Pair<Integer, String>> fields,
        RelCollation collation, boolean contextSensitive) {
      super(rel, validatedRowType, kind, fields, collation);
      this.contextSensitive = contextSensitive;
    }

    public static RelRootPlus of(RelNode rel, RelDataType validatedRowType, SqlKind kind, boolean contextSensitive) {
      final ImmutableIntList refs = ImmutableIntList.identity(validatedRowType.getFieldCount());
      final List<String> names = validatedRowType.getFieldNames();
      return new RelRootPlus(rel, validatedRowType, kind, Pair.zip(refs, names), RelCollations.EMPTY, contextSensitive);
    }

    public boolean isContextSensitive() {
      return contextSensitive;
    }
  }

  /**
   *
   * @param sql
   *          the SQL sent to the server
   * @param pos
   *          the position of the error
   * @return The sql with a ^ character under the error
   */
  static String formatSQLParsingError(String sql, SqlParserPos pos) {
    StringBuilder sb = new StringBuilder();
    String[] lines = sql.split("\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      sb.append(line).append("\n");
      if (i == (pos.getLineNum() - 1)) {
        for (int j = 0; j < pos.getColumnNum() - 1; j++) {
          sb.append(" ");
        }
        sb.append("^\n");
      }
    }
    return sb.toString();
  }

}
