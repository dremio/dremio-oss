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

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.AccelerationAwareSubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.catalog.AutoVDSFixer;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.SqlValidatorImpl.FlattenOpCounter;
import com.dremio.exec.planner.sql.handlers.PlannerUtil;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for managing parsing, validation and toRel conversion for sql statements. Owns
 * the PlannerCatalog.
 */
public class SqlConverter {
  private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

  private final ParserConfig parserConfig;
  private final PlannerSettings settings;
  private final SqlOperatorTable opTab;
  private final RelOptCostFactory costFactory;
  private final FunctionContext functionContext;
  private final FunctionImplementationRegistry functions;
  private final RelOptPlanner planner;
  private final RelOptCluster cluster;
  private final AttemptObserver observer;
  private final AccelerationAwareSubstitutionProvider substitutions;
  private final MaterializationList materializations;
  private final UserSession session;
  private final ViewExpansionContext viewExpansionContext;
  private final FlattenOpCounter flattenCounter;
  private final ScanResult scanResult;
  private final SabotConfig config;
  private final PlannerCatalog plannerCatalog;
  private final SqlRexConvertletTable convertletTable;
  private final ViewExpander viewExpander;
  private final SqlValidatorAndToRelContextBuilderFactoryImpl
      userQuerySqlValidatorAndToRelContextBuilderFactory;
  private final SqlValidatorAndToRelContextBuilderFactoryImpl
      expansionSqlValidatorAndToRelContextBuilderFactory;
  private final DremioToRelContext.DremioQueryToRelContext toRelContext;

  public SqlConverter(
      final PlannerSettings settings,
      final SqlOperatorTable operatorTable,
      final QueryContext context,
      final MaterializationDescriptorProvider materializationProvider,
      final FunctionImplementationRegistry functions,
      final UserSession session,
      final AttemptObserver observer,
      final SubstitutionProviderFactory factory,
      final SabotConfig config,
      final ScanResult scanResult,
      final RelMetadataQuerySupplier relMetadataQuerySupplier) {
    this.flattenCounter = new FlattenOpCounter();
    this.observer = observer;
    this.settings = checkNotNull(settings, "settings");
    this.functionContext = context;
    this.functions = functions;
    this.session = checkNotNull(session, "user session is required");
    this.opTab = operatorTable;
    this.costFactory = (settings.useDefaultCosting()) ? null : new DremioCost.Factory();
    this.materializations = new MaterializationList(this, session, materializationProvider);
    this.substitutions =
        buildAccelerationAwareSubstitutionProvider(
            config, context, factory, materializations, observer, settings);
    this.planner = DremioVolcanoPlanner.of(this);
    this.cluster =
        RelOptCluster.create(planner, new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE));
    this.cluster.setMetadataQuery(relMetadataQuerySupplier);
    this.viewExpansionContext =
        new ViewExpansionContext(CatalogUser.from(session.getCredentials().getUserName()));
    this.config = config;
    this.scanResult = scanResult;
    this.convertletTable =
        new ConvertletTable(
            settings.getOptions().getOption(PlannerSettings.IEEE_754_DIVIDE_SEMANTICS));
    AutoVDSFixer autoVDSFixer =
        new AutoVDSFixer(
            context.getCatalog(), settings.getOptions(), session, context.getWorkloadType());

    // There is a circular dependency of:
    //  expansionSqlValidatorAndToRelContextBuilderFactory -> viewExpander -> ToRelContext -> ...
    // And
    //  expansionSqlValidatorAndToRelContextBuilderFactory -> viewExpander -> PlannerCatalog -> ...
    //
    // This circular dependency is needed since expanding view and udf is a recursive algorithm.

    this.parserConfig = ParserConfig.newInstance(session, settings);
    ParserConfig expansionPaserConfig = parserConfig.cloneWithSystemDefault();

    this.userQuerySqlValidatorAndToRelContextBuilderFactory =
        new SqlValidatorAndToRelContextBuilderFactoryImpl(
            new DremioStringToSqlNode(parserConfig),
            operatorTable,
            flattenCounter,
            convertletTable,
            cluster,
            settings.getOptions(),
            context.getContextInformation());
    this.expansionSqlValidatorAndToRelContextBuilderFactory =
        new SqlValidatorAndToRelContextBuilderFactoryImpl(
            new DremioStringToSqlNode(expansionPaserConfig),
            operatorTable,
            flattenCounter,
            convertletTable,
            cluster,
            settings.getOptions(),
            context.getContextInformation());

    this.viewExpander =
        new ViewExpander(
            this.expansionSqlValidatorAndToRelContextBuilderFactory,
            this.functionContext.getContextInformation(),
            this.viewExpansionContext,
            this.substitutions,
            autoVDSFixer);
    this.toRelContext =
        DremioToRelContext.createQueryContext(
            this.expansionSqlValidatorAndToRelContextBuilderFactory,
            this.cluster,
            this.viewExpander,
            context.getMetadataStatsCollector());
    this.plannerCatalog = context.createPlannerCatalog(this.viewExpander, settings.getOptions());

    userQuerySqlValidatorAndToRelContextBuilderFactory.initialize(toRelContext, plannerCatalog);
    expansionSqlValidatorAndToRelContextBuilderFactory.initialize(toRelContext, plannerCatalog);
  }

  @Deprecated
  private static SqlNodeList parseMultipleStatementsImpl(
      String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      return parser.parseStmtList();
    } catch (SqlParseException e) {
      UserException.Builder builder = SqlExceptionHelper.parseError(sql, e);

      if (e.getCause() instanceof StackOverflowError) {
        builder.message(SqlExceptionHelper.PLANNING_STACK_OVERFLOW_ERROR);
      } else if (isInnerQuery) {
        builder.message("Failure parsing a view your query is dependent upon.");
      }

      throw builder.build(logger);
    }
  }

  @Deprecated
  @VisibleForTesting
  static SqlNode parseSingleStatementImpl(
      String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
    SqlNodeList list = parseMultipleStatementsImpl(sql, parserConfig, isInnerQuery);
    if (list.size() > 1) {
      UserException.Builder builder = UserException.parseError();
      builder.message(
          "Dremio only supports single statement execution. Unable to execute the given query with %s statements: %s",
          list.size(), sql);
      throw builder.buildSilently();
    }
    SqlNode newNode = list.get(0);
    return newNode;
  }

  @Deprecated
  public SqlNode parse(String sql) {
    return parseSingleStatementImpl(sql, parserConfig, false);
  }

  public SqlValidatorAndToRelContext.BuilderFactory
      getUserQuerySqlValidatorAndToRelContextBuilderFactory() {
    return userQuerySqlValidatorAndToRelContextBuilderFactory;
  }

  public SqlValidatorAndToRelContext.BuilderFactory
      getExpansionSqlValidatorAndToRelContextBuilderFactory() {
    return expansionSqlValidatorAndToRelContextBuilderFactory;
  }

  public ViewExpansionContext getViewExpansionContext() {
    return viewExpansionContext;
  }

  public UserSession getSession() {
    return session;
  }

  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return functions;
  }

  public PlannerSettings getSettings() {
    return settings;
  }

  public OptionResolver getOptionResolver() {
    return settings.getOptions();
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

  public SqlParser.Config getParserConfig() {
    return parserConfig;
  }

  public AttemptObserver getObserver() {
    return observer;
  }

  public MaterializationList getMaterializations() {
    return materializations;
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  public AccelerationAwareSubstitutionProvider getSubstitutionProvider() {
    return substitutions;
  }

  public RelSerializerFactory getSerializerFactory() {
    boolean isLegacy =
        settings != null
            && settings.getOptions() != null
            && settings.getOptions().getOption(PlannerSettings.LEGACY_SERIALIZER_ENABLED);
    return isLegacy
        ? RelSerializerFactory.getLegacyPlanningFactory(config, scanResult)
        : RelSerializerFactory.getPlanningFactory(config, scanResult);
  }

  public RelSerializerFactory getLegacySerializerFactory() {
    return RelSerializerFactory.getLegacyPlanningFactory(config, scanResult);
  }

  public ViewExpander getViewExpander() {
    return viewExpander;
  }

  public SabotConfig getConfig() {
    return config;
  }

  public PlannerCatalog getPlannerCatalog() {
    return plannerCatalog;
  }

  public DremioToRelContext.DremioQueryToRelContext getToRelContext() {
    return toRelContext;
  }

  /**
   * When the plans generated by this converter are going to be long lived such as in the plan cache
   * or materialization cache, then it is critical to de-reference objects that are no longer needed
   * for garbage collection.
   *
   * <p>Specifically, the plan will hold on to these objects and contents that aren't explicitly
   * disposed. {@link RelNode#getCluster()} {@link RelOptCluster#getPlanner()} {@link
   * RelOptPlanner#getContext()} {@link PlannerSettings} {@link DremioVolcanoPlanner} {@link
   * QueryContext} is also held so some clean up is done in {@link QueryContext#close()}
   */
  public void dispose() {
    final DremioVolcanoPlanner planner = (DremioVolcanoPlanner) this.planner;
    planner.dispose();
    plannerCatalog.dispose();
  }

  /**
   * @param sql the SQL sent to the server
   * @param pos the position of the error
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

  /**
   * This factory requires initialization due to the circular dependencies. These circular
   * dependency are due to the recursive algorithm. This is functionality represent a producer of
   * scopes for a parsing of a user queries, views or udfs.
   */
  private static class SqlValidatorAndToRelContextBuilderFactoryImpl
      implements SqlValidatorAndToRelContext.BuilderFactory {
    private final DremioStringToSqlNode dremioStringToSqlNode;
    private final SqlOperatorTable globalSqlOperatorTable;
    private final SqlValidatorImpl.FlattenOpCounter flattenOpCounter;
    private final SqlRexConvertletTable sqlRexConvertletTable;
    private final RelOptCluster relOptCluster;
    private final OptionResolver optionResolver;
    private final ContextInformation contextInformation;

    private RelOptTable.ToRelContext toRelContext = null;
    private PlannerCatalog plannerCatalog = null;
    private boolean isInitialized = false;

    public SqlValidatorAndToRelContextBuilderFactoryImpl(
        DremioStringToSqlNode dremioStringToSqlNode,
        SqlOperatorTable globalSqlOperatorTable,
        FlattenOpCounter flattenOpCounter,
        SqlRexConvertletTable sqlRexConvertletTable,
        RelOptCluster relOptCluster,
        OptionResolver optionResolver,
        ContextInformation contextInformation) {
      this.dremioStringToSqlNode = dremioStringToSqlNode;
      this.globalSqlOperatorTable = globalSqlOperatorTable;
      this.flattenOpCounter = flattenOpCounter;
      this.sqlRexConvertletTable = sqlRexConvertletTable;
      this.relOptCluster = relOptCluster;
      this.optionResolver = optionResolver;
      this.contextInformation = contextInformation;
    }

    void initialize(RelOptTable.ToRelContext toRelContext, PlannerCatalog plannerCatalog) {
      if (isInitialized) {
        throw new IllegalStateException(
            "SqlValidatorAndToRelContextBuilderFactory is already initialized");
      }
      this.toRelContext = toRelContext;
      this.plannerCatalog = plannerCatalog;
      this.isInitialized = true;
    }

    @Override
    public SqlValidatorAndToRelContext.Builder builder() {
      if (!isInitialized) {
        throw new IllegalStateException("Uninitialized SqlValidatorAndToRelContextBuilderFactory");
      }
      return SqlValidatorAndToRelContext.builder(
          plannerCatalog,
          dremioStringToSqlNode,
          globalSqlOperatorTable,
          flattenOpCounter,
          toRelContext,
          sqlRexConvertletTable,
          relOptCluster,
          optionResolver,
          contextInformation);
    }
  }

  private static AccelerationAwareSubstitutionProvider buildAccelerationAwareSubstitutionProvider(
      SabotConfig sabotConfig,
      QueryContext queryContext,
      SubstitutionProviderFactory factory,
      MaterializationList materializations,
      AttemptObserver attemptObserver,
      PlannerSettings plannerSettings) {
    AccelerationAwareSubstitutionProvider substitutions =
        AccelerationAwareSubstitutionProvider.of(
            factory.getSubstitutionProvider(
                sabotConfig, materializations, plannerSettings.getOptions()));
    substitutions.setObserver(attemptObserver);
    substitutions.setPostSubstitutionTransformer(
        PlannerUtil.createPostSubstitutionTransformer(
            queryContext, plannerSettings, PlannerPhase.POST_SUBSTITUTION));
    return substitutions;
  }
}
