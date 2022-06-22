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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.udf.SqlUserDefinedFunctionExpanderRule;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.AccelerationAwareSubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.SqlValidatorImpl.FlattenOpCounter;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Class responsible for managing parsing, validation and toRel conversion for sql statements.
 */
public class SqlConverter {
  private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

  private final JavaTypeFactory typeFactory;
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
  private final Catalog catalog;
  private final SqlRexConvertletTable convertletTable;
  private final ReflectionAllowedMonitoringConvertletTable.ConvertletTableNotes convertletTableNotes;

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
      final ScanResult scanResult,
      final RelMetadataQuerySupplier relMetadataQuerySupplier
      ) {
    this.catalog = catalog;
    this.flattenCounter = new FlattenOpCounter();
    this.observer = observer;
    this.settings = settings;
    this.functionContext = functionContext;
    this.functions = functions;
    this.session = Preconditions.checkNotNull(session, "user session is required");
    this.parserConfig = ParserConfig.newInstance(session, settings);
    this.typeFactory = JavaTypeFactoryImpl.INSTANCE;
    this.opTab = operatorTable;
    this.costFactory = (settings.useDefaultCosting()) ? null : new DremioCost.Factory();
    this.materializations = new MaterializationList(this, session, materializationProvider);
    this.substitutions = AccelerationAwareSubstitutionProvider.of(factory.getSubstitutionProvider(config,  materializations, this.settings.options,
      operatorTable instanceof OperatorTable ? (OperatorTable) operatorTable : new OperatorTable(functions)));
    this.planner = DremioVolcanoPlanner.of(this);
    this.cluster = RelOptCluster.create(planner, new DremioRexBuilder(typeFactory));
    this.cluster.setMetadataQuery(relMetadataQuerySupplier);
    this.viewExpansionContext = new ViewExpansionContext(CatalogUser.from(session.getCredentials().getUserName()));
    this.config = config;
    this.scanResult = scanResult;
    this.convertletTableNotes = new ReflectionAllowedMonitoringConvertletTable.ConvertletTableNotes();
    this.convertletTable =
      new ChainedSqlRexConvertletTable(
        new ReflectionAllowedMonitoringConvertletTable(convertletTableNotes),
        new SqlUserDefinedFunctionExpanderRule.UserDefinedFunctionSqlRexConvertletTable(this),
        new ConvertletTable(
          functionContext.getContextInformation(),
          settings.getOptions().getOption(PlannerSettings.IEEE_754_DIVIDE_SEMANTICS)));
  }


  private static SqlNodeList parseMultipleStatementsImpl(String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
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
    return parseMultipleStatementsImpl(sql, getParserConfig(), false);
  }

  @VisibleForTesting
  static SqlNode parseSingleStatementImpl(String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
    SqlNodeList list = parseMultipleStatementsImpl(sql, parserConfig, isInnerQuery);
    if (list.size() > 1) {
      UserException.Builder builder = UserException.parseError();
      builder.message("Dremio only supports single statement execution. Unable to execute the given query with %s statements: %s",
        list.size(), sql);
      throw builder.buildSilently();
    }
    SqlNode newNode = list.get(0);
    return newNode;
  }

  public SqlNode parse(String sql) {
    return parseSingleStatementImpl(sql, getParserConfig(), false);
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

  public ReflectionAllowedMonitoringConvertletTable.ConvertletTableNotes getConvertletTableNotes() {
    return convertletTableNotes;
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

  public Catalog getCatalog() {
    return catalog;
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
    boolean isLegacy = settings != null && settings.getOptions() != null &&
      settings.getOptions().getOption(PlannerSettings.LEGACY_SERIALIZER_ENABLED);
    return isLegacy ?
      RelSerializerFactory.getLegacyPlanningFactory(config, scanResult) :
      RelSerializerFactory.getPlanningFactory(config, scanResult);
  }

  public RelSerializerFactory getLegacySerializerFactory() {
    return RelSerializerFactory.getLegacyPlanningFactory(config, scanResult);
  }

  public SabotConfig getConfig() {
    return config;
  }

  FlattenOpCounter getFlattenCounter() {
    return flattenCounter;
  }

  public SqlRexConvertletTable getConvertletTable() {
    return convertletTable;
  }

  /**
   * A RelRoot that carries additional conversion information.
   */
  public static class RelRootPlus extends RelRoot {

    private final boolean contextSensitive;
    private final boolean planCacheable;

    private RelRootPlus(RelNode rel, RelDataType validatedRowType, SqlKind kind, List<Pair<Integer, String>> fields,
        RelCollation collation, boolean contextSensitive, boolean planCacheable) {
      super(rel, validatedRowType, kind, fields, collation, ImmutableList.of());
      this.contextSensitive = contextSensitive;
      this.planCacheable = planCacheable;
    }

    public static RelRootPlus of(RelRoot root, boolean contextSensitive, boolean planCacheable) {
      return new RelRootPlus(root.rel,root.validatedRowType,root.kind,root.fields,root.collation,contextSensitive,planCacheable);
    }

    public static RelRootPlus of(RelNode rel, RelDataType validatedRowType, SqlKind kind, boolean contextSensitive) {
      return RelRootPlus.of(rel, validatedRowType,kind,contextSensitive,true);
    }

    public static RelRootPlus of(RelNode rel, RelDataType validatedRowType, SqlKind kind, boolean contextSensitive, boolean planCacheable) {
      final ImmutableIntList refs = ImmutableIntList.identity(validatedRowType.getFieldCount());
      final List<String> names = validatedRowType.getFieldNames();
      return new RelRootPlus(rel, validatedRowType, kind, Pair.zip(refs, names), RelCollations.EMPTY, contextSensitive,planCacheable);
    }

    public boolean isContextSensitive() {
      return contextSensitive;
    }

    public boolean isPlanCacheable() {
      return planCacheable;
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
