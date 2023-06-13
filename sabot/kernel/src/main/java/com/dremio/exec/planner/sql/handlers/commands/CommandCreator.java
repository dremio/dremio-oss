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
package com.dremio.exec.planner.sql.handlers.commands;

import static com.dremio.exec.planner.physical.PlannerSettings.REUSE_PREPARE_HANDLES;
import static com.dremio.exec.planner.physical.PlannerSettings.STORE_QUERY_RESULTS;

import java.util.Locale;
import java.util.Optional;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PlannerSettings.StoreQueryResultsPolicy;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.AccelAddExternalReflectionHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelCreateReflectionHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelDropReflectionHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelToggleHandler;
import com.dremio.exec.planner.sql.handlers.direct.AddColumnsHandler;
import com.dremio.exec.planner.sql.handlers.direct.AlterClearPlanCacheHandler;
import com.dremio.exec.planner.sql.handlers.direct.AlterTableChangeColumnSetOptionHandler;
import com.dremio.exec.planner.sql.handlers.direct.AlterTablePartitionSpecHandler;
import com.dremio.exec.planner.sql.handlers.direct.AlterTablePropertiesHandler;
import com.dremio.exec.planner.sql.handlers.direct.AlterTableSetOptionHandler;
import com.dremio.exec.planner.sql.handlers.direct.AnalyzeTableStatisticsHandler;
import com.dremio.exec.planner.sql.handlers.direct.ChangeColumnHandler;
import com.dremio.exec.planner.sql.handlers.direct.CreateEmptyTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.CreateFunctionHandler;
import com.dremio.exec.planner.sql.handlers.direct.CreateViewHandler;
import com.dremio.exec.planner.sql.handlers.direct.DescribeFunctionHandler;
import com.dremio.exec.planner.sql.handlers.direct.DescribeTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropColumnHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropFunctionHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropViewHandler;
import com.dremio.exec.planner.sql.handlers.direct.ExplainHandler;
import com.dremio.exec.planner.sql.handlers.direct.ExplainJsonHandler;
import com.dremio.exec.planner.sql.handlers.direct.ForgetTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.RefreshSourceStatusHandler;
import com.dremio.exec.planner.sql.handlers.direct.RefreshTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.RollbackHandler;
import com.dremio.exec.planner.sql.handlers.direct.SetApproxHandler;
import com.dremio.exec.planner.sql.handlers.direct.SetOptionHandler;
import com.dremio.exec.planner.sql.handlers.direct.ShowFunctionsHandler;
import com.dremio.exec.planner.sql.handlers.direct.ShowSchemasHandler;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.dremio.exec.planner.sql.handlers.direct.SqlAlterTableToggleSchemaLearningHandler;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.direct.TruncateTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.UseSchemaHandler;
import com.dremio.exec.planner.sql.handlers.query.CreateTableHandler;
import com.dremio.exec.planner.sql.handlers.query.DeleteHandler;
import com.dremio.exec.planner.sql.handlers.query.InsertTableHandler;
import com.dremio.exec.planner.sql.handlers.query.MergeHandler;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.UpdateHandler;
import com.dremio.exec.planner.sql.parser.SqlAccelToggle;
import com.dremio.exec.planner.sql.parser.SqlAddExternalReflection;
import com.dremio.exec.planner.sql.parser.SqlAlterClearPlanCache;
import com.dremio.exec.planner.sql.parser.SqlAlterTableAddColumns;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumn;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumnSetOption;
import com.dremio.exec.planner.sql.parser.SqlAlterTableDropColumn;
import com.dremio.exec.planner.sql.parser.SqlAlterTablePartitionColumns;
import com.dremio.exec.planner.sql.parser.SqlAlterTableProperties;
import com.dremio.exec.planner.sql.parser.SqlAlterTableSetOption;
import com.dremio.exec.planner.sql.parser.SqlAlterTableToggleSchemaLearning;
import com.dremio.exec.planner.sql.parser.SqlAnalyzeTableStatistics;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.planner.sql.parser.SqlCreateFolder;
import com.dremio.exec.planner.sql.parser.SqlCreateFunction;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection;
import com.dremio.exec.planner.sql.parser.SqlCreateTable;
import com.dremio.exec.planner.sql.parser.SqlDescribeFunction;
import com.dremio.exec.planner.sql.parser.SqlDropFunction;
import com.dremio.exec.planner.sql.parser.SqlDropReflection;
import com.dremio.exec.planner.sql.parser.SqlExplainJson;
import com.dremio.exec.planner.sql.parser.SqlForgetTable;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlRefreshSourceStatus;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.planner.sql.parser.SqlSetApprox;
import com.dremio.exec.planner.sql.parser.SqlShowFunctions;
import com.dremio.exec.planner.sql.parser.SqlShowSchemas;
import com.dremio.exec.planner.sql.parser.SqlTruncateTable;
import com.dremio.exec.planner.sql.parser.SqlUseSchema;
import com.dremio.exec.planner.sql.parser.SqlVersionBase;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementArrowReq;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.protobuf.InvalidProtocolBufferException;

import io.opentelemetry.api.trace.Span;

/**
 * Takes a request and creates the appropriate type of command.
 */
public class CommandCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommandCreator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(CommandCreator.class);

  private static final String QUERY_ID_SPAN_ATTRIBUTE_NAME = "dremio.query.id";
  private static final String QUERY_KIND_SPAN_ATTRIBUTE_NAME = "dremio.query.kind";


  private final QueryContext context;
  private final UserRequest request;
  private final AttemptObserver observer;
  private final SabotContext dbContext;
  private final Cache<Long, PreparedPlan> preparedPlans;
  private final int attemptNumber;
  private final Pointer<QueryId> prepareId;

  public CommandCreator(
    SabotContext dbContext,
    QueryContext context,
    UserRequest request,
    AttemptObserver observer,
    Cache<Long, PreparedPlan> preparedPlans,
    Pointer<QueryId> prepareId,
    int attemptNumber) {
    this.context = context;
    this.request = request;
    this.observer = observer;
    this.dbContext = dbContext;
    this.preparedPlans = preparedPlans;
    this.prepareId = prepareId;
    this.attemptNumber = attemptNumber;
  }

  private static MetadataCommandParameters getParameters(UserSession userSession, QueryId queryId) {
    return new ImmutableMetadataCommandParameters.Builder()
      .setCatalogName(userSession.getCatalogName())
      .setUsername(userSession.getCredentials().getUserName())
      .setMaxMetadataCount(userSession.getMaxMetadataCount())
      .setQueryId(queryId)
      .build();
  }

  private void applyQueryLabel(SqlNode sqlNode) {
    // Only support COPYINTO/OPTIMIZE for now
    if (sqlNode instanceof SqlCopyIntoTable) {
      context.getSession().setQueryLabel("COPY");
    } else if (sqlNode instanceof SqlOptimize) {
      context.getSession().setQueryLabel("OPTIMIZATION");
    } else {
      // set the default Query Label to None
      context.getSession().setQueryLabel("NONE");
    }
  }

  public CommandRunner<?> toCommand() throws ForemanException {
      injector.injectChecked(context.getExecutionControls(), "run-try-beginning", ForemanException.class);
      switch(request.getType()){
      case GET_CATALOGS:
        return new MetadataProvider.CatalogsProvider(dbContext.getConduitInProcessChannelProviderProvider(),
          getParameters(context.getSession(), context.getQueryId()), request.unwrap(GetCatalogsReq.class));

      case GET_SCHEMAS:
        return new MetadataProvider.SchemasProvider(dbContext.getConduitInProcessChannelProviderProvider(),
          getParameters(context.getSession(), context.getQueryId()), request.unwrap(GetSchemasReq.class));

      case GET_TABLES:
        return new MetadataProvider.TablesProvider(dbContext.getConduitInProcessChannelProviderProvider(),
          getParameters(context.getSession(), context.getQueryId()), request.unwrap(GetTablesReq.class));

      case GET_COLUMNS:
        return new MetadataProvider.ColumnsProvider(dbContext.getConduitInProcessChannelProviderProvider(),
          getParameters(context.getSession(), context.getQueryId()), dbContext.getCatalogService(),
          request.unwrap(GetColumnsReq.class));

      case CREATE_PREPARED_STATEMENT: {
        final CreatePreparedStatementReq req = request.unwrap(CreatePreparedStatementReq.class);
        return getSqlCommand(req.getSqlQuery(), PrepareMetadataType.USER_RPC);
      }

      case CREATE_PREPARED_STATEMENT_ARROW: {
        final CreatePreparedStatementArrowReq req = request.unwrap(CreatePreparedStatementArrowReq.class);
        return getSqlCommand(req.getSqlQuery(), PrepareMetadataType.ARROW);
      }

      case GET_SERVER_META:
        return new ServerMetaProvider.ServerMetaCommandRunner(context.getQueryId(), context.getSession(), dbContext,
          request.unwrap(GetServerMetaReq.class));

      case RUN_QUERY:
        final RunQuery query = request.unwrap(RunQuery.class);

        switch (query.getType()) {
        case PREPARED_STATEMENT:
          try {
            final ServerPreparedStatementState preparedStatement =
              ServerPreparedStatementState.PARSER.parseFrom(query.getPreparedStatementHandle().getServerInfo());
            if(preparedStatement.hasPrepareId()){
              prepareId.value = preparedStatement.getPrepareId();
            }
            // we'll try to use the cached prepared statement iff this is the first
            // attempt to run the query. Otherwise, we might get stale information
            // for schema on a schema change.
            if(attemptNumber == 0){

              final long handle = preparedStatement.getHandle();
              PreparedPlan plan = preparedPlans.getIfPresent(handle);
              if(plan != null){
                if (!context.getOptions().getOption(REUSE_PREPARE_HANDLES)) {
                  preparedPlans.invalidate(handle);
                }

                // Only reuse the cached prepared plan if the query and username match
                final boolean queryMatches = plan.getQuery().equals(preparedStatement.getSqlQuery());
                final boolean usernameMatches = plan.getUsername() == null || plan.getUsername().equals(context.getSession().getCredentials().getUserName());
                if (queryMatches && usernameMatches) {
                  context.setQueryRequiresGroupsInfo(plan.getQueryRequiresGroupsInfo());
                  return new PrepareToExecution(plan, observer);
                }
              }
            }

            return getSqlCommand(preparedStatement.getSqlQuery(), PrepareMetadataType.NONE);

          } catch (InvalidProtocolBufferException e){
            throw UserException.connectionError(e)
              .message("Unable to parse provided prepared statement. " +
                "It is likely that the client failed to handle this message.")
              .build(logger);
          }

        case SQL:
          return getSqlCommand(query.getPlan(), PrepareMetadataType.NONE);

        case PHYSICAL: // should be deprecated once tests are removed.
          return new PhysicalPlanCommand(dbContext.getPlanReader(), query.getPlanBytes());

        default:
          throw new IllegalArgumentException(
              String.format("Unknown query type [%s] received", query.getType()));
        }
      default:
        throw new UnsupportedOperationException("Unsupported type.");

      }
  }
  /**
   * Validate the command before execution
   *
   * Execute an optional validation step before converting the
   * parsed SQL tree into a {@code CommandRunner} instance
   *
   * @param sqlNode
   */
  protected void validateCommand(SqlNode sqlNode) throws ForemanException {
  }

  protected ReflectionContext getReflectionContext() {
    return ReflectionContext.SYSTEM_USER_CONTEXT;
  }

  @SuppressWarnings("FallThrough") // FIXME: remove suppression by properly handling switch fallthrough
  @VisibleForTesting
  CommandRunner<?> getSqlCommand(String sql, PrepareMetadataType prepareMetadataType) {
    try{
      final SqlConverter parser = new SqlConverter(
          context.getPlannerSettings(),
          context.getOperatorTable(),
          context,
          context.getMaterializationProvider(),
          context.getFunctionRegistry(),
          context.getSession(),
          observer,
          context.getCatalog(),
          context.getSubstitutionProviderFactory(),
          context.getConfig(),
          context.getScanResult(),
          context.getRelMetadataQuerySupplier());

      injector.injectChecked(context.getExecutionControls(), "sql-parsing", ForemanSetupException.class);
      final Catalog catalog = context.getCatalog();
      final SqlNode sqlNode = parser.parse(sql);
      final SqlHandlerConfig config = new SqlHandlerConfig(context, parser, observer, parser.getMaterializations());

      Span.current().setAttribute(QUERY_ID_SPAN_ATTRIBUTE_NAME, QueryIdHelper.getQueryId(context.getQueryId()));
      Span.current().setAttribute(QUERY_KIND_SPAN_ATTRIBUTE_NAME, SqlNodeUtil.getQueryKind(sqlNode));

      final DirectBuilder direct = new DirectBuilder(sql, sqlNode, prepareMetadataType);
      final AsyncBuilder async = new AsyncBuilder(sql, sqlNode, prepareMetadataType);

      validateCommand(sqlNode);
      if (context.getOptions().getOption(ExecConstants.ENABLE_QUERY_LABEL)) {
        applyQueryLabel(sqlNode);
      }

      //TODO DX-10976 refactor all handlers to use similar Creator interfaces
      if(sqlNode instanceof SqlToPlanHandler.Creator) {
        SqlToPlanHandler.Creator creator = (SqlToPlanHandler.Creator) sqlNode;
        return async.create(creator.toPlanHandler(), config);
      } else if (sqlNode instanceof SimpleDirectHandler.Creator) {
        SimpleDirectHandler.Creator creator = (SimpleDirectHandler.Creator) sqlNode;
        return direct.create(creator.toDirectHandler(context));
      }

      switch (sqlNode.getKind()) {
      case EXPLAIN:
        return direct.create(new ExplainHandler(config));

      case SET_OPTION:
        if (sqlNode instanceof SqlAlterTableSetOption) {
          return direct.create(new AlterTableSetOptionHandler(catalog));
        } else if (sqlNode instanceof SqlAlterTableChangeColumnSetOption) {
          return direct.create(new AlterTableChangeColumnSetOptionHandler(catalog));
        } else if (sqlNode instanceof SqlSetOption) {
          checkIfAllowedToSetOption(sqlNode);
          return direct.create(new SetOptionHandler(context));
        }

      case DESCRIBE_TABLE:
        SqlValidatorAndToRelContext sqlValidatorAndToRelContext = SqlValidatorAndToRelContext.builder(parser).build();
        final DremioCatalogReader reader = sqlValidatorAndToRelContext.getDremioCatalogReader();
        return direct.create(DescribeTableHandler.create(reader, context));

      case CREATE_VIEW:
        return direct.create(CreateViewHandler.create(config));

      case DROP_TABLE:
        return direct.create(new DropTableHandler(catalog, context.getSession()));

      case ROLLBACK:
        return direct.create(new RollbackHandler(catalog, config));

      case DROP_VIEW:
        return direct.create(new DropViewHandler(config));

      case CREATE_TABLE:
        NamespaceKey tableKey = catalog.resolveSingle(((SqlCreateTable) sqlNode).getPath());
        DremioTable table = catalog.getTableNoResolve(tableKey);
        if (table != null && ((SqlCreateTable) sqlNode).getIfNotExists()) {
            return direct.create(CreateEmptyTableHandler.create(catalog, config, context.getSession(), ((SqlCreateTable) sqlNode).getIfNotExists()));
        }
        return async.create(CreateTableHandler.create(), config);

      case ALTER_TABLE:
        if (sqlNode instanceof SqlAlterTableAddColumns) {
          return direct.create(new AddColumnsHandler(catalog, config));
        } else if (sqlNode instanceof SqlAlterTableChangeColumn) {
          return direct.create(ChangeColumnHandler.create(catalog, config));
        } else if (sqlNode instanceof SqlAlterTableDropColumn) {
          return direct.create(DropColumnHandler.create(catalog, config));
        } else if (sqlNode instanceof SqlAlterTableToggleSchemaLearning) {
          return direct.create(new SqlAlterTableToggleSchemaLearningHandler(catalog, config));
        } else if (sqlNode instanceof SqlAlterTablePartitionColumns) {
          return direct.create(new AlterTablePartitionSpecHandler(catalog, config));
        } else if (sqlNode instanceof SqlAlterTableProperties) {
          return direct.create(new AlterTablePropertiesHandler(catalog, config));
        }

      case INSERT:
        return async.create(new InsertTableHandler(), config);

      case DELETE:
        return async.create(new DeleteHandler(), config);

      case MERGE:
        return async.create(new MergeHandler(), config);

      case UPDATE:
        return async.create(new UpdateHandler(), config);

      case OTHER:
      case OTHER_DDL:
        if (sqlNode instanceof SqlShowSchemas) {
          return direct.create(new ShowSchemasHandler(catalog));
        } else if (sqlNode instanceof SqlExplainJson) {
          return direct.create(new ExplainJsonHandler(config));
        } else if (sqlNode instanceof SqlUseSchema) {
          return direct.create(new UseSchemaHandler(context.getSession(), catalog));
        } else if (sqlNode instanceof SqlCreateReflection) {
          return direct.create(new AccelCreateReflectionHandler(catalog, context, getReflectionContext()));
        } else if (sqlNode instanceof SqlAddExternalReflection) {
          return direct.create(new AccelAddExternalReflectionHandler(catalog, context, getReflectionContext()));
        } else if (sqlNode instanceof SqlAccelToggle) {
          return direct.create(new AccelToggleHandler(catalog, context, getReflectionContext()));
        } else if (sqlNode instanceof SqlDropReflection) {
          return direct.create(new AccelDropReflectionHandler(catalog, context, getReflectionContext()));
        } else if (sqlNode instanceof SqlForgetTable) {
          return direct.create(new ForgetTableHandler(catalog));
        } else if (sqlNode instanceof SqlRefreshTable) {
          return direct.create(new RefreshTableHandler(catalog,
                  context.getNamespaceService(context.getQueryUserName()),
                  context.getOptions().getOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT),
                  context.getQueryUserName()));
        } else if (sqlNode instanceof SqlRefreshSourceStatus) {
          return direct.create(new RefreshSourceStatusHandler(catalog));
        } else if (sqlNode instanceof SqlSetApprox) {
          return direct.create(new SetApproxHandler(catalog));
        } else if (sqlNode instanceof SqlCreateEmptyTable) {
          return direct.create(CreateEmptyTableHandler.create(catalog, config, context.getSession(), ((SqlCreateEmptyTable) sqlNode).getIfNotExists()));
        } else if (sqlNode instanceof SqlTruncateTable) {
          return direct.create(new TruncateTableHandler(config));
        } else if (sqlNode instanceof SqlAlterClearPlanCache) {
          return direct.create(new AlterClearPlanCacheHandler(context));
        } else if (sqlNode instanceof SqlAnalyzeTableStatistics) {
          return direct.create(new AnalyzeTableStatisticsHandler(catalog, config, context.getStatisticsAdministrationFactory()));
        } else if (sqlNode instanceof SqlVersionBase) {
          return direct.create(((SqlVersionBase) sqlNode).toDirectHandler(context));
        } else if (sqlNode instanceof SqlCreateFunction) {
          return direct.create(new CreateFunctionHandler(context));
        } else if (sqlNode instanceof SqlDropFunction) {
          return direct.create(new DropFunctionHandler(context));
        } else if(sqlNode instanceof SqlDescribeFunction){
          return direct.create(DescribeFunctionHandler.create(context));
        } else if (sqlNode instanceof SqlShowFunctions){
          return direct.create(new ShowFunctionsHandler(context));
        } else if (sqlNode instanceof SqlCopyIntoTable) {
          return async.create(new InsertTableHandler(), config);
        } else if (sqlNode instanceof SqlCreateFolder) {
          return direct.create(((SqlCreateFolder) sqlNode).toDirectHandler(context));
        }

        // fallthrough
      default:
        return async.create(new NormalHandler(), config);
      }

    } catch (UserException userException) {
      throw userException;
    } catch(SqlUnsupportedException e) {
      throw UserException.unsupportedError(e)
          .addContext(SqlExceptionHelper.SQL_QUERY_CONTEXT, sql)
          .build(logger);
    } catch (final Throwable ex) {
      throw UserException.planError(ex)
          .addContext(SqlExceptionHelper.SQL_QUERY_CONTEXT, sql)
          .build(logger);
    }
  }

  protected void checkIfAllowedToSetOption(SqlNode sqlNode) throws ForemanSetupException {
    // no-op. This is overridden in derived classes
  }

  private class DirectBuilder {
    private final String sql;
    private final SqlNode sqlNode;
    private final PrepareMetadataType prepareMetadataType;
    private final boolean storeResults;

    DirectBuilder(String sql, SqlNode sqlNode, PrepareMetadataType prepareMetadataType) {
      this.sqlNode = sqlNode;
      this.prepareMetadataType = prepareMetadataType;
      this.sql = sql;

      final StoreQueryResultsPolicy storeQueryResultsPolicy = Optional
          .ofNullable(context.getOptions().getOption(STORE_QUERY_RESULTS.getOptionName()))
          .map(o -> StoreQueryResultsPolicy.valueOf(o.getStringVal().toUpperCase(Locale.ROOT)))
          .orElse(StoreQueryResultsPolicy.NO);
      this.storeResults = storeQueryResultsPolicy != StoreQueryResultsPolicy.NO;
    }

    // handlers in handlers.direct package
    public CommandRunner<?> create(SqlDirectHandler<?> handler) {
      switch (prepareMetadataType) {
        case USER_RPC:
          return new HandlerToPrepareDirect(sql, context, handler);
        case ARROW:
          return new HandlerToPrepareArrowDirect(sql, context, handler);
        case NONE:
        default: {
          if (storeResults) {
            return new DirectWriterCommand<>(sql, context, sqlNode, handler, observer);
          }
          return new DirectCommand<>(sql, context, sqlNode, handler, observer);
        }
      }
    }
  }

  private class AsyncBuilder {
    private final SqlNode sqlNode;
    private final String sql;
    private final PrepareMetadataType prepareMetadataType;

    AsyncBuilder(String sql, SqlNode sqlNode, PrepareMetadataType prepareMetadataType) {
      this.sqlNode = sqlNode;
      this.sql = sql;
      this.prepareMetadataType = prepareMetadataType;
    }

    // handlers in handlers.query package
    public CommandRunner<?> create(SqlToPlanHandler handler, SqlHandlerConfig config){
      switch (prepareMetadataType) {
        case USER_RPC:
          return new HandlerToPreparePlan(context, sqlNode, handler, preparedPlans, sql, observer, config);
        case ARROW:
          return new HandlerToPrepareArrowPlan(context, sqlNode, handler, preparedPlans, sql, observer, config);
        case NONE:
        default:
          return new HandlerToExec(observer, sql, sqlNode, handler, config);
      }
    }
  }

  /**
   * Helper values to distinguish between handlers needing to provide no plans, plans with
   * USER_RPC metadata, or Arrow metadata.
   */
  @VisibleForTesting
  enum PrepareMetadataType {
    NONE,
    USER_RPC,
    ARROW
  }
}
