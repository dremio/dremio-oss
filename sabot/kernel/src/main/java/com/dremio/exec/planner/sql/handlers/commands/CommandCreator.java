/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.exec.planner.physical.PlannerSettings.STORE_QUERY_RESULTS;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.AccelAddLayoutHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelDisableHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelDropLayoutHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelEnableHandler;
import com.dremio.exec.planner.sql.handlers.direct.AccelToggleHandler;
import com.dremio.exec.planner.sql.handlers.direct.CreateViewHandler;
import com.dremio.exec.planner.sql.handlers.direct.DescribeTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.DropViewHandler;
import com.dremio.exec.planner.sql.handlers.direct.ExplainHandler;
import com.dremio.exec.planner.sql.handlers.direct.ForgetTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.RefreshMetadataHandler;
import com.dremio.exec.planner.sql.handlers.direct.RefreshTableHandler;
import com.dremio.exec.planner.sql.handlers.direct.SetOptionHandler;
import com.dremio.exec.planner.sql.handlers.direct.ShowFileHandler;
import com.dremio.exec.planner.sql.handlers.direct.ShowSchemasHandler;
import com.dremio.exec.planner.sql.handlers.direct.ShowTablesHandler;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.planner.sql.handlers.direct.UseSchemaHandler;
import com.dremio.exec.planner.sql.handlers.query.CreateTableHandler;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.parser.SqlAccelDisable;
import com.dremio.exec.planner.sql.parser.SqlAccelEnable;
import com.dremio.exec.planner.sql.parser.SqlAccelToggle;
import com.dremio.exec.planner.sql.parser.SqlAddLayout;
import com.dremio.exec.planner.sql.parser.SqlDropLayout;
import com.dremio.exec.planner.sql.parser.SqlForgetTable;
import com.dremio.exec.planner.sql.parser.SqlRefreshMetadata;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.planner.sql.parser.SqlShowFiles;
import com.dremio.exec.planner.sql.parser.SqlShowSchemas;
import com.dremio.exec.planner.sql.parser.SqlShowTables;
import com.dremio.exec.planner.sql.parser.SqlUseSchema;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.service.Pointer;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Takes a request and creates the appropriate type of command.
 */
public class CommandCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommandCreator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(CommandCreator.class);

  private final QueryContext context;
  private final CoordToExecTunnelCreator tunnelCreator;
  private final UserRequest request;
  private final AttemptObserver observer;
  private final SabotContext dbContext;
  private final Cache<Long, PreparedPlan> plans;
  private final int attemptNumber;
  private final Pointer<QueryId> prepareId;

  public CommandCreator(
      SabotContext dbContext,
      QueryContext context,
      CoordToExecTunnelCreator tunnelCreator,
      UserRequest request,
      AttemptObserver observer,
      Cache<Long, PreparedPlan> plans,
      Pointer<QueryId> prepareId,
      int attemptNumber) {
    this.context = context;
    this.tunnelCreator = tunnelCreator;
    this.request = request;
    this.observer = observer;
    this.dbContext = dbContext;
    this.plans = plans;
    this.prepareId = prepareId;
    this.attemptNumber = attemptNumber;
  }

  public CommandRunner<?> toCommand() throws ForemanException {
      injector.injectChecked(context.getExecutionControls(), "run-try-beginning", ForemanException.class);
      switch(request.getType()){
      case GET_CATALOGS:
        return new MetadataProvider.CatalogsProvider(context.getQueryId(), context.getSession(), dbContext,
          request.unwrap(GetCatalogsReq.class));

      case GET_COLUMNS:
        return new MetadataProvider.ColumnsProvider(context.getQueryId(), context.getSession(), dbContext,
          request.unwrap(GetColumnsReq.class));

      case GET_SCHEMAS:
        return new MetadataProvider.SchemasProvider(context.getQueryId(), context.getSession(), dbContext,
          request.unwrap(GetSchemasReq.class));

      case GET_TABLES:
        return new MetadataProvider.TablesProvider(context.getQueryId(), context.getSession(), dbContext,
          request.unwrap(GetTablesReq.class));

      case CREATE_PREPARED_STATEMENT: {
        final CreatePreparedStatementReq req = request.unwrap(CreatePreparedStatementReq.class);
        return getSqlCommand(req.getSqlQuery(), true);
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

              PreparedPlan plan = plans.getIfPresent(preparedStatement.getHandle());
              if(plan != null){
                // we need to verify that client provided valid handle.
                Preconditions.checkArgument(plan.getQuery().equals(preparedStatement.getSqlQuery()),
                  "Prepared statement's sql query doesn't match what sent when prepared statement was created.");
                if(plan.getUsername() != null){
                  Preconditions.checkArgument(
                    plan.getUsername()
                        .equals(context.getSession()
                            .getCredentials()
                            .getUserName()));
                }
                return new PrepareToExecution(plan, context, observer, dbContext.getPlanReader(), tunnelCreator);
              }
            }

            return getSqlCommand(preparedStatement.getSqlQuery(), false);

          } catch (InvalidProtocolBufferException e){
            throw UserException.connectionError(e)
              .message("Unable to parse provided prepared statement. " +
                "It is likely that the client failed to handle this message.")
              .build(logger);
          }

        case SQL:
          return getSqlCommand(query.getPlan(), false);

        case PHYSICAL: // should be deprecated once tests are removed.
          return new PhysicalPlanCommand(tunnelCreator, context, dbContext.getPlanReader(), observer, query.getPlan());

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

  private CommandRunner<?> getSqlCommand(String sql, boolean isPrepare) {
    try{
      final SqlConverter parser = new SqlConverter(context.getPlannerSettings(), context.getNewDefaultSchema(),
          context.getOperatorTable(), context, context.getMaterializationProvider(), context.getFunctionRegistry(),
          context.getSession(), observer, context.getStorage(), context.getSubstitutionProviderFactory());

      injector.injectChecked(context.getExecutionControls(), "sql-parsing", ForemanSetupException.class);

      final SqlNode sqlNode = parser.parse(sql);
      final SqlHandlerConfig config = new SqlHandlerConfig(context, parser, observer, parser.getMaterializations());
      final boolean systemUser = SystemUser.SYSTEM_USERNAME.equals(context.getSession().getCredentials().getUserName());

      validateCommand(sqlNode);

      final DirectBuilder direct = new DirectBuilder(sql, sqlNode, isPrepare);
      final AsyncBuilder async = new AsyncBuilder(sql, sqlNode, isPrepare);

      switch (sqlNode.getKind()) {
      case EXPLAIN:
        return direct.create(new ExplainHandler(config));

      case SET_OPTION:
        return direct.create(new SetOptionHandler(context.getOptions()));

      case DESCRIBE_TABLE:
        return direct.create(new DescribeTableHandler(context.getNewDefaultSchema()));


      case CREATE_VIEW:
        return direct.create(new CreateViewHandler(config, systemUser));

      case DROP_TABLE:
        return direct.create(new DropTableHandler(context.getNewDefaultSchema(), systemUser));

      case DROP_VIEW:
        return direct.create(new DropViewHandler(context.getNewDefaultSchema(), systemUser));

      case CREATE_TABLE:
        return async.create(new CreateTableHandler(systemUser), config);

      case OTHER:
      case OTHER_DDL:
        if (sqlNode instanceof SqlRefreshMetadata) {
          return direct.create(new RefreshMetadataHandler(context.getNewDefaultSchema()));
        } else if (sqlNode instanceof SqlShowFiles) {
          return direct.create(new ShowFileHandler(context.getNewDefaultSchema()));
        } else if (sqlNode instanceof SqlShowSchemas) {
          return direct.create(new ShowSchemasHandler(context.getRootSchema(true)));
        } else if (sqlNode instanceof SqlShowTables) {
          return direct.create(new ShowTablesHandler(context.getNewDefaultSchema()));
        } else if (sqlNode instanceof SqlUseSchema) {
          return direct.create(new UseSchemaHandler(context.getSession(), context.getNewDefaultSchema()));
        } else if (sqlNode instanceof SqlAccelEnable) {
          return direct.create(new AccelEnableHandler(context.getNewDefaultSchema(), context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlAccelDisable) {
          return direct.create(new AccelDisableHandler(context.getNewDefaultSchema(), context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlAddLayout) {
          return direct.create(new AccelAddLayoutHandler(context.getNewDefaultSchema(), context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlAccelToggle) {
          return direct.create(new AccelToggleHandler(context.getNewDefaultSchema(), context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlDropLayout) {
          return direct.create(new AccelDropLayoutHandler(context.getNewDefaultSchema(), context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlForgetTable) {
          return direct.create(new ForgetTableHandler(context.getNewDefaultSchema(), context.getNamespaceService(),
            context.getAccelerationManager()));
        } else if (sqlNode instanceof SqlRefreshTable) {
          return direct.create(new RefreshTableHandler(context.getNewDefaultSchema(), context.getCatalogService()));
        }

        // fallthrough
      default:
        return async.create(new NormalHandler(), config);
      }

    } catch(SqlUnsupportedException e) {
      throw UserException.unsupportedError(e)
          .addContext("Sql Query", sql)
          .build(logger);
    } catch (final Throwable ex) {
      throw UserException.planError(ex)
          .addContext("Sql Query", sql)
          .build(logger);
    }
  }

  private class DirectBuilder {
    private final String sql;
    private final SqlNode sqlNode;
    private final boolean prepare;
    private final boolean storeResults;

    DirectBuilder(String sql, SqlNode sqlNode, boolean prepare) {
      this.sqlNode = sqlNode;
      this.prepare = prepare;
      this.sql = sql;

      OptionValue value = context.getOptions().getOption(STORE_QUERY_RESULTS.getOptionName());
      this.storeResults = value != null && value.bool_val;

    }

    // handlers in handlers.direct package
    public CommandRunner<?> create(SqlDirectHandler<?> handler){
      if(prepare) {
        return new HandlerToPrepareDirect(sql, context, handler);
      }
      if(storeResults) {
        return new DirectWriterCommand<>(sql, context, sqlNode, handler, observer);
      }
      return new DirectCommand<>(sql, context, sqlNode, handler, observer);
    }
  }

  private class AsyncBuilder {
    private final SqlNode sqlNode;
    private final String sql;
    private final boolean prepare;

    AsyncBuilder(String sql, SqlNode sqlNode, boolean prepare) {
      this.sqlNode = sqlNode;
      this.sql = sql;
      this.prepare = prepare;
    }

    // handlers in handlers.query package
    public CommandRunner<?> create(SqlToPlanHandler handler, SqlHandlerConfig config){
      if(prepare){
        return new HandlerToPreparePlan(context, sqlNode, handler, plans, sql, observer, config);
      }
      return new HandlerToExec(tunnelCreator, context, dbContext.getPlanReader(), observer, sql, sqlNode,
        handler, config);
    }
  }

}
