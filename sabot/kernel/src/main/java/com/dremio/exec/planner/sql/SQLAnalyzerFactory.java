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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.ImmutableList;

public class SQLAnalyzerFactory {

  private static final Logger logger = LoggerFactory.getLogger(SQLAnalyzerFactory.class);

  /**
   * Factory method to create the SQLAnalyzer using the appropriate implementation of SqlValidatorWithHints.
   *
   * If createForSqlSuggestions is true, construct a SqlAdvisorValidator instance,
   * otherwise construct a SqlValidatorImpl instance. Inject this into the constructor
   * for a SQLAnalyzer object.
   *
   * @param username
   * @param sabotContext
   * @param context
   * @param createForSqlSuggestions
   * @return SQLAnalyzer instance
   */
  public static SQLAnalyzer createSQLAnalyzer(final String username, final SabotContext sabotContext,
                                              final List<String> context, final boolean createForSqlSuggestions) {

    // Build dependencies required to instantiate and implementation of SqlValidatorWithHints
    UserSession session = UserSession.Builder.newBuilder()
      .withCredentials(UserBitShared.UserCredentials.newBuilder()
        .setUserName(username)
        .build())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withOptionManager(sabotContext.getOptionManager())
      .withDefaultSchema(context)
      .build();

    final ViewExpansionContext viewExpansionContext = new ViewExpansionContext(username);
    QueryOptionManager optionManager = new QueryOptionManager(session.getOptions());
    final SchemaConfig newSchemaConfig = SchemaConfig.newBuilder(username)
      .defaultSchema(session.getDefaultSchemaPath())
      .optionManager(optionManager)
      .setViewExpansionContext(viewExpansionContext)
      .exposeInternalSources(session.exposeInternalSources())
      .build();

    Catalog catalog = sabotContext.getCatalogService()
        .getCatalog(MetadataRequestOptions.of(newSchemaConfig));
    JavaTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
    DremioCatalogReader catalogReader = new DremioCatalogReader(catalog, typeFactory);

    FunctionImplementationRegistry functionImplementationRegistry = optionManager.getOption
      (PlannerSettings.ENABLE_DECIMAL_V2_KEY).getBoolVal() ? sabotContext.getDecimalFunctionImplementationRegistry()
        : sabotContext.getFunctionImplementationRegistry();
    OperatorTable opTable = new OperatorTable(functionImplementationRegistry);
    SqlOperatorTable chainedOpTable =  new ChainedSqlOperatorTable(ImmutableList.<SqlOperatorTable>of(opTable, catalogReader));

    // Create the appropriate implementation depending on intended use of the validator.
    SqlValidatorWithHints validator =
      createForSqlSuggestions ?
        new SqlAdvisorValidator(chainedOpTable, catalogReader, typeFactory, DremioSqlConformance.INSTANCE) :
        SqlValidatorUtil.newValidator(chainedOpTable, catalogReader, typeFactory, DremioSqlConformance.INSTANCE);

    return new SQLAnalyzer(validator);
  }
}
