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
package com.dremio.exec.planner.physical.visitor;

import static com.dremio.exec.planner.physical.PlannerSettings.QUERY_RESULTS_STORE_TABLE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.text.StrTokenizer;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableMap;

public class WriterPathUpdater extends BasePrelVisitor<Prel, CreateTableEntry, RuntimeException> {
  private final SqlHandlerConfig sqlConfig;

  private WriterPathUpdater(SqlHandlerConfig config){
    sqlConfig = config;
  }

  public static Prel update(Prel prel, SqlHandlerConfig config) {
    return prel.accept(new WriterPathUpdater(config), null);
  }

  @Override
  public Prel visitWriterCommitter(WriterCommitterPrel prel, CreateTableEntry tableEntry) throws RuntimeException {
    final SqlParser.Config config = sqlConfig.getConverter().getParserConfig();
    final QueryContext context = sqlConfig.getContext();
    final OptionManager options = context.getOptions();
    final String storeTablePath = options.getOption(QUERY_RESULTS_STORE_TABLE.getOptionName()).getStringVal();
    final List<String> storeTable =
      new StrTokenizer(storeTablePath, '.', config.quoting().string.charAt(0))
        .setIgnoreEmptyTokens(true)
        .getTokenList();
    // QueryId is same as attempt id. Using its string form for the table name
    storeTable.add(QueryIdHelper.getQueryId(context.getQueryId()));
    // Query results are stored in arrow format. If need arises, we can change this to a configuration option.
    final Map<String, Object> storageOptions = ImmutableMap.<String, Object>of("type", ArrowFormatPlugin.ARROW_DEFAULT_NAME);

    WriterOptions writerOptions = WriterOptions.DEFAULT;
    if (options.getOption(PlannerSettings.ENABLE_OUTPUT_LIMITS)) {
      writerOptions = WriterOptions.DEFAULT
        .withOutputLimitEnabled(options.getOption(PlannerSettings.ENABLE_OUTPUT_LIMITS))
        .withOutputLimitSize(options.getOption(PlannerSettings.OUTPUT_LIMIT_SIZE));
    }

    // store table as system user.
    final CreateTableEntry createTableEntry = context.getCatalog()
      .resolveCatalog(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
      .createNewTable(new NamespaceKey(storeTable), null, writerOptions, storageOptions, true);

    WriterCommitterPrel committerPrel = new WriterCommitterPrel(prel.getCluster(),
      prel.getTraitSet(), ((Prel) prel.getInput(0)).accept(this, createTableEntry), createTableEntry.getPlugin(),
      null, createTableEntry.getLocation(), createTableEntry.getUserName(), createTableEntry, Optional.empty(), prel.isPartialRefresh(), prel.isReadSignatureEnabled());
    return committerPrel;
  }

  @Override
  public Prel visitPrel(Prel prel, CreateTableEntry tableEntry) throws RuntimeException {

    List<RelNode> newInputs = new ArrayList<>();
    for (Prel input : prel) {
      newInputs.add(input.accept(this, tableEntry));
    }

    return (Prel) prel.copy(prel.getTraitSet(), newInputs);
  }

  @Override
  public Prel visitWriter(WriterPrel prel, CreateTableEntry tableEntry) throws RuntimeException {
    return new WriterPrel(prel.getCluster(), prel.getTraitSet(), prel.getInput(), tableEntry, prel.getExpectedInboundRowType());
  }
}
