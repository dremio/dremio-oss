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
package com.dremio.exec.store.ischema;

import java.util.Collections;

import org.apache.calcite.schema.SchemaPlus;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaConfig.SchemaInfoProvider;
import com.dremio.exec.store.SchemaTreeProvider;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.rpc.user.UserSession;

@SuppressWarnings("unused")
public class InfoSchemaBatchCreator implements ProducerOperator.Creator<InfoSchemaSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaBatchCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fec, final OperatorContext context, InfoSchemaSubScan config) throws ExecutionSetupException {
    final ContextInformation info = context.getClassProducer().getFunctionContext().getContextInformation();
    final SchemaTreeProvider tree = new SchemaTreeProvider(config.getContext());
    SchemaConfig schemaConfig = SchemaConfig
        .newBuilder(info.getQueryUser())
        .setProvider(
            new SchemaInfoProvider() {
              private final ViewExpansionContext viewExpansionContext = new ViewExpansionContext(this, tree, info.getQueryUser());

              @Override
              public ViewExpansionContext getViewExpansionContext() {
                return viewExpansionContext;
              }

              @Override
              public OptionValue getOption(String optionKey) {
                return context.getOptions().getOption(optionKey);
              }
            }
        )
        .setIgnoreAuthErrors(true)
        .exposeSubSchemasAsTopLevelSchemas(true)
        .build();
    final SchemaPlus root = tree.getRootSchema(schemaConfig);
    RecordReader rr = config.getTable().getRecordReader(UserSession.getCatalogName(context.getOptions()), root, config.getFilter());
    return new ScanOperator(fec.getSchemaUpdater(), config, context, Collections.singleton(rr).iterator());
  }
}
