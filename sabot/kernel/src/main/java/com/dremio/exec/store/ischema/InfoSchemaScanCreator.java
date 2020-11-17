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
package com.dremio.exec.store.ischema;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

public class InfoSchemaScanCreator implements ProducerOperator.Creator<InfoSchemaSubScan>{

  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, InfoSchemaSubScan config) throws ExecutionSetupException {
    final InformationSchemaTable table = config.getTable();
    final InfoSchemaStoragePlugin plugin = fec.getStoragePlugin(config.getPluginId());
    final String catalogName =
        context.getOptions().getOption(ExecConstants.USE_LEGACY_CATALOG_NAME)
            ? InfoSchemaConstants.IS_LEGACY_CATALOG_NAME
            : InfoSchemaConstants.IS_CATALOG_NAME;
    final RecordReader reader = new InformationSchemaRecordReader(context, config.getColumns(),
        plugin.getSabotContext().getInformationSchemaServiceBlockingStub(), table,
        catalogName, config.getProps().getUserName(), config.getQuery(), context.getOptions().getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT));

    return new ScanOperator(config, context, RecordReaderIterator.from(reader));
  }

}
