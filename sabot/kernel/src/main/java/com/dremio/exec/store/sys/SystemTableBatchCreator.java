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
package com.dremio.exec.store.sys;

import java.util.Collections;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

/**
 * This class creates batches based on the the type of {@link com.dremio.exec.store.sys.SystemTable}.
 * The distributed tables and the local tables use different record readers.
 * Local system tables do not require a full-fledged query because these records are present on every SabotNode.
 */
public class SystemTableBatchCreator implements ProducerOperator.Creator<SystemTableScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, SystemTableScan config) throws ExecutionSetupException {
    return new ScanOperator(fec.getSchemaUpdater(), config, context, Collections.singleton(config.getRecordReader(context)).iterator());
  }
}
