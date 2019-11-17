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
package com.dremio.exec.store.hive.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.hive.StoragePluginCreator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.spi.ProducerOperator;

/**
 * Creates operators from scans that are in a separate ClassLoader.
 */
public class HiveProxyingScanBatchCreator implements ProducerOperator.Creator<HiveProxyingSubScan> {

  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context,
                                 HiveProxyingSubScan config) throws ExecutionSetupException {
    final StoragePluginCreator.PF4JStoragePlugin plugin =
      fragmentExecContext.getStoragePlugin(config.getPluginId());

    return plugin.createScanBatchCreator().create(fragmentExecContext, context, config);
  }
}
