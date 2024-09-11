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
package com.dremio.exec.store.easy.triggerpipe;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.easy.EasyScanTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import java.util.List;

/**
 * This class extends {@link EasyScanTableFunction} and is responsible for scanning tables for
 * trigger pipe queries.
 */
public class TriggerPipeEasyScanTableFunction extends EasyScanTableFunction {
  private final TriggerPipeScanUtils scanUtils;

  /**
   * Constructs a new {@code TriggerPipeEasyScanTableFunction} instance.
   *
   * @param fec The fragment execution context.
   * @param context The operator context.
   * @param props The operator properties.
   * @param functionConfig The table function configuration.
   */
  public TriggerPipeEasyScanTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);
    scanUtils = new TriggerPipeScanUtils();
  }

  /** Initializes incoming vectors for trigger pipe functionality. */
  @Override
  protected void initializeIncomingVectors() {
    super.initializeIncomingVectors();
    scanUtils.initializeTriggerPipeIncomingVectors(incoming);
  }

  @Override
  protected SplitAndPartitionInfo getSplitAndPartitionInfo(int idx) {
    SplitAndPartitionInfo splitAndPartitionInfo = super.getSplitAndPartitionInfo(idx);
    scanUtils.addSplitIngestionProperties(idx);
    return splitAndPartitionInfo;
  }

  /**
   * Adds splits and ingestion properties for trigger pipe functionality.
   *
   * @param splits The splits to add.
   */
  @Override
  protected void addSplits(List<SplitAndPartitionInfo> splits) {
    ((TriggerPipeEasySplitReaderCreatorIterator) splitReaderCreatorIterator)
        .addSplitsIngestionProperties(scanUtils.getSplitsIngestionProperties());
    super.addSplits(splits);
  }

  /**
   * Sets the split reader creator iterator for trigger pipe functionality.
   *
   * @throws ExecutionSetupException If an error occurs during setup.
   */
  @Override
  protected void setSplitReaderCreatorIterator() throws ExecutionSetupException {
    splitReaderCreatorIterator =
        new TriggerPipeEasySplitReaderCreatorIterator(fec, context, props, functionConfig, false);
  }
}
