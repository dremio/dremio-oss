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
import com.dremio.exec.physical.config.copyinto.IngestionProperties;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.easy.EasySplitReaderCreatorIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;

/**
 * This class extends {@link EasySplitReaderCreatorIterator} and is responsible for creating split
 * readers for trigger pipe scans.
 */
public class TriggerPipeEasySplitReaderCreatorIterator extends EasySplitReaderCreatorIterator {

  private List<IngestionProperties> splitsIngestionProperties;
  private Iterator<IngestionProperties> ingestionPropertiesIterator;
  private IngestionProperties currIngestionProperties;

  /**
   * Constructs a new {@code TriggerPipeEasySplitReaderCreatorIterator} instance.
   *
   * @param fragmentExecContext The fragment execution context.
   * @param context The operator context.
   * @param props The operator properties.
   * @param config The table function configuration.
   * @param produceFromBufferedSplits Flag indicating whether to produce from buffered splits.
   * @throws ExecutionSetupException If an error occurs during setup.
   */
  public TriggerPipeEasySplitReaderCreatorIterator(
      FragmentExecutionContext fragmentExecContext,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig config,
      boolean produceFromBufferedSplits)
      throws ExecutionSetupException {
    super(fragmentExecContext, context, props, config, produceFromBufferedSplits);
  }

  /**
   * Adds ingestion properties for splits to this iterator.
   *
   * @param splitsIngestionProperties The list of ingestion properties for splits.
   */
  public void addSplitsIngestionProperties(List<IngestionProperties> splitsIngestionProperties) {
    this.splitsIngestionProperties = splitsIngestionProperties;
  }

  /** Processes splits and initializes ingestion properties iterator. */
  @Override
  protected void processSplits() {
    super.processSplits();
    if (splitsIngestionProperties == null) {
      return;
    }
    ingestionPropertiesIterator = splitsIngestionProperties.iterator();
    currIngestionProperties =
        ingestionPropertiesIterator.hasNext() ? ingestionPropertiesIterator.next() : null;
  }

  /**
   * Creates a split reader creator for trigger pipe scans.
   *
   * @return The created split reader creator.
   */
  @Override
  protected SplitReaderCreator createSplitReaderCreator() {
    SplitReaderCreator creator =
        new TriggerPipeEasySplitReaderCreator(
            context,
            fs,
            currentSplitInfo,
            currIngestionProperties,
            tablePath,
            columns,
            formatPlugin,
            ((FileSystemPlugin<?>) this.plugin).getCompressionCodecFactory(),
            extendedFormatOptions,
            extendedProperty);
    if (splitAndPartitionInfoIterator.hasNext() && ingestionPropertiesIterator.hasNext()) {
      currentSplitInfo = splitAndPartitionInfoIterator.next();
      currIngestionProperties = ingestionPropertiesIterator.next();
    } else {
      Preconditions.checkArgument(!rowGroupSplitIterator.hasNext());
      currentSplitInfo = null;
      currIngestionProperties = null;
    }
    return creator;
  }
}
