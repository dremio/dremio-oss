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
package com.dremio.exec.store.hive.metadata;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.InputSplit;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Manages a list of Hive InputSplit objects to help generate Dremio PartitionChunk objects.
 * <p>
 * The list is broken up into batches of
 * {@link com.dremio.exec.store.hive.HivePluginOptions#HIVE_MAX_INPUTSPLITS_PER_PARTITION_VALIDATOR}
 * elements to be iterated over by the caller.
 */
public class InputSplitBatchIterator extends AbstractIterator<List<InputSplit>> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InputSplitBatchIterator.class);

  private final Iterator<List<InputSplit>> partitionedInputSplitsIterator;
  private final TableMetadata tableMetadata;
  private final Partition partition;
  private int batchIndexForLoggingOnly;

  private InputSplitBatchIterator(final TableMetadata tableMetadata, final Partition partition,
                                  final List<InputSplit> inputSplits, final int maxInputSplitsPerPartition) {
    this.tableMetadata = tableMetadata;
    this.partition = partition;

    List<List<InputSplit>> partitionedInputSplits =
      Lists.partition(inputSplits, maxInputSplitsPerPartition);

    if (logger.isDebugEnabled()) {
      logger.debug("Table '{}', partition '{}', {} input splits partitioned into {} batches with batch size: {}",
        tableMetadata.getTable().getTableName(),
        HiveMetadataUtils.getPartitionValueLogString(partition),
        inputSplits.size(),
        partitionedInputSplits.size(),
        maxInputSplitsPerPartition);
    }

    batchIndexForLoggingOnly = 0;
    partitionedInputSplitsIterator = partitionedInputSplits.iterator();
  }

  @Override
  public List<InputSplit> computeNext() {
    if (partitionedInputSplitsIterator.hasNext()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Table '{}', partition '{}', Advance to input split batch index: {}",
          tableMetadata.getTable().getTableName(),
          HiveMetadataUtils.getPartitionValueLogString(partition),
          batchIndexForLoggingOnly++);
      }
      return partitionedInputSplitsIterator.next();
    } else {
      if (logger.isTraceEnabled()) {
        logger.trace("Table '{}', partition '{}', No more input split batches exist.",
          tableMetadata.getTable().getTableName(),
          HiveMetadataUtils.getPartitionValueLogString(partition));
      }
      return endOfData();
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private List<InputSplit> inputSplits;
    private TableMetadata tableMetadata;
    private Partition partition;
    private Integer maxInputSplitsPerPartition;

    private Builder() {
    }

    public Builder inputSplits(List<InputSplit> inputSplits) {
      this.inputSplits = inputSplits;
      return this;
    }

    public Builder tableMetadata(TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
      return this;
    }

    public Builder partition(Partition partition) {
      this.partition = partition;
      return this;
    }

    public Builder maxInputSplitsPerPartition(Integer maxInputSplitsPerPartition) {
      this.maxInputSplitsPerPartition = maxInputSplitsPerPartition;
      return this;
    }

    public InputSplitBatchIterator build() {

      Objects.requireNonNull(tableMetadata, "table metadata is required");
      Objects.requireNonNull(inputSplits, "input splits is required");
      Objects.requireNonNull(maxInputSplitsPerPartition, "maxInputSplitsPerPartition is required");

      return new InputSplitBatchIterator(tableMetadata, partition, inputSplits, maxInputSplitsPerPartition);
    }
  }
}
