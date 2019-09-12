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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.store.hive.ContextClassLoaderSwapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * This class facilitates iterating through Hive {@link Partition} and
 * {@link org.apache.hadoop.mapred.InputSplit} objects to generate Dremio {@link PartitionChunk}
 * objects.
 * <p>
 * A {@link PartitionChunk} object represents:
 * - A single {@link Partition} object.
 * - Some number of associated {@link org.apache.hadoop.mapred.InputSplit} objects.
 * <p>
 * Details:
 * - {@link PartitionIterator} is used to lazily load {@link Partition} objects as required.
 * - At least one {@link PartitionChunk} will be created for every {@link Partition} which has
 * a list of {@link org.apache.hadoop.mapred.InputSplit} objects.
 * - If a {@link Partition} has "too many" {@link org.apache.hadoop.mapred.InputSplit} objects,
 * then > 1 {@link PartitionChunk} will be generated, each with the same {@link Partition} and
 * a batch of associated {@link org.apache.hadoop.mapred.InputSplit} objects.
 * - {@link InputSplitBatchIterator} is used to manage a list of
 * {@link org.apache.hadoop.mapred.InputSplit} objects as batches.
 */
public class HivePartitionChunkListing implements PartitionChunkListing {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HivePartitionChunkListing.class);

  private final boolean storageImpersonationEnabled;
  private final PartitionIterator partitions;
  private final HiveConf hiveConf;
  private final StatsEstimationParameters statsParams;
  private final MetadataAccumulator metadataAccumulator;
  private final int maxInputSplitsPerPartition;

  private final TableMetadata tableMetadata;

  protected int currentPartitionIndex = -1;
  private PartitionMetadata currentPartitionMetadata;

  private HivePartitionChunkListing(final boolean storageImpersonationEnabled, final TableMetadata tableMetadata,
                                    final HiveConf hiveConf, final StatsEstimationParameters statsParams, PartitionIterator partitions,
                                    final int maxInputSplitsPerPartition) {
    this.storageImpersonationEnabled = storageImpersonationEnabled;
    this.tableMetadata = tableMetadata;
    this.hiveConf = hiveConf;
    this.statsParams = statsParams;
    this.partitions = partitions;
    this.maxInputSplitsPerPartition = maxInputSplitsPerPartition;

    this.metadataAccumulator = new MetadataAccumulator();

    // Prime the iterator

    Partition partition = null;
    currentPartitionIndex++;

    if (null == partitions) {
      if (logger.isDebugEnabled()) {
        logger.debug("Table '{}', 1 partition exists.",
          tableMetadata.getTable().getTableName());
      }
      // Do nothing.

    } else if (partitions.hasNext()) {
      partition = partitions.next();

      if (logger.isDebugEnabled()) {
        logger.debug("Table '{}', advance to the next partition, '{}', partition chunk index: {}",
          tableMetadata.getTable().getTableName(),
          HiveMetadataUtils.getPartitionValueLogString(partition),
          currentPartitionIndex);
      }
    } else {
      // the table is partitioned, but no partitions exist.
      currentPartitionMetadata = PartitionMetadata.newBuilder().partition(null)
        .partitionId(currentPartitionIndex)
        .partitionValues(Collections.EMPTY_LIST)
        .datasetSplitBuildConf(null)
        .inputSplitBatchIterator(
          InputSplitBatchIterator.newBuilder()
            .partition(null)
            .tableMetadata(tableMetadata)
            .inputSplits(Collections.EMPTY_LIST)
            .maxInputSplitsPerPartition(maxInputSplitsPerPartition)
            .build())
        .build();
      return;
    }

    // Read Hive partition metadata.
    currentPartitionMetadata = HiveMetadataUtils.getPartitionMetadata(
      storageImpersonationEnabled, tableMetadata, metadataAccumulator, partition,
      hiveConf, currentPartitionIndex, maxInputSplitsPerPartition);
  }

  private class HivePartitionChunkIterator extends AbstractIterator<PartitionChunk> {
    @Override
    public PartitionChunk computeNext() {
      try(ContextClassLoaderSwapper ccls = ContextClassLoaderSwapper.newInstance()) {
        do {
          // Check if current hive partition does not have remaining splits.
          if (!currentPartitionMetadata.getInputSplitBatchIterator().hasNext()) {
            final Partition partition;

            if (null != partitions && partitions.hasNext()) {
              partition = partitions.next();
              currentPartitionIndex++;

              if (logger.isDebugEnabled()) {
                logger.debug("Table '{}', advance to the next partition, '{}', partition chunk index {}",
                  tableMetadata.getTable().getTableName(),
                  HiveMetadataUtils.getPartitionValueLogString(partition),
                  currentPartitionIndex);
              }
            } else {
              if (logger.isDebugEnabled()) {
                logger.debug("Table '{}', no more partitions exist.",
                  tableMetadata.getTable().getTableName());
              }
              return endOfData();
            }

            // Read Hive partition metadata.
            currentPartitionMetadata = HiveMetadataUtils.getPartitionMetadata(
              storageImpersonationEnabled, tableMetadata, metadataAccumulator, partition,
              hiveConf, currentPartitionIndex, maxInputSplitsPerPartition);
          }
          // Current partition may have no splits. Advance to the next partition.
        } while (!currentPartitionMetadata.getInputSplitBatchIterator().hasNext());

        Preconditions.checkArgument(currentPartitionMetadata.getInputSplitBatchIterator().hasNext(), "no value exists in hive input split batch iterator.");

        List<DatasetSplit> datasetSplits = HiveMetadataUtils.getDatasetSplits(tableMetadata, metadataAccumulator, currentPartitionMetadata, statsParams);
        return PartitionChunk.of(currentPartitionMetadata.getPartitionValues(), datasetSplits,
          os -> os.write(currentPartitionMetadata.getPartitionXattr().toByteArray()));
      }
    }
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public MetadataAccumulator getMetadataAccumulator() {
    return metadataAccumulator;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Iterator<PartitionChunk> iterator() {
    return new HivePartitionChunkIterator();
  }

  public static final class Builder {
    private boolean storageImpersonationEnabled;
    private TableMetadata tableMetadata;
    private PartitionIterator partitions;
    private HiveConf hiveConf;
    private StatsEstimationParameters statsParams;
    private Integer maxInputSplitsPerPartition;

    private Builder() {
    }

    public Builder storageImpersonationEnabled(boolean storageImpersonationEnabled) {
      this.storageImpersonationEnabled = storageImpersonationEnabled;
      return this;
    }

    public Builder tableMetadata(TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
      return this;
    }

    public Builder partitions(PartitionIterator partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder hiveConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    public Builder statsParams(StatsEstimationParameters statsParams) {
      this.statsParams = statsParams;
      return this;
    }

    public Builder maxInputSplitsPerPartition(Integer maxInputSplitsPerPartition) {
      this.maxInputSplitsPerPartition = maxInputSplitsPerPartition;
      return this;
    }

    public HivePartitionChunkListing build() {

      Objects.requireNonNull(tableMetadata, "table metadata is required");
      Objects.requireNonNull(hiveConf, "hive conf is required");
      Objects.requireNonNull(statsParams, "stats params is required");
      Objects.requireNonNull(maxInputSplitsPerPartition, "maxInputSplitsPerPartition is required");

      return new HivePartitionChunkListing(storageImpersonationEnabled, tableMetadata, hiveConf, statsParams, partitions, maxInputSplitsPerPartition);
    }
  }
}
