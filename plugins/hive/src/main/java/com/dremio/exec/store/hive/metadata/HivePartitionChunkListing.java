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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.DatasetSaverImpl;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.deltalake.DeltaHiveInputFormat;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * This class facilitates iterating through Hive {@link Partition} and
 * {@link org.apache.hadoop.mapred.InputSplit} or {@link com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto}
 * objects depending on {@link SplitType} to generate Dremio {@link PartitionChunk}
 * objects.
 * <p>
 * A {@link PartitionChunk} object represents:
 * - A single {@link Partition} object.
 * - Some number of associated {@link org.apache.hadoop.mapred.InputSplit} objects or
 * - a single {@link com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto} object.
 * <p>
 * Details:
 * - {@link PartitionIterator} is used to lazily load {@link Partition} objects as required.
 * - At least one {@link PartitionChunk} will be created for every {@link Partition} which has
 * a list of {@link org.apache.hadoop.mapred.InputSplit} objects or a
 * single {@link com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto} object.
 * - For {@link SplitType#INPUT_SPLIT}, if a {@link Partition} has "too many" {@link org.apache.hadoop.mapred.InputSplit} objects,
 * then > 1 {@link PartitionChunk} will be generated, each with the same {@link Partition} and
 * - For {@link SplitType#ICEBERG_MANIFEST_SPLIT}, The partition files are not read directly,
 * it simply returns a partition chunk with the metadata json file with additional table property
 * to identify whether the table type is iceberg.
 * a batch of associated {@link org.apache.hadoop.mapred.InputSplit} objects.
 * - {@link InputSplitBatchIterator} is used to manage a list of
 * {@link org.apache.hadoop.mapred.InputSplit} objects as batches.
 */
public final class HivePartitionChunkListing implements PartitionChunkListing {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HivePartitionChunkListing.class);

  private final boolean storageImpersonationEnabled;
  private final boolean enforceVarcharWidth;
  private final PartitionIterator partitions;
  private final HiveConf hiveConf;
  private final StatsEstimationParameters statsParams;
  private final MetadataAccumulator metadataAccumulator;
  private final int maxInputSplitsPerPartition;
  private final SplitType splitType;

  private final TableMetadata tableMetadata;

  private int currentPartitionIndex = -1;
  private PartitionMetadata currentPartitionMetadata;

  private final List<DatasetSplit> deltaSplits;

  private final OptionManager optionManager;

  public enum SplitType {
    UNKNOWN,
    INPUT_SPLIT,
    DIR_LIST_INPUT_SPLIT,
    ICEBERG_MANIFEST_SPLIT,
    DELTA_COMMIT_LOGS
  }

  private HivePartitionChunkListing(final boolean storageImpersonationEnabled, final boolean enforceVarcharWidth, final TableMetadata tableMetadata,
                                    final HiveConf hiveConf, final StatsEstimationParameters statsParams, PartitionIterator partitions,
                                    final int maxInputSplitsPerPartition, final SplitType splitType, final List<DatasetSplit> deltaSplits,
                                    OptionManager optionsManager) {
    this.storageImpersonationEnabled = storageImpersonationEnabled;
    this.enforceVarcharWidth = enforceVarcharWidth;
    this.tableMetadata = tableMetadata;
    this.hiveConf = hiveConf;
    this.statsParams = statsParams;
    this.partitions = partitions;
    this.maxInputSplitsPerPartition = maxInputSplitsPerPartition;
    this.splitType = splitType;

    this.metadataAccumulator = new MetadataAccumulator();
    this.deltaSplits = deltaSplits;
    this.optionManager = optionsManager;

    // Prime the iterator

    Partition partition = null;
    currentPartitionIndex++;

    if (SplitType.ICEBERG_MANIFEST_SPLIT.equals(splitType)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Table '{}', data read from iceberg root pointer.",
          tableMetadata.getTable().getTableName());
      }
      //If it's a iceberg table only the partition xattr is needed.
      currentPartitionMetadata = PartitionMetadata.newBuilder().partition(null)
        .partitionXattr(HiveMetadataUtils.getPartitionXattr(tableMetadata.getTable(),
          HiveMetadataUtils.fromProperties(tableMetadata.getTableProperties())))
        .partitionValues(Collections.EMPTY_LIST)
        .inputSplitBatchIterator(
          InputSplitBatchIterator.newBuilder()
            .partition(null)
            .tableMetadata(tableMetadata)
            .inputSplits(Collections.EMPTY_LIST)
            .maxInputSplitsPerPartition(maxInputSplitsPerPartition)
            .build())
        .build();

      metadataAccumulator.accumulateReaderType(MapredParquetInputFormat.class);
      metadataAccumulator.setIsExactRecordCount(true);
      metadataAccumulator.accumulateTotalEstimatedRecords(tableMetadata.getRecordCount());
      metadataAccumulator.setNotAllFSBasedPartitions();
      HiveReaderProto.RootPointer rootPointer = HiveReaderProto.RootPointer.newBuilder()
        .setPath(tableMetadata.getTableProperties().getProperty(HiveMetadataUtils.METADATA_LOCATION, ""))
        .build();
      metadataAccumulator.setRootPointer(rootPointer);
      return;
    } else if (SplitType.DELTA_COMMIT_LOGS.equals(splitType)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Table '{}', data read from delta root pointer.",
          tableMetadata.getTable().getTableName());
      }
      //If it's a deltalake table only the partition xattr is needed.
      currentPartitionMetadata = PartitionMetadata.newBuilder().partition(null)
        .partitionXattr(HiveMetadataUtils.getPartitionXattr(tableMetadata.getTable(),
          HiveMetadataUtils.fromProperties(tableMetadata.getTableProperties())))
        .partitionValues(Collections.EMPTY_LIST)
        .inputSplitBatchIterator(
          InputSplitBatchIterator.newBuilder()
            .partition(null)
            .tableMetadata(tableMetadata)
            .inputSplits(Collections.EMPTY_LIST)
            .maxInputSplitsPerPartition(maxInputSplitsPerPartition)
            .build())
        .build();

      metadataAccumulator.accumulateReaderType(DeltaHiveInputFormat.class);
      metadataAccumulator.setTableLocation(DeltaHiveInputFormat.getLocation(tableMetadata.getTable(), optionManager));
      metadataAccumulator.setNotAllFSBasedPartitions();
      return;
    } else if (null == partitions) {
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
        .partitionXattr(HiveMetadataUtils.getPartitionXattr(tableMetadata.getTable(),
          HiveMetadataUtils.fromProperties(tableMetadata.getTableProperties())))
        .partitionValues(Collections.EMPTY_LIST)
        .datasetSplitBuildConf(null)
        .inputSplitBatchIterator(
          InputSplitBatchIterator.newBuilder()
            .partition(null)
            .tableMetadata(tableMetadata)
            .inputSplits(Collections.EMPTY_LIST)
            .maxInputSplitsPerPartition(maxInputSplitsPerPartition)
            .build())
        .dirListInputSplit(null)
        .build();

      metadataAccumulator.setTableLocation(tableMetadata.getTable().getSd().getLocation());
      final JobConf job = new JobConf(hiveConf);
      final Class<? extends InputFormat> inputFormatClazz = HiveMetadataUtils.getInputFormatClass(job, tableMetadata.getTable(), null);
      metadataAccumulator.accumulateReaderType(inputFormatClazz);
      return;
    }

    // Read Hive partition metadata.
    currentPartitionMetadata = HiveMetadataUtils.getPartitionMetadata(
      storageImpersonationEnabled, enforceVarcharWidth, tableMetadata, metadataAccumulator, partition,
      hiveConf, currentPartitionIndex, maxInputSplitsPerPartition, splitType, optionsManager);
  }

  private class HivePartitionChunkIteratorForInputSplit extends AbstractIterator<PartitionChunk> {
    @Override
    public PartitionChunk computeNext() {
      try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        do {
          // Check if current hive partition does not have remaining splits.
          if (!currentPartitionMetadata.getInputSplitBatchIterator().hasNext()) {

            // move to next partition
            final Partition partition = nextPartition();
            if (partition == null) {
              return endOfData();
            }

            // Read Hive partition metadata.
            currentPartitionMetadata = HiveMetadataUtils.getPartitionMetadata(
              storageImpersonationEnabled, enforceVarcharWidth, tableMetadata, metadataAccumulator, partition,
              hiveConf, currentPartitionIndex, maxInputSplitsPerPartition, splitType, optionManager);
          }
          // Current partition may have no splits. Advance to the next partition.
        } while (!currentPartitionMetadata.getInputSplitBatchIterator().hasNext());

        Preconditions.checkArgument(currentPartitionMetadata.getInputSplitBatchIterator().hasNext(), "no value exists in hive input split batch iterator.");

        List<DatasetSplit> datasetSplits = HiveMetadataUtils.getDatasetSplitsFromInputSplits(tableMetadata, metadataAccumulator, currentPartitionMetadata, statsParams);
        return PartitionChunk.of(currentPartitionMetadata.getPartitionValues(), datasetSplits,
          os -> os.write(currentPartitionMetadata.getPartitionXattr().toByteArray()));
      }
    }
  }

  private class HivePartitionChunkIteratorForDirListInputSplit extends AbstractIterator<PartitionChunk> {
    @Override
    public PartitionChunk computeNext() {
      try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        if (currentPartitionMetadata == null || currentPartitionMetadata.getDirListInputSplit() == null) {
          // move to next partition
          final Partition partition = nextPartition();
          if (partition == null) {
            return endOfData();
          }

          // Read Hive partition metadata.
          currentPartitionMetadata = HiveMetadataUtils.getPartitionMetadata(
            storageImpersonationEnabled, enforceVarcharWidth, tableMetadata, metadataAccumulator, partition,
            hiveConf, currentPartitionIndex, maxInputSplitsPerPartition, splitType, optionManager);

          // throw error if partitions found using different input format
          if (!metadataAccumulator.isAllPartitionsUseSameInputFormat()) {
            String errorMsg = String.format(DatasetSaverImpl.unsupportedPartitionListingError + " [%s]", tableMetadata.getTable().getTableName());
            logger.error(errorMsg);
            throw UserException.unsupportedError().message(errorMsg).build(logger);
          }
        }

        final List<DatasetSplit> datasetSplits = HiveMetadataUtils.getDatasetSplitsFromDirListSplits(tableMetadata, currentPartitionMetadata);
        PartitionChunk chunk = PartitionChunk.of(currentPartitionMetadata.getPartitionValues(), datasetSplits,
          os -> os.write(currentPartitionMetadata.getPartitionXattr().toByteArray()));

        // reset current Hive partition metadata.
        currentPartitionMetadata = null;
        return chunk;
      }
    }
  }

  private Partition nextPartition() {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      if (null != partitions && partitions.hasNext()) {
        final Partition partition = partitions.next();
        currentPartitionIndex++;

        if (logger.isDebugEnabled()) {
          logger.debug("Table '{}', advance to the next partition, '{}', partition chunk index {}",
            tableMetadata.getTable().getTableName(),
            HiveMetadataUtils.getPartitionValueLogString(partition),
            currentPartitionIndex);
        }
        return partition;
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Table '{}', no more partitions exist.",
            tableMetadata.getTable().getTableName());
        }
        return null;
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
    switch (splitType) {
      case INPUT_SPLIT:
        return new HivePartitionChunkIteratorForInputSplit();
      case DIR_LIST_INPUT_SPLIT:
        return new HivePartitionChunkIteratorForDirListInputSplit();
      case ICEBERG_MANIFEST_SPLIT:
        return Arrays.asList(
          PartitionChunk.of(HiveMetadataUtils.getDatasetSplitsForIcebergTables(tableMetadata),
            os -> os.write(currentPartitionMetadata.getPartitionXattr().toByteArray())))
          .iterator();
      case DELTA_COMMIT_LOGS:
        return Arrays.asList(
            PartitionChunk.of(deltaSplits,
              os -> os.write(currentPartitionMetadata.getPartitionXattr().toByteArray())))
          .iterator();
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException("Invalid Split type " + splitType);
    }
  }

  public static final class Builder {
    private boolean storageImpersonationEnabled;
    private boolean enforceVarcharWidth = false;
    private TableMetadata tableMetadata;
    private PartitionIterator partitions;
    private HiveConf hiveConf;
    private StatsEstimationParameters statsParams;
    private Integer maxInputSplitsPerPartition;
    private SplitType splitType;
    private List<DatasetSplit> deltaSplits;
    private OptionManager optionsManager;

    private Builder() {
    }

    public Builder deltaSplits(List<DatasetSplit> deltaSplits) {
      this.deltaSplits = deltaSplits;
      return this;
    }

    public Builder storageImpersonationEnabled(boolean storageImpersonationEnabled) {
      this.storageImpersonationEnabled = storageImpersonationEnabled;
      return this;
    }

    public Builder enforceVarcharWidth(boolean enforceVarcharWidth) {
      this.enforceVarcharWidth = enforceVarcharWidth;
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

    public Builder splitType(SplitType splitType) {
      this.splitType = splitType;
      return this;
    }

    public Builder optionManager(OptionManager optionsManager) {
      this.optionsManager = optionsManager;
      return this;
    }

    public HivePartitionChunkListing build() {

      Objects.requireNonNull(tableMetadata, "table metadata is required");
      Objects.requireNonNull(hiveConf, "hive conf is required");
      Objects.requireNonNull(statsParams, "stats params is required");
      Objects.requireNonNull(maxInputSplitsPerPartition, "maxInputSplitsPerPartition is required");
      Objects.requireNonNull(splitType, "splitType is required");
      Objects.requireNonNull(optionsManager, "optionsManager is required");

      return new HivePartitionChunkListing(storageImpersonationEnabled, enforceVarcharWidth, tableMetadata, hiveConf, statsParams, partitions, maxInputSplitsPerPartition, splitType, deltaSplits, optionsManager);
    }
  }
}
