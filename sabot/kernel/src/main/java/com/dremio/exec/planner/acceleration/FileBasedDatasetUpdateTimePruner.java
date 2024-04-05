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

package com.dremio.exec.planner.acceleration;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.AbstractPartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.TableScan;

public final class FileBasedDatasetUpdateTimePruner {

  private FileBasedDatasetUpdateTimePruner() {}

  /**
   * Checks if a dataset supports non-partition pruning of splits during planning based on last
   * modified times.
   */
  public static boolean scanSupportsSplitUpdateTimePruning(TableScan tableScan) {
    if (!(tableScan instanceof ScanCrel)) {
      return false;
    }

    ScanCrel scanCrel = (ScanCrel) tableScan;
    DatasetConfig dataset = scanCrel.getTableMetadata().getDatasetConfig();

    // it must be a file-based dataset
    if (dataset == null
        || dataset.getPhysicalDataset() == null
        || dataset.getPhysicalDataset().getFormatSettings() == null) {
      return false;
    }

    // if update column is a partition column, standard sarg-based partition pruning can be used
    // instead to prune
    List<String> partitionColumns =
        dataset.getReadDefinition() == null
            ? null
            : dataset.getReadDefinition().getPartitionColumnsList();
    if (partitionColumns != null
        && partitionColumns.contains(IncrementalUpdateUtils.UPDATE_COLUMN)) {
      return false;
    }

    // only text, json, and excel datasets qualify for this
    switch (dataset.getPhysicalDataset().getFormatSettings().getType()) {
        // text formats
      case TEXT:
      case CSV:
      case TSV:
      case PSV:
        // json format
      case JSON:
        // excel formats
      case EXCEL:
      case XLS:
        return true;
      default:
        return false;
    }
  }

  public static TableScan prune(TableScan tableScan, Long minModTime) {
    if (minModTime == null) {
      return tableScan;
    }

    ScanCrel scanCrel = (ScanCrel) tableScan;
    TableMetadata prunedMetadata = filterSplits(scanCrel.getTableMetadata(), minModTime);
    return scanCrel.withTableMetadata(prunedMetadata);
  }

  private static TableMetadata filterSplits(TableMetadata tableMetadata, long minModTime) {
    List<PartitionChunkMetadata> filteredSplits =
        Streams.stream(tableMetadata.getSplits())
            .map(chunk -> filterPartitionChunk(chunk, minModTime))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    return tableMetadata.prune(filteredSplits);
  }

  private static PartitionChunkMetadata filterPartitionChunk(
      PartitionChunkMetadata metadata, long minModTime) {
    long size = 0;
    long rowCount = 0;
    ImmutableList.Builder<PartitionProtobuf.DatasetSplit> splitListBuilder =
        new ImmutableList.Builder<>();

    for (PartitionProtobuf.DatasetSplit split : metadata.getDatasetSplits()) {
      EasyProtobuf.EasyDatasetSplitXAttr xattr = null;
      if (split.hasSplitExtendedProperty()) {
        try {
          xattr = EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(split.getSplitExtendedProperty());
        } catch (InvalidProtocolBufferException ex) {
          throw new RuntimeException("Failed to deserialize split info", ex);
        }
      }

      if (xattr == null
          || !xattr.hasUpdateKey()
          || !xattr.getUpdateKey().hasLastModificationTime()) {
        throw UserException.invalidMetadataError()
            .message(
                "Dataset is missing last modified times metadata for data files.  Please refresh metadata.")
            .buildSilently();
      }

      long modTime = xattr.getUpdateKey().getLastModificationTime();
      if (modTime > minModTime) {
        splitListBuilder.add(split);
        size += split.getSize();
        rowCount += split.getRecordCount();
      }
    }

    List<PartitionProtobuf.DatasetSplit> splits = splitListBuilder.build();
    if (splits.size() > 0) {
      PartitionProtobuf.PartitionChunk chunk =
          PartitionProtobuf.PartitionChunk.newBuilder()
              .setSize(size)
              .setRowCount(rowCount)
              .addAllPartitionValues(metadata.getPartitionValues())
              .setSplitKey(metadata.getSplitKey())
              .setSplitCount(splits.size())
              .setPartitionExtendedProperty(metadata.getPartitionExtendedProperty())
              .build();

      return new AbstractPartitionChunkMetadata(chunk) {
        @Override
        public Iterable<PartitionProtobuf.DatasetSplit> getDatasetSplits() {
          return splits;
        }

        @Override
        public boolean checkPartitionChunkMetadataConsistency() {
          return true;
        }
      };
    }

    return null;
  }
}
