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
package com.dremio.exec.store.parquet;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.parquet.hadoop.PageHeaderWithOffset;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Utility class to reduce the heap size of Parquet footer. Useful in removing
 * portions of the footer that are no longer required
 */
public class MutableParquetMetadata {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MutableParquetMetadata.class);
  private final long[] accumulatedRowCount; // Accumulation of row counts by row groups.
  private ParquetMetadata footer;
  private String fileName;
  private boolean columnsTrimmed = false;
  private long numColumnsTrimmed = 0;

  public MutableParquetMetadata(ParquetMetadata footer, String fileName) {
    this.footer = footer;
    this.fileName = fileName;
    // accumulatedRowCount has counts for each row group + a total count for the file in the last element
    this.accumulatedRowCount = new long[footer.getBlocks().size() + 1];

    // Initialize accumulated row counts
    if (footer.getBlocks().size() > 0) {
      // Accumulation of the row count for ith row group is the summary of row counts for all row groups before ith row group.
      accumulatedRowCount[0] = 0;
      for (int i = 0; i < footer.getBlocks().size();  i++) {
        accumulatedRowCount[i + 1] = accumulatedRowCount[i] + footer.getBlocks().get(i).getRowCount();
      }
    }
    logger.debug("Created parquet metadata with row group size {} for file {}", footer.getBlocks().size(), this.fileName);
  }

  public List<BlockMetaData> getBlocks() {
    return footer.getBlocks();
  }

  public FileMetaData getFileMetaData() {
    return footer.getFileMetaData();
  }

  public long removeUnusedRowGroups(Set<Integer> rowGroupsToRetain) {
    List<BlockMetaData> blocks = getBlocks();
    long numRowGroupsRemoved = 0;

    for(int i = 0; i < blocks.size(); i++) {
      if (!rowGroupsToRetain.contains(i)) {
        logger.debug("Removing row group index {} for file {}", i, this.fileName);
        blocks.set(i, null);
        numRowGroupsRemoved++;
      }
    }

    // ParquetMetadata returns its intenal ArrayList as part of the call to getBlocks()
    // so, it is not required to create a new footer object here
    return numRowGroupsRemoved;
  }

  public Long getAccumulatedRowCount(int rowGroupIndex) {
    Preconditions.checkArgument(rowGroupIndex < accumulatedRowCount.length - 1, "Row group index out of bounds");
    return accumulatedRowCount[rowGroupIndex];
  }

  public long getEndRowPos(int rowGroupIndex) {
    Preconditions.checkArgument(rowGroupIndex < accumulatedRowCount.length - 1, "Row group index out of bounds");
    return accumulatedRowCount[rowGroupIndex + 1];
  }

  private BlockMetaData stripUnneededColumnInfo(BlockMetaData blockMetaData, Set<String> parquetColumnNamesToRetain) {
    BlockMetaData newBlockMetaData = new BlockMetaData();
    // retain the first column always... This is used to figure out the first offset of a row group in the context of Hive
    boolean firstColumn = true;
    for(ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
      // iterate over all columns in the row group
      // for nested types, this should be the top-level name
      String columnNameInParquet = columnChunkMetaData.getPath().iterator().next();
      if (firstColumn || parquetColumnNamesToRetain.contains(columnNameInParquet.toLowerCase())) {
        // need to retain information about this column
        newBlockMetaData.addColumn(columnChunkMetaData);
      } else {
        numColumnsTrimmed++;
      }

      firstColumn = false;
    }

    // copy over the rest of the fields
    newBlockMetaData.setPath(blockMetaData.getPath());
    newBlockMetaData.setRowCount(blockMetaData.getRowCount());
    newBlockMetaData.setTotalByteSize(blockMetaData.getTotalByteSize());
    return newBlockMetaData;
  }

  /**
   * Removes unneeded columns from the footer. This function removes the columns on the first call - subsequent calls do nothing
   *
   * @param parquetColumnNamesToRetain: lower case column names from the parquet file
   */
  public long removeUnneededColumns(Set<String> parquetColumnNamesToRetain) {
    if (columnsTrimmed) {
      // columns already trimmed
      return 0;
    }

    List<BlockMetaData> blocks = getBlocks();
    if (blocks.size() == 0) {
      return 0;
    }

    List<BlockMetaData> newBlocks = Lists.newArrayList();
    for(BlockMetaData blockMetaData : blocks) {
      BlockMetaData newBlock = blockMetaData;
      if (blockMetaData != null) {
        newBlock = stripUnneededColumnInfo(blockMetaData, parquetColumnNamesToRetain);
      }

      newBlocks.add(newBlock);
    }

    Preconditions.checkArgument(newBlocks.size() == blocks.size(),
      "Number of row groups does not match after removing unneeded columns, old = " + blocks.size() + ", new = " + newBlocks.size());
    footer = new ParquetMetadata(getFileMetaData(), newBlocks);
    columnsTrimmed = true;
    return numColumnsTrimmed;
  }

  public void removeRowGroupInformation(int rowGroupIndex) {
    logger.debug("Removing row group index {} for file {}", rowGroupIndex, this.fileName);
    List<BlockMetaData> blocks = getBlocks();
    blocks.set(rowGroupIndex, null);

    // ParquetMetadata returns its intenal ArrayList as part of the call to getBlocks()
    // so, it is not required to create a new footer object here
  }

  // removes PageHeaderWithOffset objects from the footer
  public void removePageHeaders(int rowGroupIndex) {
    List<BlockMetaData> blocks = getBlocks();
    if (blocks.size() <= rowGroupIndex) {
      return;
    }

    BlockMetaData blockMetaData = blocks.get(rowGroupIndex);
    BlockMetaData newBlockMetaData = new BlockMetaData();
    for(ColumnChunkMetaData column : blockMetaData.getColumns()) {
      ColumnChunkMetaData newColumn = column;
      List<PageHeaderWithOffset> headers = column.getPageHeaders();
      if (headers != null && headers.size() > 0) {
        // clone without page headers
        newColumn = ColumnChunkMetaData.get(
          column.getPath(),
          column.getPrimitiveType(),
          column.getCodec(),
          column.getEncodingStats(),
          column.getEncodings(),
          column.getStatistics(),
          column.getFirstDataPageOffset(),
          column.getDictionaryPageOffset(),
          column.getValueCount(),
          column.getTotalSize(),
          column.getTotalUncompressedSize(),
          Collections.emptyList());
      }
      newBlockMetaData.addColumn(newColumn);
    }

    // copy over the rest of the fields
    newBlockMetaData.setPath(blockMetaData.getPath());
    newBlockMetaData.setRowCount(blockMetaData.getRowCount());
    newBlockMetaData.setTotalByteSize(blockMetaData.getTotalByteSize());
    blocks.set(rowGroupIndex, newBlockMetaData);
  }

  public String toString() {
    final long numNonNullBlocks = getBlocks()
      .stream()
      .filter((b) -> (b != null))
      .count();
    return "ParquetMetaData{" + this.getFileMetaData() + ", num blocks=" + getBlocks().size() + ", non-null blocks=" + numNonNullBlocks + "}";
  }
}
