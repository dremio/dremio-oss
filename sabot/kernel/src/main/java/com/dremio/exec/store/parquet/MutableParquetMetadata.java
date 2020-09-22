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

import java.util.List;
import java.util.Set;

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
  private ParquetMetadata footer;
  private boolean columnsTrimmed = false;
  private long numColumnsTrimmed = 0;

  public MutableParquetMetadata(ParquetMetadata footer) {
    this.footer = footer;
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
        blocks.set(i, null);
        numRowGroupsRemoved++;
      }
    }

    // ParquetMetadata returns its intenal ArrayList as part of the call to getBlocks()
    // so, it is not required to create a new footer object here
    return numRowGroupsRemoved;
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
    List<BlockMetaData> blocks = getBlocks();
    blocks.set(rowGroupIndex, null);

    // ParquetMetadata returns its intenal ArrayList as part of the call to getBlocks()
    // so, it is not required to create a new footer object here
  }

  public String toString() {
    final long numNonNullBlocks = getBlocks()
      .stream()
      .filter((b) -> (b != null))
      .count();
    return "ParquetMetaData{" + this.getFileMetaData() + ", num blocks=" + getBlocks().size() + ", non-null blocks=" + numNonNullBlocks + "}";
  }
}
