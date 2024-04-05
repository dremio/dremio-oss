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
package com.dremio.exec.store.easy.excel.xls;

import com.dremio.exec.store.easy.excel.xls.properties.DirectoryProperty;
import com.google.common.base.Preconditions;
import org.apache.poi.poifs.common.POIFSConstants;
import org.apache.poi.poifs.storage.BATBlock;
import org.apache.poi.poifs.storage.HeaderBlock;

/**
 * Special implementation of BlockStore to handle a mini-stream (stream of 64B blocks) stored in a
 * regular DIFAT chain.
 */
public class MiniStore extends BlockStoreBase {

  private final DirectoryProperty root;
  private final BlockStore difats;

  public MiniStore(
      final XlsInputStream is, DirectoryProperty root, HeaderBlock header, BlockStore difats) {
    super(is, header, POIFSConstants.SMALL_BLOCK_SIZE);
    this.root = root;
    this.difats = difats;

    // load all mini-FAT blocks
    int nextAt = header.getSBATStart();
    for (int i = 0; i < header.getSBATCount() && nextAt != POIFSConstants.END_OF_CHAIN; i++) {
      BATBlock sfat =
          BATBlock.createBATBlock(header.getBigBlockSize(), difats.getBlockBuffer(nextAt));
      sfat.setOurBlockIndex(nextAt);
      blocks.add(sfat);
      nextAt = difats.getNextBlock(nextAt);
    }
  }

  @Override
  public int getBlockOffset(int miniBlock) {
    // we want to read mini block with index = miniBlock

    // first we need to figure out which DIFAT block the mini block is in
    int byteOffset = miniBlock * POIFSConstants.SMALL_BLOCK_SIZE; // offset from beginning of SBAT
    int difatBlockNumber = byteOffset / difats.getBlockSize();
    int offsetInDifatBlock = byteOffset % difats.getBlockSize();

    // Now locate the data block for it, starting from the 1st block of the mini-FAT
    int nextBlock = root.getStartBlock(); // first sector in mini-FAT chain
    int idx = 0;
    while (nextBlock != POIFSConstants.END_OF_CHAIN && idx < difatBlockNumber) {
      nextBlock = difats.getNextBlock(nextBlock); // get next block in the chain
      idx++;
    }

    Preconditions.checkState(
        idx == difatBlockNumber, "DIFAT block " + difatBlockNumber + " outside stream chain");

    int difatBlockOffset = difats.getBlockOffset(nextBlock);

    return difatBlockOffset + offsetInDifatBlock;
  }
}
