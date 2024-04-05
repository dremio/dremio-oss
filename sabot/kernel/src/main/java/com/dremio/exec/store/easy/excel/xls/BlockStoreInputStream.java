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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import org.apache.poi.poifs.common.POIFSConstants;

/**
 * Very basic implementation of InputStream that uses a BlockStore to figure out which xls block it
 * should read next.
 */
class BlockStoreInputStream extends InputStream {

  private final XlsInputStream inputStream;
  private final BlockStore blockStore;
  private final int blockSize;

  private int nextBlock;
  private int remainingBlocks;
  private int offsetInBlock; // read position in current block

  BlockStoreInputStream(
      final XlsInputStream inputStream, final BlockStore blockStore, final int startBlock) {
    this.inputStream = Preconditions.checkNotNull(inputStream, "input stream should not be null");
    this.blockStore = Preconditions.checkNotNull(blockStore, "block store should not be null");
    nextBlock = startBlock;
    blockSize = blockStore.getBlockSize();

    Preconditions.checkState(
        startBlock != POIFSConstants.END_OF_CHAIN, "startBlock cannot be END_OF_CHAIN");

    // count number of blocks that are part of the stream chain, including current block!
    remainingBlocks = 0;
    int block = nextBlock;
    while (block != POIFSConstants.END_OF_CHAIN) {
      remainingBlocks++;
      block = blockStore.getNextBlock(block);
    }

    // move to the beginning of the first block in the chain
    inputStream.seek(blockStore.getBlockOffset(nextBlock));
  }

  private void seekNextBlock() {
    assert remainingBlocks > 0 : "we tried to read past the stream chain";

    nextBlock = blockStore.getNextBlock(nextBlock);
    inputStream.seek(blockStore.getBlockOffset(nextBlock));

    remainingBlocks--;
    offsetInBlock = 0;
  }

  @Override
  public int available() {
    return remainingBlocks * blockSize - offsetInBlock;
  }

  @Override
  public int read() {
    if (offsetInBlock == blockSize) {
      if (remainingBlocks == 1) {
        // no next block left to seek
        return -1;
      }
      seekNextBlock();
    }
    offsetInBlock++;
    return inputStream.read();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
