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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.poi.poifs.storage.BATBlock;
import org.apache.poi.poifs.storage.HeaderBlock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base implementation of {@link BlockStoreBase} backed by a byte array
 */
abstract class BlockStoreBase implements BlockStore {
  private final int blockSize;
  private final XlsInputStream is;
  protected final List<BATBlock> blocks = Lists.newArrayList();
  protected final HeaderBlock header;

  BlockStoreBase(final XlsInputStream is, HeaderBlock header, int blockSize) {
    this.is = is;
    this.blockSize = blockSize;
    this.header = Preconditions.checkNotNull(header, "header cannot be null");
  }

  @Override
  public int getBlockSize() {
    return blockSize;
  }

  @Override
  public ByteBuffer getBlockBuffer(int block) {
    byte[] bytes = new byte[blockSize];
    try {
      is.seek(getBlockOffset(block));
      is.read(bytes, 0, blockSize);
    } catch (IOException e) {
      // we should never hit this in a well formatted xls file
      throw new IllegalStateException("Couldn't read from xls stream");
    }
    return ByteBuffer.wrap(bytes);
  }

  @Override
  public int getNextBlock(int sector) {
    BATBlock.BATBlockAndIndex bai = getBATBlockAndIndex(sector);
    return bai.getBlock().getValueAt( bai.getIndex() );
  }

  private BATBlock.BATBlockAndIndex getBATBlockAndIndex(final int sector) {
    return BATBlock.getBATBlockAndIndex(sector, header, blocks);
  }
}
