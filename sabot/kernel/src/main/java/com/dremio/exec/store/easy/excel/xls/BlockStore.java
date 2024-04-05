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

import java.nio.ByteBuffer;

/**
 * This interface describes a way to read a series of blocks stored as a FAT chain.<br>
 * <br>
 * According to the MS-CFB file format, a file is a series of blocks that need to be read according
 * to a FAT stored in the file itself. The FAT can define "chains" as a series of blocks where we
 * store in the FAT for each block it's following block in the chain or END_OF_CHAIN if the chain
 * ended. <br>
 * <br>
 * Simplified implementation of {@link org.apache.poi.poifs.filesystem.BlockStore}
 */
public interface BlockStore {

  int getBlockSize();

  /**
   * Works out what block follows the current one.
   *
   * @param block current block index
   * @return next block index or POIFSConstants.END_OF_CHAIN
   */
  int getNextBlock(int block);

  /**
   * computes start offset of a given block stored in this BlockStore
   *
   * @param block block index in the store
   * @return absolute block offset
   */
  int getBlockOffset(int block);

  ByteBuffer getBlockBuffer(int block);
}
