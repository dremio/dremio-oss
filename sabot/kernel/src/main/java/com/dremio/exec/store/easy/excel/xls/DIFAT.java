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

import org.apache.poi.poifs.common.POIFSBigBlockSize;
import org.apache.poi.poifs.common.POIFSConstants;
import org.apache.poi.poifs.storage.BATBlock;
import org.apache.poi.poifs.storage.HeaderBlock;

/**
 * the DIFAT or Double Indirect File Allocation Table, is a structure that is used to locate FAT blocks in a
 * compound file.
 */
class DIFAT extends BlockStoreBase {

  private final POIFSBigBlockSize bigBlockSize;

  DIFAT(HeaderBlock header, final XlsInputStream is) {
    super(is, header, header.getBigBlockSize().getBigBlockSize());

    bigBlockSize = header.getBigBlockSize();

    readAllBATs();
  }

  @Override
  public int getBlockOffset(int block) {
    int blockWanted = block + 1; // The header block doesn't count, so add one
    return blockWanted * getBlockSize();
  }

  /**
   * reads all DIFAT sectors.<br>
   * <br>
   * the first DIFAT array, which contains up to 109 element is part of the header
   * (POI refers to it as BAT table). The remaining DIFATs (or XBATs) are stored as a FAT chain where the first
   * one is at sector header._xbat_start
   */
  private void readAllBATs() {
    // Most likely we only need it to return nextBlock of a given sector

    // the first 109 FAT sectors' indices are stored in the header
    for(int fatAt : header.getBATArray()) {
      readBAT(fatAt);
    }
    // Any additional BAT sectors are held in the XBAT (DIFAT) sectors in a chain.

    // Work out how many FAT blocks remain in the XFATs
    int remainingFATs = header.getBATCount() - header.getBATArray().length;

    // Now read the XFAT blocks, and the FATs within them
    int nextAt = header.getXBATIndex();
    for(int i = 0; i < header.getXBATCount(); i++) {
      BATBlock xfat = BATBlock.createBATBlock(bigBlockSize, getBlockBuffer(nextAt));
      xfat.setOurBlockIndex(nextAt);
      // notice that XFATs are stored as a chain in the FAT
      nextAt = xfat.getValueAt(bigBlockSize.getXBATEntriesPerBlock());

      // Process all the (used) FATs from this XFAT
      int xbatFATs = Math.min(remainingFATs, bigBlockSize.getXBATEntriesPerBlock());
      for(int j = 0; j < xbatFATs; j++) {
        int fatAt = xfat.getValueAt(j);
        if (fatAt == POIFSConstants.UNUSED_BLOCK || fatAt == POIFSConstants.END_OF_CHAIN) {
          break;
        }
        readBAT(fatAt);
      }
      remainingFATs -= xbatFATs;
    }
  }

  private void readBAT(int sector) {
    BATBlock bat = BATBlock.createBATBlock(bigBlockSize, getBlockBuffer(sector));
    bat.setOurBlockIndex(sector);
    blocks.add(bat);
  }

}
