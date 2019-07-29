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

import static org.apache.poi.hssf.model.InternalWorkbook.OLD_WORKBOOK_DIR_ENTRY_NAME;
import static org.apache.poi.hssf.model.InternalWorkbook.WORKBOOK_DIR_ENTRY_NAMES;

import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.OldExcelFormatException;
import org.apache.poi.poifs.common.POIFSConstants;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.storage.HeaderBlock;

import com.dremio.exec.store.easy.excel.xls.poi.DirectoryNode;
import com.dremio.exec.store.easy.excel.xls.poi.DocumentNode;
import com.dremio.exec.store.easy.excel.xls.properties.PropertyTable;
import com.google.common.base.Preconditions;

/**
 * Bare minimum xls reader based on POI
 *
 * XLS files are Compound File Binary (CFB) Files.<br>
 * A compound file is divided into equal-length sectors.
 * The first sector contains the compound file header.
 * Subsequent sectors are identified by a 32-bit nonnegative integer number, called the sector number.
 *
 * A group of sectors can form a sector chain, which is a linked list of sectors forming a logical byte array,
 * even though the sectors can be in non-consecutive locations in the compound file
 */
class XlsReader {
  private final XlsInputStream is;

  private DIFAT difats;
  private MiniStore miniStore;

  private PropertyTable propertyTable;

  /**
   * Reads the core content of an XLS binary array.<br>
   * Extracts all information necessary to read the workbook stream.
   *
   * @param inputStream XLS input stream
   *
   * @throws IOException if data doesn't start with proper header
   */
  private XlsReader(final XlsInputStream inputStream) throws IOException {
    this.is = inputStream;
    readCoreContent();
  }

  /**
   * Read and process the PropertiesTable (DIR sectors) and the FAT / XFAT blocks (DIFAT sectors),
   * so that we're ready to work with the file
   *
   * @throws IOException if data doesn't start with proper header
   */
  private void readCoreContent() throws IOException {
    // read CFBF header, it points to everything in the file
    // Grabs the first sector (512 bytes)
    // (For 4096 sized blocks, the remaining 3584 bytes are zero, so read them too)

    // we read the following fields
    // [00H,08] {0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1}: signature
    // [1EH,02] size of sectors in power-of-two and stores it in bigBlockSize
    // [2CH,04] number of SECTs in the FAT chain: _bat_count (_csectFat)
    // [30H,04] first SECT in the directory chain: _property_start (_sectDirStart)
    // [3CH,04] first SECT in the MiniFAT chain: _sbat_start (_sectMiniFatStart)
    // [40H,04] number of SECTs in the MiniFAT chain: _sbat_count (_csectMiniFat)
    // [44H,04] first SECT in the DIFAT chain: _xbat_start (_sectDifStart)
    // [48H,04] number of SECTs in the DIFAT chain: _xbat_count (_csectDif)
    // [4CH,436] the SECTs of first 109 FAT sectors: remaining bytes in header (SECT _sectFat[109])
    final HeaderBlock header = new HeaderBlock(is);

    // Read the FAT blocks
    difats = new DIFAT(header, is);

    // We're now able to load steams
    propertyTable = new PropertyTable(is, header, difats);

    // Finally read the Small Stream FAT (SBAT or mini-FAT) blocks
    // also called mini-FAT in CFB specification
    miniStore = new MiniStore(is, propertyTable.getRoot(), header, difats);
  }

  private static String getWorkbookDirEntryName(DirectoryNode directory) {

    for (final String wbName : WORKBOOK_DIR_ENTRY_NAMES) {
      try {
        directory.getEntry(wbName);
        return wbName;
      } catch (IllegalStateException e) {
        // continue - to try other options
      }
    }

    // check for an encrypted .xlsx file - they get OLE2 wrapped
    try {
      directory.getEntry(Decryptor.DEFAULT_POIFS_ENTRY);
      throw new EncryptedDocumentException("The supplied spreadsheet seems to be an Encrypted .xlsx file." +
              "It must be decrypted before it can be read");
    } catch (IllegalStateException e) {
      // fall through
    }

    // check for previous version of file format
    try {
      directory.getEntry(OLD_WORKBOOK_DIR_ENTRY_NAME);
      throw new OldExcelFormatException("The supplied spreadsheet seems to be Excel 5.0/7.0 (BIFF5) format. "
              + "We only support BIFF8 format (from Excel versions 97/2000/XP/2003)");
    } catch (IllegalStateException e) {
      // fall through
    }

    throw new IllegalArgumentException("The supplied XLS file does not contain a BIFF8 'Workbook' entry. "
            + "Is it really an excel file?");
  }

  private DocumentEntry getWorkbookEntry() {
    final DirectoryNode dir = new DirectoryNode(propertyTable.getRoot(), null, difats);

    String name = getWorkbookDirEntryName(dir);

    Entry workbook = dir.getEntry(name);
    Preconditions.checkState(!workbook.isDocumentEntry(), "Entry '" + workbook.getName() + "' is not a DocumentEntry");

    return (DocumentEntry) workbook;
  }

  /**
   * returns a {@link BlockStoreInputStream} that exposes all workbook sectors in their correct order
   *
   * @param is XLS InputStream
   * @return {@link BlockStoreInputStream} that wraps the workbook's stream
   *
   * @throws IOException if the data doesn't contain a proper MS-CFB header
   * @throws OldExcelFormatException if the file is too old to be supported
   */
  static InputStream createWorkbookInputStream(final XlsInputStream is) throws IOException {
    final XlsReader xlsReader = new XlsReader(is);
    DocumentEntry workBookEntry = xlsReader.getWorkbookEntry();
    DocumentNode workbookNode = (DocumentNode) workBookEntry;

    // use proper blockStore
    final boolean useMiniStore = workbookNode.getSize() < POIFSConstants.BIG_BLOCK_MINIMUM_DOCUMENT_SIZE;
    final BlockStore blockStore = useMiniStore ? xlsReader.miniStore : xlsReader.difats;

    return new BlockStoreInputStream(is, blockStore, workbookNode.getProperty().getStartBlock());
  }

}
