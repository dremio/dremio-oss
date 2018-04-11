/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.easy.excel.xls.properties;

import com.dremio.exec.store.easy.excel.xls.BlockStore;
import com.dremio.exec.store.easy.excel.xls.XlsInputStream;
import com.google.common.collect.Lists;

import org.apache.poi.poifs.common.POIFSConstants;
import org.apache.poi.poifs.property.PropertyConstants;
import org.apache.poi.poifs.storage.HeaderBlock;

import java.io.IOException;
import java.util.List;
import java.util.Stack;

/**
 * DIRECTORY table. Contains meta information about the file's structure.<br>
 * <br>
 * table properties are Directory entries that describe the content of the file
 * each entry can be either a root, storage or stream. In case of .xls storage=workbook, stream=sheet
 *
 * Simplified version of {@link org.apache.poi.poifs.property.PropertyTable}
 */
public class PropertyTable {

  private final XlsInputStream is;
  private final int sectorSize;

  private final List<Property> properties = Lists.newArrayList();

  /**
   * determine whether the specified index is valid
   *
   * @param index value to be checked
   *
   * @return true if the index is valid
   */
  private static boolean isValidIndex(int index) {
    return index != -1;
  }

  public PropertyTable(final XlsInputStream is, HeaderBlock header, BlockStore blockStore) {
    this.is = is;
    this.sectorSize = header.getBigBlockSize().getBigBlockSize();


    // Directory sectors are stored as a chain starting from sector # header.propertyStart
    // and FAT table contains link to next sector in the chain

    int nextBlock = header.getPropertyStart(); // first sector in directory chain
    while (nextBlock != POIFSConstants.END_OF_CHAIN) {
      int blockOffset = blockStore.getBlockOffset(nextBlock);
      processSector(blockOffset);
      nextBlock = blockStore.getNextBlock(nextBlock); // get next block in the chain
    }

    populatePropertyTree((DirectoryProperty) properties.get(0));
  }

  private void processSector(int sectorOffset) {
    final int property_count = sectorSize / POIFSConstants.PROPERTY_SIZE;

    byte[] bytes = new byte[POIFSConstants.PROPERTY_SIZE];

    int index = properties.size();
    is.seek(sectorOffset);
    for (int k = 0; k < property_count; k++, index++) {
      try {
        is.read(bytes, 0, bytes.length);
      } catch (IOException e) {
        // shouldn't be possible in a well formatted xls stream
        throw new IllegalStateException("Couldn't read from stream");
      }

      switch (bytes[PropertyConstants.PROPERTY_TYPE_OFFSET ]) {
        case PropertyConstants.DIRECTORY_TYPE :
        case PropertyConstants.ROOT_TYPE :
          properties.add(new DirectoryProperty(index, bytes));
          break;
        case PropertyConstants.DOCUMENT_TYPE :
          properties.add(new DocumentProperty(index, bytes));
          break;
        default :
          // add a null as we'll need to access properties by index later (or do we ?)
          properties.add(null);
          break;
      }
    }
  }


  private void populatePropertyTree(DirectoryProperty root) {
    int index = root.getChildIndex();

    if (!isValidIndex(index)) {
      return; // property has no children
    }
    Stack<Property> children = new Stack<>();

    children.push(properties.get(index));
    while (!children.empty()) {
      Property property = children.pop();
      if (property == null) {
        continue; // unknown / unsupported / corrupted property, skip
      }

      root.addChild(property);
      if (property.isDirectory()) {
        populatePropertyTree((DirectoryProperty) property);
      }
      index = property.getPreviousChildIndex();
      if (isValidIndex(index))
      {
        children.push(properties.get(index));
      }
      index = property.getNextChildIndex();
      if (isValidIndex(index))
      {
        children.push(properties.get(index));
      }
    }
  }

  /**
   * @return root property of the compound file
   */
  public DirectoryProperty getRoot() {
    // it's always the first element in the List
    return (DirectoryProperty) properties.get(0);
  }

  public int size() {
    return properties.size();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("PropertyTable with ")
            .append(size())
            .append(" entries: [\n");
    for (Property property : properties) {
      if (property != null) {
        builder.append("\t").append(property.getName()).append("\n");
      }
    }
    builder.append("]");
    return builder.toString();
  }
}
