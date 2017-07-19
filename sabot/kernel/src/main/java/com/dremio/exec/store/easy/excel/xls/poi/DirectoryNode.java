/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.easy.excel.xls.poi;

import com.dremio.exec.store.easy.excel.xls.BlockStore;
import com.dremio.exec.store.easy.excel.xls.properties.DirectoryProperty;
import com.dremio.exec.store.easy.excel.xls.properties.DocumentProperty;
import com.dremio.exec.store.easy.excel.xls.properties.Property;
import com.google.common.collect.Maps;

import org.apache.poi.poifs.filesystem.Entry;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Simplified implementation of {@link org.apache.poi.poifs.filesystem.DirectoryNode}
 */
public class DirectoryNode extends EntryNode {


  // Map of Entry instances, keyed by their names
  private final Map<String,Entry> byname = Maps.newHashMap();

  public DirectoryNode(final DirectoryProperty property, DirectoryNode parent, final BlockStore blockStore) {
    super(property, parent);

    Iterator<Property> iter = property.getChildren();

    while (iter.hasNext()) {
      Property child = iter.next();
      Entry childNode;

      if (child.isDirectory()) {
        DirectoryProperty childDir = (DirectoryProperty) child;
        childNode = new DirectoryNode(childDir, this, blockStore);
      } else {
        childNode = new DocumentNode((DocumentProperty) child, this);
      }
      byname.put(childNode.getName(), childNode);
    }

  }

  public Set<String> getEntryNames() {
    return byname.keySet();
  }

  /**
   * get a specified Entry by name
   *
   * @param name the name of the Entry to obtain.
   *
   * @return the specified Entry, if it is directly contained in
   *         this DirectoryEntry
   *
   * @exception IllegalStateException if no Entry with the specified name exists in this DirectoryEntry
   */

  public Entry getEntry(final String name) {
    Entry rval = null;

    if (name != null) {
      rval = byname.get(name);
    }
    if (rval == null) { // either a null name was given, or there is no such name
      throw new IllegalStateException("no such entry: \"" + name + "\", had: " + byname.keySet());
    }
    return rval;
  }

}
