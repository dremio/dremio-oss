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
package com.dremio.exec.store.easy.excel.xls.poi;

import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.Entry;

import com.dremio.exec.store.easy.excel.xls.properties.Property;

/**
 * Simplified implementation of {@link org.apache.poi.poifs.filesystem.EntryNode}
 */

public abstract class EntryNode implements Entry {

  // the DocumentProperty backing this object
  private Property property;

  // this object's parent Entry
  private DirectoryNode parent;

  /**
   * create a DocumentNode. This method is not public by design; it
   * is intended strictly for the internal use of extending classes
   *
   * @param property the Property for this Entry
   * @param parent the parent of this entry
   */

  protected EntryNode(final Property property, final DirectoryNode parent) {
    this.property = property;
    this.parent   = parent;
  }

  /**
   * grant access to the property
   *
   * @return the property backing this entry
   */

  protected Property getProperty()
  {
    return property;
  }

  /**
   * is this the root of the tree?
   *
   * @return true if so, else false
   */

  protected boolean isRoot() {
    // only the root Entry has no parent ...
    return (parent == null);
  }

    /* ********** START implementation of Entry ********** */

  /**
   * get the name of the Entry
   *
   * @return name
   */
  public String getName()
  {
    return property.getName();
  }

  /**
   * is this a DirectoryEntry?
   *
   * @return true if the Entry is a DirectoryEntry, else false
   */
  public boolean isDirectoryEntry()
  {
    return false;
  }

  /**
   * is this a DocumentEntry?
   *
   * @return true if the Entry is a DocumentEntry, else false
   */
  public boolean isDocumentEntry()
  {
    return false;
  }

  /**
   * get this Entry's parent (the DocumentEntry that owns this
   * Entry). All Entry objects, except the root Entry, has a parent.
   *
   * @return this Entry's parent; null iff this is the root Entry
   */
  public DirectoryEntry getParent() {
    throw new IllegalStateException("Not Implemented");
  }

  /**
   * Delete this Entry. This operation should succeed, but there are
   * special circumstances when it will not:
   *
   * If this Entry is the root of the Entry tree, it cannot be
   * deleted, as there is no way to create another one.
   *
   * If this Entry is a directory, it cannot be deleted unless it is
   * empty.
   *
   * @return true if the Entry was successfully deleted, else false
   */
  public boolean delete() {
    throw new IllegalStateException("Not Supported");
  }


  /**
   * Rename this Entry. This operation will fail if:
   *
   * There is a sibling Entry (i.e., an Entry whose parent is the
   * same as this Entry's parent) with the same name.
   *
   * This Entry is the root of the Entry tree. Its name is dictated
   * by the Filesystem and many not be changed.
   *
   * @param newName the new name for this Entry
   *
   * @return true if the operation succeeded, else false
   */

  public boolean renameTo(final String newName) {
    throw new IllegalStateException("Not Supported");
  }

}
