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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Simplified implementation of {@link org.apache.poi.poifs.property.DirectoryProperty}
 */
public class DirectoryProperty extends Property {

  /** List of Property instances */
  private List<Property> children = Lists.newArrayList();

  /** set of children's names */
  private Set<String> childrenNames = Sets.newHashSet();

  public DirectoryProperty(final int index, final byte [] array) {
    super(index, array);
  }

  @Override
  public boolean isDirectory() {
    return true;
  }

  @Override
  public int getChildIndex() {
    return super.getChildIndex();
  }

  /**
   * Get an iterator over the children of this Parent; all elements
   * are instances of Property.
   *
   * @return Iterator of children; may refer to an empty collection
   */
  public Iterator<Property> getChildren() {
    return children.iterator();
  }

  /**
   * Add a new child to the collection of children
   *
   * @param property the new child to be added; must not be null
   *
   * @exception IllegalStateException if we already have a child with the same name
   */
  public void addChild(final Property property) {
    String name = property.getName();
    Preconditions.checkState(!childrenNames.contains(name), "Duplicate name \"" + name + "\"");
    childrenNames.add(name);
    children.add(property);
  }
}
