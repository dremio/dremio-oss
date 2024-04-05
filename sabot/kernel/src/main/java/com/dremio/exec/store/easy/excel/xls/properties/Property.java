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
package com.dremio.exec.store.easy.excel.xls.properties;

import org.apache.poi.util.IntegerField;
import org.apache.poi.util.LittleEndianConsts;
import org.apache.poi.util.ShortField;

/**
 * This abstract base class is the ancestor of all classes implementing POIFS Property behavior.
 *
 * <p>Simplified implementation of {@link org.apache.poi.poifs.property.Property}
 */
public abstract class Property {
  private static final int _name_size_offset = 0x40;

  // useful offsets
  private static final int _previous_property_offset = 0x44;
  private static final int _next_property_offset = 0x48;
  private static final int _child_property_offset = 0x4C;
  private static final int _start_block_offset = 0x74;
  private static final int _size_offset = 0x78;

  private String _name;
  private IntegerField _previous_property;
  private IntegerField _next_property;
  private IntegerField _child_property;
  private IntegerField _start_block;
  private IntegerField _size;
  private int _index;

  /**
   * Constructor from byte data
   *
   * @param index index number
   * @param array byte data
   */
  protected Property(int index, byte[] array) {
    short _name_size = new ShortField(_name_size_offset, array).get();
    _previous_property = new IntegerField(_previous_property_offset, array);
    _next_property = new IntegerField(_next_property_offset, array);
    _child_property = new IntegerField(_child_property_offset, array);
    _start_block = new IntegerField(_start_block_offset, array);
    _size = new IntegerField(_size_offset, array);
    _index = index;
    int name_length = (_name_size / LittleEndianConsts.SHORT_SIZE) - 1;

    if (name_length < 1) {
      _name = "";
    } else {
      char[] char_array = new char[name_length];
      int name_offset = 0;

      for (int j = 0; j < name_length; j++) {
        char_array[j] = (char) new ShortField(name_offset, array).get();
        name_offset += LittleEndianConsts.SHORT_SIZE;
      }
      _name = new String(char_array, 0, name_length);
    }
  }

  /**
   * @return the start block
   */
  public int getStartBlock() {
    return _start_block.get();
  }

  /**
   * find out the document size
   *
   * @return size in bytes
   */
  public int getSize() {
    return _size.get();
  }

  /**
   * Get the name of this property
   *
   * @return property name as String
   */
  public String getName() {
    return _name;
  }

  /**
   * @return true if a directory type Property
   */
  public abstract boolean isDirectory();

  /**
   * Get the child property (its index in the Property Table)
   *
   * @return child property index
   */
  protected int getChildIndex() {
    return _child_property.get();
  }

  /**
   * Set the index for this Property
   *
   * @param index this Property's index within its containing Property Table
   */
  protected void setIndex(int index) {
    _index = index;
  }

  /**
   * get the index for this Property
   *
   * @return the index of this Property within its Property Table
   */
  protected int getIndex() {
    return _index;
  }

  /**
   * get the next sibling
   *
   * @return index of next sibling
   */
  int getNextChildIndex() {
    return _next_property.get();
  }

  /**
   * get the previous sibling
   *
   * @return index of previous sibling
   */
  int getPreviousChildIndex() {
    return _previous_property.get();
  }
}
