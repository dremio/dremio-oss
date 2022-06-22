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

package org.apache.arrow.vector;


import org.apache.arrow.memory.BufferAllocator;

public final class SimpleIntVector extends AbstractVector implements AutoCloseable {
  private static final int TYPE_WIDTH = 4;

  public SimpleIntVector(String name, BufferAllocator allocator) {
    super(name, allocator, TYPE_WIDTH);
  }

  public void set(int index, int value) {
    dataBuffer.setInt(index * TYPE_WIDTH, value);
  }

  public void setSafe(int index, int value) {
    while(index >= getValueCapacity()) {
      reAlloc();
    }
    dataBuffer.setInt(index * TYPE_WIDTH, value);
  }

  public void setZero(int index, int length) {
    dataBuffer.setZero(index * TYPE_WIDTH, length * TYPE_WIDTH);
  }

  public int get(int index) {
    return dataBuffer.getInt(index * TYPE_WIDTH);
  }

  public Integer getObject(int index) {
    return get(index);
  }
}
