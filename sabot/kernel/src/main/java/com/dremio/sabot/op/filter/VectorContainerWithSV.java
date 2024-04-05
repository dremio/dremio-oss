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
package com.dremio.sabot.op.filter;

import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import org.apache.arrow.memory.BufferAllocator;

/** VectorContainers with sv2 (filters). */
public class VectorContainerWithSV extends VectorContainer {
  private final SelectionVector2 sv2;

  public VectorContainerWithSV(BufferAllocator allocator, SelectionVector2 sv2) {
    super(allocator);
    this.sv2 = sv2;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv2;
  }

  @Override
  public void close() {
    sv2.clear();
    super.close();
  }
}
