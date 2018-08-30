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
package com.dremio.sabot.op.sort.external;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.selection.SelectionVector4;

public class Sv4HyperContainer extends ExpandableHyperContainer {

  private SelectionVector4 sv4;

  public Sv4HyperContainer(BufferAllocator allocator, Schema schema) {
    super(allocator, schema);
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  public void setSelectionVector4(SelectionVector4 sv4){
    this.sv4 = sv4;
  }

  @Override
  public void close() {
    if(sv4 != null){
      sv4.clear();
    }
    super.close();
  }
}
