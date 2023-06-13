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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.ValueVector;

import com.dremio.exec.record.SimpleVectorWrapper;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;

public class VectorContainerList {
  private final Map<Integer, List<VectorWrapper<?>>> wrapperIdToWrappers = new HashMap<>();
  private final int recordCount;

  VectorContainerList(List<VectorContainer> containers) {
    int count = 0;
    for (VectorContainer current : containers) {
      count += current.getRecordCount();

      int wrapperIdx = 0;
      for (VectorWrapper<?> wrapper : current) {
        List<VectorWrapper<?>> oldList = wrapperIdToWrappers.computeIfAbsent(wrapperIdx, i -> new ArrayList<>());
        oldList.add(wrapper);
        ++wrapperIdx;
      }
    }
    recordCount = count;
  }

  /**
   * When there is only one list of vectors of same types, this constructor can be used.
   * For a single list, only one wrapper index is needed.
   * @param singleList
   * @param index
   */
  public VectorContainerList(final List<? extends ValueVector> singleList, final int index) {
    final List<VectorWrapper<?>> wrapperList = new ArrayList<>();

    int count = 0;
    for (final ValueVector v : singleList) {
      final VectorWrapper wrap = new SimpleVectorWrapper(v);
      wrapperList.add(wrap);
      count += v.getValueCount();
    }
    wrapperIdToWrappers.put(index, wrapperList);

    this.recordCount = count;
  }

  List<VectorWrapper<?>> getWrappers(int wrapperIdx) {
    return wrapperIdToWrappers.get(wrapperIdx);
  }

  int getRecordCount() {
    return recordCount;
  }

}
