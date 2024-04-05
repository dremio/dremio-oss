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
package com.dremio.exec.util;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.lucene.util.InPlaceMergeSorter;

/** In place merge sorter for fixed length block values. Intended for small value sets only. */
public class ArrowInPlaceMergeSorter extends InPlaceMergeSorter {
  private final ArrowBuf arrowBuf;
  private final int blockSize;
  private final ArrowCrossBufComparator comparator;

  public ArrowInPlaceMergeSorter(
      final ArrowBuf arrowBuf, final int blockSize, final ArrowCrossBufComparator comparator) {
    this.arrowBuf = arrowBuf;
    this.blockSize = blockSize;
    this.comparator = comparator;
  }

  @Override
  protected int compare(int idx1, int idx2) {
    return comparator.compare(arrowBuf, idx1, arrowBuf, idx2);
  }

  @Override
  protected void swap(int idx1, int idx2) {
    byte[] val1 = new byte[blockSize];
    byte[] val2 = new byte[blockSize];

    arrowBuf.getBytes(idx2 * blockSize, val2);
    arrowBuf.getBytes(idx1 * blockSize, val1);

    arrowBuf.setBytes(idx1 * blockSize, val2);
    arrowBuf.setBytes(idx2 * blockSize, val1);
  }
}
