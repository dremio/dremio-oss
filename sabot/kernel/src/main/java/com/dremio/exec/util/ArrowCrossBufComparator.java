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

/** For making comparisons across different buffers. */
public interface ArrowCrossBufComparator {

  /**
   * Compares item at idx1 at buf1 and idx2 at buf2. The index is not the block position.
   *
   * @param buf1 Buffer 1
   * @param idx1 Index against buffer 1
   * @param buf2 Buffer 2
   * @param idx2 Index against buffer 2
   * @return less than 0 if buf1:idx1 is smaller, greater than 0 if buf1:idx1 is larger, 0 if both
   *     are equal.
   */
  int compare(ArrowBuf buf1, int idx1, ArrowBuf buf2, int idx2);
}
