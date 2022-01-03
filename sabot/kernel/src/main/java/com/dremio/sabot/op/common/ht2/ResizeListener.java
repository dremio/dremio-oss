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
package com.dremio.sabot.op.common.ht2;

import java.util.concurrent.TimeUnit;

public interface ResizeListener {
  public static ResizeListener NO_OP = new ResizeListener() {
    public void addBatch() {}

    @Override
    public void resetToMinimumSize() throws Exception {}

    @Override
    public void revertResize() {}

    @Override
    public void commitResize() {}

    @Override
    public void verifyBatchCount(int batches) { }

    @Override
    public void releaseBatch(final int batchIdx) { }

    @Override
    public void accumulate(final long memoryAddr, final int count,
                           final int bitsInChunk, final int chunkOffsetMask) {}
  };

  void addBatch() throws Exception;

  void resetToMinimumSize() throws Exception;

  void revertResize();

  void commitResize();

  void verifyBatchCount(int batches);

  void releaseBatch(final int batchIdx);

  default boolean hasSpace(final int space, final int batchIndex) {
    return true;
  }

  public void accumulate(final long memoryAddr, final int count,
                         final int bitsInChunk, final int chunkOffsetMask);

  default int getAccumCompactionCount() {
    return 0;
  }

  default long getAccumCompactionTime(TimeUnit unit) {
    return 0;
  }
}
