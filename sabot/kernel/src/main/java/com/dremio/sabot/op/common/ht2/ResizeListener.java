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
  };

  void addBatch() throws Exception;

  void resetToMinimumSize() throws Exception;

  void revertResize();

  void commitResize();

  void verifyBatchCount(int batches);

  void releaseBatch(final int batchIdx);
}
