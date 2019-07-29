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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import com.dremio.common.AutoCloseables;

public class NestedAccumulatorNoSpill implements AccumulatorNoSpill {
  private final AccumulatorNoSpill[] children;

  public NestedAccumulatorNoSpill(AccumulatorNoSpill... children) {
    super();
    this.children = children;
  }

  @Override
  public void resized(int newCapacity) {
    for(AccumulatorNoSpill a : children){
      a.resized(newCapacity);
    }
  }

  @Override
  public void accumulate(final long offsetAddr, final int count) {
    for(AccumulatorNoSpill a : children){
      a.accumulate(offsetAddr, count);
    }
  }

  @Override
  public void output(int batchIndex) {
    for(AccumulatorNoSpill a : children){
      a.output(batchIndex);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(children);
  }
}
