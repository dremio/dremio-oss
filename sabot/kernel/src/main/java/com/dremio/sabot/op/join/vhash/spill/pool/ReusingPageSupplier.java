package com.dremio.sabot.op.join.vhash.spill.pool;

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
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.RoundUtil;

/**
 * Page supplier that tries to re-use the page until the space is exhausted.
 */
public class ReusingPageSupplier implements PageSupplier, AutoCloseable {
  private final PagePool pool;
  private Page current;
  private int currentRemaining;

  public ReusingPageSupplier(PagePool pool) {
    this.pool = pool;
  }

  @Override
  public BufferAllocator getAllocator() {
    return pool.getAllocator();
  }

  public Page getPage(int size) {
    // round-up so that vector/buffer allocations happen at 8-byte alignments.
    size = RoundUtil.round8up(size);

    if (current == null || currentRemaining < size) {
      AutoCloseables.closeNoChecked(current);
      current = pool.newPage(); // ref to be released on close
      currentRemaining = current.getRemainingBytes();
    }
    currentRemaining -= size;
    return new PageSlice(current, size);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(current);
  }
}
