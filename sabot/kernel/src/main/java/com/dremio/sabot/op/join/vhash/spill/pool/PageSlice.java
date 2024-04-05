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
package com.dremio.sabot.op.join.vhash.spill.pool;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.RoundUtil;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;

/** Wrapper over page that enforces a cap on the usage. */
public class PageSlice implements Page {
  private final Page inner;
  private final ArrowBuf slicedBuf;
  private int used;

  public PageSlice(Page page, int cap) {
    this.inner = page;
    // All future allocations in the page will be from the slicedBuf.
    this.slicedBuf = page.slice(cap);
    page.retain();
  }

  @Override
  public int getPageSize() {
    return inner.getPageSize();
  }

  @Override
  public long getAddress() {
    return inner.getAddress();
  }

  @Override
  public ArrowBuf getBackingBuf() {
    return inner.getBackingBuf();
  }

  @Override
  public ArrowBuf slice(int size) {
    final ArrowBuf buf = slicedBuf.slice(used, size).writerIndex(0);
    buf.getReferenceManager().retain();
    used += size;
    return buf;
  }

  @Override
  public ArrowBuf sliceAligned(int size) {
    if (size > 0) {
      int alignedOffset = RoundUtil.round8up(used);
      if (alignedOffset > used) {
        deadSlice(alignedOffset - used);
      }
    }
    return slice(size);
  }

  @Override
  public void deadSlice(int size) {
    slicedBuf.slice(used, size);
    used += size;
  }

  @Override
  public int getRemainingBytes() {
    return LargeMemoryUtil.checkedCastToInt(slicedBuf.capacity() - used);
  }

  @Override
  public void retain() {
    inner.retain();
  }

  @Override
  public void release() {
    inner.release();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(slicedBuf, inner);
  }
}
