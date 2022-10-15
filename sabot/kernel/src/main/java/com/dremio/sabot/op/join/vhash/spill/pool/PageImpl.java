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

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.common.HistoricalLog;
import com.dremio.common.VM;
import com.dremio.exec.util.RoundUtil;

/**
 * Implementation for Page.
 */
@NotThreadSafe
public class PageImpl implements Page {
  public static final boolean DEBUG = VM.areAssertsEnabled();
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PageImpl.class);
  private static final AtomicLong idCounter = new AtomicLong(0);

  private final int pageSize;
  private final PagePool.Release release;
  private final ArrowBuf memory;
  private final long id;
  private int offset = 0;
  private int referenceCount = 0;
  // debugging only (helps catch ref leaks)
  private final HistoricalLog historicalLog;

  static {
    logger.info("Debug mode {}", DEBUG ? "enabled." : "disabled.");
  }

  PageImpl(int pageSize, ArrowBuf memory, PagePool.Release release) {
    this.pageSize = pageSize;
    this.memory = memory;
    this.release = release;
    this.id = idCounter.incrementAndGet();
    this.historicalLog = DEBUG ? new HistoricalLog("Page[%d]", id) : null;
  }

  @Override
  public int getPageSize() {
    checkHasReferences();
    return pageSize;
  }

  @Override
  public long getAddress() {
    checkHasReferences();
    return memory.memoryAddress();
  }

  @Override
  public ArrowBuf getBackingBuf() {
    return memory;
  }

  @Override
  public ArrowBuf slice(int size) {
    checkHasReferences();
    if (size + offset > pageSize) {
      throw new IllegalArgumentException(String.format("Attempting to slice beyond limit. Desired size: %d, available space: %d.", size, pageSize - offset));
    }
    final ArrowBuf buf = memory.slice(offset, size).writerIndex(0);
    memory.getReferenceManager().retain();
    offset += size;
    return buf;
  }

  @Override
  public ArrowBuf sliceAligned(int size) {
    if (size > 0) {
      int alignedOffset = RoundUtil.round8up(offset);
      if (alignedOffset > offset) {
        deadSlice(alignedOffset - offset);
      }
    }
    return slice(size);
  }

  @Override
  public void deadSlice(int size) {
    checkHasReferences();
    slice(size).close();
  }

  @Override
  public int getRemainingBytes() {
    checkHasReferences();
    return pageSize - offset;
  }

  private void checkHasReferences() {
    if (referenceCount == 0) {
      throw new IllegalStateException("Operation not allowed after page was closed.");
    }
  }

  private void checkNoReferences() {
    if (referenceCount > 0) {
      throw new IllegalStateException("Operation not allowed until page is closed.");
    }
    if (memory.refCnt() != 1) {
      throw new IllegalStateException("Unexpected refCnt on page buffer, expected 1 and found " + memory.refCnt());
    }
  }

  /**
   * Release the memory of this page. Should only be done if this page has no pending references.
   */
  void deallocate() {
    checkNoReferences();
    memory.close();
  }

  /**
   * Generate a new version of this page to slice. Can only be done once this page
   * has been closed.
   *
   * @return The new page pointing to the same memory as this page.
   */
  PageImpl toNewPage() {
    checkNoReferences();
    return new PageImpl(pageSize, memory, release);
  }

  void initialRetain() {
    if (DEBUG) {
      historicalLog.recordEvent("initialRetain");
    }
    checkNoReferences();
    referenceCount++;
  }

  @Override
  public void retain() {
    if (DEBUG) {
      historicalLog.recordEvent("retain");
    }
    checkHasReferences();
    referenceCount++;
  }

  /**
   * Recycle the page back to the pool for future use. This does not release any
   * memory back to the system, only the pool.
   */
  @Override
  public void release() {
    checkHasReferences();
    if (DEBUG) {
      historicalLog.recordEvent("release");
    }

    referenceCount--;
    if (referenceCount == 0) {
      release.release(this);
    }
  }

  /**
   * Recycle the page back to the pool for future use. This does not release any
   * memory back to the system, only the pool.
   */
  @Override
  public void close() {
    release();
  }
}
