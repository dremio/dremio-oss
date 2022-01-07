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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.google.common.base.Preconditions;

/**
 * Provides a pool of equal sized memory pages.
 *
 * Not thread safe.
 */
@NotThreadSafe
public class PagePool implements AutoCloseable {

  // The default batch size is 1MB. Making this smaller than that value will cause a proportionate increase in the
  // heap footprint (ArrowBuf/ValueVector).
  private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;

  private enum State {NEW, INIT, CLOSED}
  private State state = State.NEW;
  private final int pageSize;
  private final int minimumCount;
  private final BufferAllocator allocator;
  private final Set<Page> pages = new HashSet<>();
  private final List<Page> unused = new ArrayList<>();

  private final Release releaser = page -> {
      Preconditions.checkArgument(pages.remove(page));
      Page p = page.toNewPage();
      pages.add(p);
      unused.add(p);
    };

  public PagePool(BufferAllocator allocator) {
    this(allocator, DEFAULT_PAGE_SIZE, 0);
  }

  public PagePool(BufferAllocator allocator, int pageSize, int minimumCount) {
    super();
    this.pageSize = pageSize;
    this.minimumCount = minimumCount;
    this.allocator = allocator.newChildAllocator("page-pool", pageSize * minimumCount, Long.MAX_VALUE);
  }

  public void start() {
    Preconditions.checkArgument(state == State.NEW);
    try(RollbackCloseable rb = new RollbackCloseable()){
      for (int i = 0; i < minimumCount; i++) {
        Page p = createNewPage();
        pages.add(rb.add(p));
        unused.add(p);
      }
      rb.commit();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    state = State.INIT;
  }

  public int getPageCount() {
    return pages.size();
  }

  public int getUsedPageCount() {
    return pages.size() - unused.size();
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public void releaseUnusedToMinimum() {
    while (pages.size() > minimumCount && !unused.isEmpty()) {
      Page p = unused.remove(unused.size() - 1);
      pages.remove(p);
      p.deallocate();
    }
  }

  public int getPageSize() {
    return pageSize;
  }

  /**
   * Get the requested number of pages. Returns null if all of the pages can't be allocated.
   * @param count
   * @return
   */
  public List<Page> getPages(int count) {
    try(RollbackCloseable rb = new RollbackCloseable()){
      List<Page> pages = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        Page p = newPage();
        pages.add(rb.add(p));
      }
      rb.commit();
      return pages;
    } catch (OutOfMemoryException e) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Page createNewPage() {
    return new Page(pageSize, allocator.buffer(pageSize), releaser);
  }

  public Page newPage() {
    if (!unused.isEmpty()) {
      Page p = unused.remove(unused.size() - 1);
      p.initialRetain();
      return p;
    }

    Page p = createNewPage();
    pages.add(p);
    p.initialRetain();
    return p;
  }

  public void close() {
    Preconditions.checkArgument(state != State.CLOSED);

    try {
      if (pages.size() > unused.size()) {
        throw new IllegalStateException("Some pages " +  (pages.size() - unused.size()) + " are still in use");
      }

      List<AutoCloseable> ac = Stream.concat(
        unused.stream().map(p -> {
          return (AutoCloseable) p::deallocate;
        }),
        Stream.of((AutoCloseable) allocator))
        .collect(Collectors.toList());
      AutoCloseables.close(ac);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      state = State.CLOSED;
    }
  }

  interface Release {
    void release(Page page);
  }

}
