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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.dremio.common.AutoCloseables;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test the PagePool */
public class TestPagePool {

  private BufferAllocator allocator;
  private PagePool pool;

  @Before
  public void before() {
    this.allocator = new RootAllocator(70 * 1024);
    this.pool = new PagePool(allocator, 1024, 64);
    pool.start();
  }

  @After
  public void after() throws Exception {
    AutoCloseables.close(pool, allocator);
  }

  @Test
  public void initialState() {
    assertEquals(64, pool.getPageCount());
    assertEquals(0, pool.getUsedPageCount());
  }

  @Test
  public void overPrealloc() throws Exception {
    List<AutoCloseable> c1 = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      c1.add(pool.newPage());
    }

    List<AutoCloseable> c2 = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      c2.add(pool.newPage());
    }

    Page p1 = pool.newPage();
    Page p2 = pool.newPage();

    assertEquals(66, pool.getPageCount());
    assertEquals(66, pool.getUsedPageCount());

    p1.close();

    assertEquals(66, pool.getPageCount());
    assertEquals(65, pool.getUsedPageCount());

    pool.releaseUnusedToMinimum();

    assertEquals(65, pool.getPageCount());
    assertEquals(65, pool.getUsedPageCount());

    p2.close();

    // closing page decrements use count but maintains page count.
    assertEquals(65, pool.getPageCount());
    assertEquals(64, pool.getUsedPageCount());

    pool.releaseUnusedToMinimum();

    // release over reserve amount.
    assertEquals(64, pool.getPageCount());
    assertEquals(64, pool.getUsedPageCount());

    AutoCloseables.close(c2);

    // closing releasing pages back to general pool
    assertEquals(64, pool.getPageCount());
    assertEquals(32, pool.getUsedPageCount());

    pool.releaseUnusedToMinimum();

    // no change since below minimum
    assertEquals(64, pool.getPageCount());
    assertEquals(32, pool.getUsedPageCount());

    AutoCloseables.close(c1);

    // no change since below minimum
    assertEquals(64, pool.getPageCount());
    assertEquals(0, pool.getUsedPageCount());
  }

  @Test
  public void allocMulti() throws Exception {
    List<Page> pages = pool.getPages(64);
    assertEquals(64, pool.getPageCount());
    assertEquals(64, pool.getUsedPageCount());

    AutoCloseables.close(pages);
    pool.releaseUnusedToMinimum();

    assertEquals(64, pool.getPageCount());
    assertEquals(0, pool.getUsedPageCount());
  }

  @Test
  public void allocMultiNeg() throws Exception {
    List<Page> pages = pool.getPages(64);
    assertEquals(64, pool.getPageCount());
    assertEquals(64, pool.getUsedPageCount());

    // should fail since the limit is 70.
    List<Page> pagesExtra = pool.getPages(64);
    assertNull(pagesExtra);

    // no change in used list, pool can go upto the limit
    assertEquals(70, pool.getPageCount());
    assertEquals(64, pool.getUsedPageCount());

    AutoCloseables.close(pages);
    pool.releaseUnusedToMinimum();

    assertEquals(64, pool.getPageCount());
    assertEquals(0, pool.getUsedPageCount());
  }
}
