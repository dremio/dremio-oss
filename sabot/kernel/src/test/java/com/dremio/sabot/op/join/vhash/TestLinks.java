/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.join.vhash;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.memory.DremioRootAllocator;

import io.netty.util.internal.PlatformDependent;

/**
 * Unit test for {@link JoinLinks}, and {@link JoinLinksReusePool}
 */
public class TestLinks {
  private static final int TEST_MEM_SIZE = 1 * 1024 * 1024;
  private static final int TEST_BUF_NUM_RECORDS = 1024;
  private static final int TEST_INITIAL_POOL_BUFS = 5;

  private DremioRootAllocator rootAllocator;
  private BufferAllocator alloc;
  private JoinLinksReusePool reusePool;
  private JoinLinksFactory linksFactory;

  @Before
  public void setup() {
    rootAllocator = DremioRootAllocator.create(TEST_MEM_SIZE);
    alloc = rootAllocator.newChildAllocator("testLinks", 0, TEST_MEM_SIZE);
    reusePool = new JoinLinksReusePool(TEST_BUF_NUM_RECORDS, TEST_INITIAL_POOL_BUFS, alloc);
    linksFactory = new JoinLinksFactory(reusePool, alloc);
  }

  @After
  public void cleanup() throws Exception {
    reusePool.close();
    alloc.close();
    rootAllocator.close();
  }

  // single-buffer links
  @Test
  public void testSingle() throws Exception {
    testLinksHelper(TEST_BUF_NUM_RECORDS, false, false);
    assertEquals(TEST_INITIAL_POOL_BUFS, reusePool.getNumBuffers());
  }

  // multi-buffer links
  @Test
  public void testMulti() throws Exception {
    testLinksHelper(4 * TEST_BUF_NUM_RECORDS, false, false);
    assertEquals(TEST_INITIAL_POOL_BUFS, reusePool.getNumBuffers());
  }

  // odd-numbered buffers
  @Test
  public void testOddNumbered() throws Exception {
    testLinksHelper(3 * TEST_BUF_NUM_RECORDS / 2, false, false);
    assertEquals(TEST_INITIAL_POOL_BUFS, reusePool.getNumBuffers());
    testLinksHelper(3 * TEST_BUF_NUM_RECORDS / 2, true, false);
    assertEquals(TEST_INITIAL_POOL_BUFS - 2, reusePool.getNumBuffers());
  }

  // reuse some of the initial buffers
  @Test
  public void testReuseInitial() throws Exception {
    testLinksHelper(TEST_BUF_NUM_RECORDS, true, false);
    assertEquals(TEST_INITIAL_POOL_BUFS - 1, reusePool.getNumBuffers());
    testLinksHelper((TEST_INITIAL_POOL_BUFS - 1) * TEST_BUF_NUM_RECORDS, true, false);
    assertEquals(0, reusePool.getNumBuffers());
  }

  // allocate, free, then reuse the initial buffers
  @Test
  public void testReuse() throws Exception {
    testLinksHelper(3 * TEST_BUF_NUM_RECORDS, false, true);
    assertEquals(TEST_INITIAL_POOL_BUFS + 3, reusePool.getNumBuffers());
    for (int i = 0; i < TEST_INITIAL_POOL_BUFS + 3; i++) {
      testLinksHelper(TEST_BUF_NUM_RECORDS, true, false);
      assertEquals(TEST_INITIAL_POOL_BUFS + 3 - (i + 1), reusePool.getNumBuffers());
    }

    // Attempting to reset to a higher number will be a no-op
    assertEquals(0, reusePool.getNumBuffers());
    reusePool.reset(TEST_INITIAL_POOL_BUFS);
    assertEquals(0, reusePool.getNumBuffers());

    // Now, let's repopulate with single bufs and reuse a multibuf
    for (int i = 0; i < TEST_INITIAL_POOL_BUFS + 3; i++) {
      testLinksHelper(TEST_BUF_NUM_RECORDS, false, true);
    }
    // resetting to a lower number should free some bufs
    assertEquals(TEST_INITIAL_POOL_BUFS + 3, reusePool.getNumBuffers());
    reusePool.reset(TEST_INITIAL_POOL_BUFS);
    assertEquals(TEST_INITIAL_POOL_BUFS, reusePool.getNumBuffers());

    // Now try to reuse past the number of reuse buffers (should get freshly allocated ones)
    testLinksHelper(TEST_BUF_NUM_RECORDS * (TEST_INITIAL_POOL_BUFS + 3), true, false);
    assertEquals(0, reusePool.getNumBuffers());
  }

  private int batchForRecord(int n) {
    return 1000 + n / 10;
  }
  private short offsetForRecord(int n) {
    return (short)(n % 10);
  }

  // Allocate a JoinLinks of a given size, populate it, then test whether it contains what we said it does
  private void testLinksHelper(final int numRecords, final boolean useReusedBufs, final boolean reuseAtFree) throws Exception {
    JoinLinks links = linksFactory.makeLinks(numRecords, useReusedBufs);
    // i-th element has batch number: 1000 + i / 10; offset number: i%10
    for (int i = 0; i < numRecords; i++) {
      final long addr = links.linkMemoryAddress(i);
      assertEquals(JoinLinks.INDEX_EMPTY, PlatformDependent.getInt(addr));
      PlatformDependent.putInt(addr, batchForRecord(i));
      PlatformDependent.putShort(addr + 4, offsetForRecord(i));
    }

    for (int i = 0; i < numRecords; i++) {
      final long addr = links.linkMemoryAddress(i);
      assertEquals(batchForRecord(i), PlatformDependent.getInt(addr));
      assertEquals(offsetForRecord(i), PlatformDependent.getShort(addr + 4));
    }

    if (reuseAtFree) {
      links.reuse();
    }
    links.close();
  }

}
