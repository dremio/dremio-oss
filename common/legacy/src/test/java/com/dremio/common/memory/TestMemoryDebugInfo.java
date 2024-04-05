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
package com.dremio.common.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.Rule;
import org.junit.Test;

public class TestMemoryDebugInfo extends DremioTest {

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void testWithFullNode() {
    try (BufferAllocator root =
            allocatorRule.newAllocator("test-memory-debug-info", 0, 1024 * 1024);
        BufferAllocator child = root.newChildAllocator("child", 0, 16 * 1024);
        BufferAllocator grandChild1 = child.newChildAllocator("grandchild1", 0, 64 * 1024);
        BufferAllocator grandChild2 = child.newChildAllocator("grandchild2", 0, 64 * 1024)) {

      // allocate from child till it gets full.
      try (ArrowBuf buf = grandChild2.buffer(32 * 1024)) {
        assertTrue("expected allocation above limit to fail", false); // should not reach here
      } catch (OutOfMemoryException e) {
        String info = MemoryDebugInfo.getDetailsOnAllocationFailure(e, grandChild2);

        // should have details about child, and two levels below that.
        assertTrue(info.contains("child"));
        assertTrue(info.contains("grandchild1"));
        assertTrue(info.contains("grandchild2"));

        // shouldn't have details about ROOT.
        assertFalse(info.contains("test-memory-debug-info"));
      }
    }
  }

  @Test
  public void testWithRoot() {
    try (BufferAllocator root =
            allocatorRule.newAllocator("test-memory-debug-info", 0, 1024 * 1024);
        BufferAllocator child = root.newChildAllocator("child", 0, Integer.MAX_VALUE);
        BufferAllocator grandChild1 = child.newChildAllocator("grandchild1", 0, Integer.MAX_VALUE);
        BufferAllocator grandChild2 =
            child.newChildAllocator("grandchild2", 0, Integer.MAX_VALUE)) {

      // allocate to hit the root limit.
      try (ArrowBuf buf = grandChild2.buffer(2 * 1024 * 1024)) {
        assertTrue("expected allocation above limit to fail", false); // should not reach here
      } catch (OutOfMemoryException e) {
        String info = MemoryDebugInfo.getDetailsOnAllocationFailure(e, grandChild2);

        // should print upto 6 levels below root.
        assertTrue(info.contains("test-memory-debug-info"));
        assertTrue(info.contains("child"));
        assertTrue(info.contains("grandchild1"));
        assertTrue(info.contains("grandchild2"));
      }
    }
  }

  /*
   * If there are lots of child allocators, test that that the smaller ones are pruned.
   */
  @Test
  public void testPrune() throws Exception {
    try (RollbackCloseable closeable = new RollbackCloseable(true)) {
      BufferAllocator root = allocatorRule.newAllocator("test-memory-debug-info", 0, 1024 * 1024);
      closeable.add(root);

      BufferAllocator twig = root.newChildAllocator("twig", 0, 1024 * 1024);
      closeable.add(twig);

      for (int i = 0; i < 20; ++i) {
        boolean isBig = (i % 2 == 0);
        BufferAllocator child =
            twig.newChildAllocator((isBig ? "big" : "small") + i, 0, Integer.MAX_VALUE);
        closeable.add(child);

        if (isBig) {
          closeable.add(child.buffer(8192));
        } else {
          closeable.add(child.buffer(4096));
        }
      }

      // allocate to hit the root limit.
      try (ArrowBuf buf = twig.buffer(1024 * 1024)) {
        assertTrue("expected allocation above limit to fail", false); // should not reach here
      } catch (OutOfMemoryException e) {
        String info = MemoryDebugInfo.getDetailsOnAllocationFailure(e, twig);

        assertTrue(!info.contains("test-memory-debug-info"));
        assertTrue(info.contains("twig"));
        assertTrue(info.contains("big"));
        assertTrue(!info.contains("small"));
      }
    }
  }
}
