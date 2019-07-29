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
package com.dremio.exec.record.vector;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.Test;

import com.dremio.exec.ExecTest;

public class TestAllocate extends ExecTest {

  @Test
  public void testAllocateFixedNew() {
    try (BufferAllocator child = allocator.newChildAllocator("test", 0, 4 * 1024)) {
      try (final ValueVector vector = new IntVector("ints", child)) {
        vector.allocateNew();
        fail("expected allocation to fail");
      }
    } catch (Exception e) {
      assertTrue(e instanceof OutOfMemoryException);
      assertTrue(((OutOfMemoryException)e).getOutcomeDetails().isPresent());
    }
  }

  @Test
  public void testAllocateFixedReAlloc() {
    boolean allocatePassed = false;

    try (BufferAllocator child = allocator.newChildAllocator("test", 0, 128 * 1024)) {
      try (final ValueVector vector = new IntVector("ints", child)) {
        vector.allocateNew();
        allocatePassed = true;

        // realloc will fail after some iterations.
        while (true) {
          vector.reAlloc();
        }
      }
    } catch (Exception e) {
      assertTrue(e instanceof OutOfMemoryException);
      assertTrue(((OutOfMemoryException)e).getOutcomeDetails().isPresent());
      assertTrue(allocatePassed);
    }
  }

  @Test
  public void testAllocateVarLenNew() {
    try (BufferAllocator child = allocator.newChildAllocator("test", 0, 4 * 1024)) {
      try (final ValueVector vector = new VarCharVector("varchar", child)) {
        vector.allocateNew();
        fail("expected allocation to fail");
      }
    } catch (Exception e) {
      assertTrue(e instanceof OutOfMemoryException);
      assertTrue(((OutOfMemoryException)e).getOutcomeDetails().isPresent());
    }
  }

  @Test
  public void testAllocateVarLenReAlloc() {
    boolean allocatePassed = false;

    try (BufferAllocator child = allocator.newChildAllocator("test", 0, 128 * 1024)) {
      try (final ValueVector vector = new VarCharVector("ints", child)) {
        vector.allocateNew();
        allocatePassed = true;

        // realloc will fail after some iterations.
        while (true) {
          vector.reAlloc();
        }
      }
    } catch (Exception e) {
      assertTrue(e instanceof OutOfMemoryException);
      assertTrue(((OutOfMemoryException)e).getOutcomeDetails().isPresent());
      assertTrue(allocatePassed);
    }
  }
}
