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

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

import io.netty.buffer.ArrowBuf;

/**
 * Unit test for the DremioRootAllocator
 */
public class TestRootAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRootAllocator.class);

  private DremioRootAllocator rootAllocator;

  @Before
  public void setup() {
    rootAllocator = DremioRootAllocator.create(16 * 1024, 5);
  }

  @After
  public void cleanup() {
    rootAllocator.close();
  }

  @Rule public final ExpectedException thrownException = ExpectedException.none();

  private ArrowBuf allocateHelper(BufferAllocator alloc, final int requestSize) throws Exception{
    try {
      return alloc.buffer(requestSize);
    } catch (OutOfMemoryException e) {
      throw UserException
        .memoryError(e)
        .addContext(MemoryDebugInfo.getDetailsOnAllocationFailure(e, alloc))
        .build(logger);
    }
  }

  /**
   * Root, plus single-level children. Allocation fails with child limit
   */
  @Test
  public void testRootWithChildrenLimit() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "Query was cancelled because it exceeded the memory limits set by the administrator.",
      "Allocator(child1)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 4 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 8 * 1024)) {
      allocateHelper(child1, 8 * 1024);
    }
  }

  @Test
  public void failOnMax() throws Exception {
    try(RollbackCloseable closeables = new RollbackCloseable(true)) {
      BufferAllocator alloc = closeables.add(this.rootAllocator.newChildAllocator("child", 0, Long.MAX_VALUE));
      closeables.add(alloc.buffer(1));

      // make sure release works
      alloc.buffer(1).release();
      closeables.add(alloc.buffer(1));
      closeables.add(alloc.buffer(1));
      closeables.add(alloc.buffer(1));
      closeables.add(alloc.buffer(1));

      // ensure
      thrownException.expect(OutOfMemoryException.class);
      closeables.add(alloc.buffer(1));
    }

    assertEquals(5l, rootAllocator.getAvailableBuffers());

  }

  @Test
  public void ensureZeroAfterUse() throws Exception {
    try(RollbackCloseable closeables = new RollbackCloseable(true)) {
      BufferAllocator alloc = closeables.add(this.rootAllocator.newChildAllocator("child", 0, Long.MAX_VALUE));
      closeables.add(alloc.buffer(1));
    }
    assertEquals(5l, rootAllocator.getAvailableBuffers());
  }

  @Test
  public void ensureZeroAfterFailedAlloc() throws Exception {
    try(RollbackCloseable closeables = new RollbackCloseable(true)) {
      BufferAllocator alloc = closeables.add(this.rootAllocator.newChildAllocator("child", 0, 1));
      try {
        closeables.add(alloc.buffer(2));
      } catch (OutOfMemoryException ex) {
        // ignore.
      }
    }
    assertEquals(5l, rootAllocator.getAvailableBuffers());
  }

  /**
   * Root, plus single-level children. Allocation fails with root limit
   */
  @Test
  public void testRootWithChildrenSize() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "Query was cancelled because it exceeded the memory limits set by the administrator.",
      "Allocator(ROOT)", "Allocator(child1)", "Allocator(child2)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 16 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 16 * 1024);
         ArrowBuf buf1 = allocateHelper(child1,8 * 1024)) {
      allocateHelper(child2, 16 * 1024);
    }
  }

  /**
   * Root, plus two levels of children
   */
  @Test
  public void testRootWithGrandchildren() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "Query was cancelled because it exceeded the memory limits set by the administrator.",
      "Allocator(ROOT)", "Allocator(child1)", "Allocator(child2)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 32 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 32 * 1024)) {
      try (BufferAllocator child11 = child1.newChildAllocator("child11", 0, 32 * 1024);
           BufferAllocator child21 = child2.newChildAllocator("child21", 0, 32 * 1024)) {
          allocateHelper(child21, 32 * 1024);
      }
    }
  }
}
