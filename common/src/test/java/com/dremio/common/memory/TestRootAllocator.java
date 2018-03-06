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
package com.dremio.common.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    rootAllocator = new DremioRootAllocator(16 * 1024);
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
      UserException.Builder b = UserException.memoryError(e);
      rootAllocator.addUsageToExceptionContext(b);
      throw b.build(logger);
    }
  }

  /**
   * Root-only, no allocations
   */
  @Test
  public void testRootSolo() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "out of memory",
      "Allocator(ROOT)"
    ));
    allocateHelper(rootAllocator, 32 * 1024);
  }

  /**
   * Root-only, a few allocations
   */
  @Test
  public void testRootSoloWithAllocs() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "out of memory",
      "Allocator(ROOT)"
    ));
    try(ArrowBuf buf1 = allocateHelper(rootAllocator, 1024)) {
      allocateHelper(rootAllocator, 32 * 1024);
    }
  }

  /**
   * Root, plus single-level children. Allocation fails with child limit
   */
  @Test
  public void testRootWithChildrenLimit() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "out of memory",
      "Allocator(ROOT)", "Allocator(child1)", "Allocator(child2)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 4 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 8 * 1024)) {
      allocateHelper(child1, 8 * 1024);
    }
  }

  /**
   * Root, plus single-level children. Allocation fails with root limit
   */
  @Test
  public void testRootWithChildrenSize() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "out of memory",
      "Allocator(ROOT)", "Allocator(child1)", "Allocator(child2)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 12 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 12 * 1024);
         ArrowBuf buf1 = allocateHelper(child1,8 * 1024)) {
      allocateHelper(child2, 10 * 1024);
    }
  }

  /**
   * Root, plus two levels of children
   */
  @Test
  public void testRootWithGrandchildren() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
      "out of memory",
      "Allocator(ROOT)", "Allocator(child1)", "Allocator(child2)"));
    try (BufferAllocator child1 = rootAllocator.newChildAllocator("child1", 0, 12 * 1024);
         BufferAllocator child2 = rootAllocator.newChildAllocator("child2", 0, 12 * 1024)) {
      try (BufferAllocator child11 = child1.newChildAllocator("child11", 0, 8 * 1024);
           BufferAllocator child21 = child2.newChildAllocator("child21", 0, 8 * 1024)) {
          allocateHelper(child21, 10 * 1024);
      }
    }
  }
}
