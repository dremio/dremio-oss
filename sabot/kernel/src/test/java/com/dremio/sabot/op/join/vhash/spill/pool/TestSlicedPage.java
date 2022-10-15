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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;

/**
 * Test SlicedPage.
 */
public class TestSlicedPage {
  private BufferAllocator allocator;

  @Before
  public void before() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void after() throws Exception {
    AutoCloseables.close(allocator);
  }

  @Test
  public void slicePos() throws Exception {
    PageImpl p = new PageImpl(16, allocator.buffer(16), buf -> {});
    p.initialRetain();

    try (Page capped = new PageSlice(p, 9);
         ArrowBuf b1 = capped.slice(4);
         ArrowBuf b2 = capped.slice(5)) {
      // test for no exceptions
    }
    p.close();
    p.deallocate();
  }

  @Test
  public void sliceNeg() throws Exception {
    PageImpl p = new PageImpl(16, allocator.buffer(16), buf -> {});
    p.initialRetain();

    try (Page capped = new PageSlice(p, 9);
         ArrowBuf b1 = capped.slice(4)) {
      // slicing beyond the cap should throw an exception
      assertThatThrownBy(() -> capped.slice(6))
        .isInstanceOf(IndexOutOfBoundsException.class);
    } finally {
      p.close();
      p.deallocate();
    }
  }

  @Test
  public void deadSlicePos() throws Exception {
    PageImpl p = new PageImpl(16, allocator.buffer(16), buf -> {});
    p.initialRetain();

    try (Page capped = new PageSlice(p, 9);
         ArrowBuf b1 = capped.slice(4)) {
         capped.deadSlice(5);
    }
    p.close();
    p.deallocate();
  }

  @Test
  public void deadSliceNeg() throws Exception {
    PageImpl p = new PageImpl(16, allocator.buffer(16), buf -> {});
    p.initialRetain();

    try (Page capped = new PageSlice(p, 9);
         ArrowBuf b1 = capped.slice(4)) {
      // slicing beyond the cap should throw an exception
      assertThatThrownBy(() -> capped.deadSlice(6))
        .isInstanceOf(IndexOutOfBoundsException.class);
    } finally {
      p.close();
      p.deallocate();
    }
  }

  @Test
  public void sliceAligned() throws Exception {
    PageImpl p = new PageImpl(32, allocator.buffer(32), buf -> {});
    p.initialRetain();

    try (Page capped = new PageSlice(p, 16);
         ArrowBuf b1 = capped.slice(1);
         ArrowBuf b2 = capped.sliceAligned(8)) {
      assertEquals(0, capped.getRemainingBytes());
    }
    p.close();
    p.deallocate();
  }
}
