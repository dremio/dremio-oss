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

import com.dremio.common.AutoCloseables;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test the Page system. */
public class TestPage {

  private BufferAllocator allocator;
  private PagePool pool;

  @Before
  public void before() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.pool = new PagePool(allocator, 1024, 0);
    pool.start();
  }

  @After
  public void after() throws Exception {
    AutoCloseables.close(pool, allocator);
  }

  @Test
  public void deallocNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    // can't release when open.
    assertThatThrownBy(p::deallocate).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void deallocPos() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();
    p.deallocate();
  }

  @Test
  public void newPageNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    assertThatThrownBy(p::toNewPage).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void newPagePos() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();
    p.toNewPage();
  }

  @Test
  public void deadSliceNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();

    assertThatThrownBy(() -> p.deadSlice(1)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void sliceNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();

    assertThatThrownBy(() -> p.slice(1)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void addrNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();

    assertThatThrownBy(p::getAddress).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void remainNeg() {
    PageImpl p = new PageImpl(1, allocator.buffer(0), buf -> {});
    p.initialRetain();
    p.close();

    assertThatThrownBy(p::getRemainingBytes).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void slice() {
    PageImpl p = new PageImpl(1, allocator.buffer(1), buf -> {});
    p.initialRetain();
    ArrowBuf b = p.slice(1);
    b.close();
    p.close();
    p.deallocate();
  }

  @Test
  public void sliceAligned() {
    PageImpl p = new PageImpl(16, allocator.buffer(16), buf -> {});
    p.initialRetain();
    ArrowBuf b = p.slice(1);
    assertEquals(15, p.getRemainingBytes());
    ArrowBuf bAligned = p.sliceAligned(1);
    assertEquals(7, p.getRemainingBytes());
    bAligned.close();
    b.close();
    p.close();
    p.deallocate();
  }

  @Test
  public void deadSlice() {
    PageImpl p = new PageImpl(1, allocator.buffer(1), buf -> {});
    p.initialRetain();
    p.deadSlice(1);
    p.close();
    p.deallocate();
  }

  @Test
  public void props() {
    ArrowBuf buf = allocator.buffer(2);
    PageImpl p = new PageImpl(2, buf, b -> {});
    p.initialRetain();
    try {
      assertEquals(buf.memoryAddress(), p.getAddress());
      assertEquals(2, p.getPageSize());
      assertEquals(2, p.getRemainingBytes());
      p.deadSlice(1);
      assertEquals(1, p.getRemainingBytes());
      p.deadSlice(1);
      assertEquals(0, p.getRemainingBytes());
      assertThatThrownBy(() -> p.deadSlice(1)).isInstanceOf(IllegalArgumentException.class);
    } finally {
      p.close();
      p.deallocate();
    }
  }
}
