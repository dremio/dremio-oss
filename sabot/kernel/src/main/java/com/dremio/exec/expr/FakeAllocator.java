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
package com.dremio.exec.expr;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationReservation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

/**
 * Non-functional allocator to be used when doing field materialization.
 */
public class FakeAllocator implements BufferAllocator {

  private static final UnsafeDirectLittleEndian emptyUdle = (new PooledByteBufAllocatorL()).empty;
  private static ArrowBuf empty = new ArrowBuf(new AtomicInteger(), null, emptyUdle, null, null, 0, 0, true);
  public static BufferAllocator INSTANCE = new FakeAllocator();

  private FakeAllocator() {}

  @Override
  public void assertOpen() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf buffer(int arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf buffer(int arg0, BufferManager arg1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getAllocatedMemory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBufAllocator getAsByteBufAllocator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getHeadroom() {
    return Long.MAX_VALUE;
  }

  @Override
  public ArrowBuf getEmpty() {
    return empty;
  }

  @Override
  public long getLimit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getInitReservation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPeakMemoryAllocation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isOverLimit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator newChildAllocator(String arg0, long arg1, long arg2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator newChildAllocator(String arg0, AllocationListener listener, long arg1, long arg2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AllocationReservation newReservation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLimit(long arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toVerboseString() {
    throw new UnsupportedOperationException();
  }
}
