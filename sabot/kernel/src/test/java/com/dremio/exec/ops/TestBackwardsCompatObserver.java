/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.ops;

import static com.dremio.exec.ops.BackwardsCompatObserver.convertBitsToBytes;
import static com.dremio.exec.ops.BackwardsCompatObserver.fieldBuffersCount;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt1Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.SerializedField;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * Test valid behavior of BackwardsCompatibleAccountingUserConnection
 */
public class TestBackwardsCompatObserver {

  private BufferAllocator allocator;

  @Before
  public void before() {
    this.allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void after() {
    this.allocator.close();
  }

  private SerializedField.Builder[] fields(int... size) {
    SerializedField.Builder[] result = new SerializedField.Builder[size.length];
    for (int i = 0; i < size.length; i++) {
      result[i] = SerializedField.newBuilder()
          .setBufferLength(size[i]).setValueCount(1);
    }
    return result;
  }

  private ByteBuf[] bufs(int... size) {
    ByteBuf[] result = new ByteBuf[size.length];
    for (int i = 0; i < size.length; i++) {
      result[i] = buf(size[i]);
    }
    return result;
  }

  private ByteBuf buf(int size) {
    return allocator.buffer(size).writerIndex(size);
  }

  @Test
  public void testFieldBuffersCount() {
    ByteBuf[] buffers = bufs(1, 8, 5, 1, 8, 35, 1, 8, 0);
    SerializedField.Builder[] fields = fields(14, 44, 9);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountFirstChild() {
    ByteBuf[] buffers = bufs(1, 8, 5);
    SerializedField.Builder[] fields = fields(1, 13);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountFirstChildLC() {
    ByteBuf[] buffers = bufs(8, 5);
    SerializedField.Builder[] fields = fields(8, 5);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountLastChild() {
    ByteBuf[] buffers = bufs(1, 8);
    SerializedField.Builder[] fields = fields(1, 8);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountLastChildLC() {
    ByteBuf[] buffers = bufs(8);
    SerializedField.Builder[] fields = fields(8, 0);
    testBuffers(buffers, fields);
  }

  private void testBuffers(ByteBuf[] buffers, SerializedField.Builder[] fields) {
    int i = 0;
    for (SerializedField.Builder field : fields) {
      i += fieldBuffersCount(field, buffers, i, buffers.length);
    }
    while (i < buffers.length && buffers[i].readableBytes() == 0) {
      ++ i;
    }
    assertEquals(buffers.length, i);
    for (ByteBuf byteBuf : buffers) {
      byteBuf.release();
    }
  }

  @Test
  public void testConvertBitsToBytes() {
    try (
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        BitVector bits = new BitVector("$bits$", allocator);
        UInt1Vector bytes = new UInt1Vector("$bits$", allocator);
        ) {

      int count = 100;
      for (int i = 0; i < count ; i++) {
        bits.getMutator().setSafe(i, i % 2);
      }
      bits.getMutator().setValueCount(count);

      ArrowBuf oldBuf = bits.getBuffer();
      oldBuf.retain();
      SerializedField.Builder fieldBuilder = TypeHelper.getMetadataBuilder(bits);
      ArrowBuf newBuf = convertBitsToBytes(allocator, fieldBuilder, oldBuf);

      TypeHelper.load(bytes, fieldBuilder.build(), newBuf);
      for (int i = 0; i < count ; i++) {
        assertEquals(i % 2, bytes.getAccessor().get(i));
      }
      newBuf.release();
    }
  }

  @Test
  public void testPadValues() {
    int originalTypeByteWidth = 8;
    int targetTypeByteWidth = 12;
    try (
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        UInt1Vector bytes = new UInt1Vector("$bits$", allocator);
        ) {

      int count = 100;
      for (int i = 0; i < count * 8 ; i++) {
        bytes.getMutator().setSafe(i, i % 8);
      }
      bytes.getMutator().setValueCount(count * 8);

      ArrowBuf oldBuf = bytes.getBuffer();
      oldBuf.retain();
      SerializedField.Builder fieldBuilder = TypeHelper.getMetadataBuilder(bytes);
      ArrowBuf newBuf = BackwardsCompatObserver.padValues(allocator, fieldBuilder, oldBuf, originalTypeByteWidth, targetTypeByteWidth);
      fieldBuilder.setValueCount(count * 12); // since we're loading it in a bytes vector
      TypeHelper.load(bytes, fieldBuilder.build(), newBuf);
      for (int i = 0; i < count ; i++) {
        for (int byteIndex = 0; byteIndex < 8 ; byteIndex++) {
          assertEquals((i * 8 + byteIndex) % 8, bytes.getAccessor().get(i * 12 + byteIndex));
        }
      }
      newBuf.release();
    }
  }
}
