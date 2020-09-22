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
package com.dremio.exec.record;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

import io.netty.buffer.ByteBuf;

public class TestArrowRecordBatchLoader extends DremioTest {
  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void list() throws InterruptedException {
    try (BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-record-batch-loader", 0, Long.MAX_VALUE); ListVector inVector = new ListVector("input", allocator, null)) {
      UnionListWriter writer = inVector.getWriter();
      writer.allocate();
      writer.setPosition(0);
      writer.startList();
      StructWriter structWriter = writer.struct();
      structWriter.bigInt("a");
      writer.endList();

      VectorContainer container = new VectorContainer(allocator);
      container.add(inVector);
      container.setAllCount(1);
      container.buildSchema();

      FragmentWritableBatch fragmentWritableBatch = FragmentWritableBatch.create(QueryId.getDefaultInstance(), 0, 0, 0, container, 0);

      ByteBuf[] buffers = fragmentWritableBatch.getBuffers();
      container.zeroVectors();

      int length = 0;
      for (ByteBuf buf : buffers) {
        length += buf.readableBytes();
      }
      ArrowBuf buffer = allocator.buffer(length);
      for (ByteBuf buf : buffers) {
        buf.writeBytes(buf);
        buf.release();
      }

      ArrowRecordBatchLoader loader = new ArrowRecordBatchLoader(container);
      RawFragmentBatch rawFragmentBatch = new RawFragmentBatch(fragmentWritableBatch.getHeader(), buffer, null);
      buffer.release();
      loader.load(rawFragmentBatch);


      container.close();
      loader.close();
      System.out.println();
      buffer.release();
    }
  }

  @Test
  public void testNullBatchBuffer() throws InterruptedException {
    try (BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-record-batch-loader", 0, Long.MAX_VALUE)) {
      VectorContainer container = new VectorContainer(allocator);
      container.setRecordCount(5);
      container.buildSchema();

      FragmentWritableBatch fragmentWritableBatch = FragmentWritableBatch.create(QueryId.getDefaultInstance(), 0, 0, 0, container, 0);
      container.zeroVectors();
      ArrowBuf buffer = null;

      ArrowRecordBatchLoader loader = new ArrowRecordBatchLoader(container);
      RawFragmentBatch rawFragmentBatch = new RawFragmentBatch(fragmentWritableBatch.getHeader(), buffer, null);
      loader.load(rawFragmentBatch);

      container.close();
      loader.close();
      System.out.println();
    }
  }
}
