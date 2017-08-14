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
package com.dremio.exec.record;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.fragment.Fragment;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.op.receiver.RawFragmentBatch;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

public class TestArrowRecordBatchLoader {
  @Test
  public void list() throws InterruptedException {
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE); ListVector inVector = new ListVector("input", allocator, null)) {
      UnionListWriter writer = inVector.getWriter();
      writer.allocate();
      writer.setPosition(0);
      writer.startList();
      MapWriter mapWriter = writer.map();
      mapWriter.bigInt("a");
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
}
