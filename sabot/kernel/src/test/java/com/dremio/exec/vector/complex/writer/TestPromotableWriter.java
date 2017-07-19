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
package com.dremio.exec.vector.complex.writer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.util.BatchPrinter;

public class TestPromotableWriter extends ExecTest {

  @Test
  public void list() throws Exception {
    try (BufferAllocator allocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);) {
      TestOutputMutator output = new TestOutputMutator(allocator);
      ComplexWriter rootWriter = new VectorContainerWriter(output);
      try(MapWriter writer = rootWriter.rootAsMap()){


        MapWriter w = writer.map("map");
        rootWriter.setPosition(0);
        {
          w.start();
          w.bigInt("a").writeBigInt(1);
          w.end();
        }
        rootWriter.setPosition(1);
        {
          w.start();
          w.float4("a").writeFloat4(2.0f);
          w.end();
        }
        rootWriter.setPosition(2);
        {
          w.start();
          w.list("a").startList();
          w.list("a").endList();
          w.end();
        }
        rootWriter.setPosition(3);
        {
          w.start();
          writer.map("map").list("a").startList();
          writer.map("map").list("a").bigInt()
          .writeBigInt(3);
          writer.map("map").list("a").float4().writeFloat4(4);
          writer.map("map").list("a").endList();
          w.end();
        }

        rootWriter.setValueCount(4);

        output.finalizeContainer(4);
        BatchPrinter.printBatch(output.getContainer());
      }
    }
  }

}
