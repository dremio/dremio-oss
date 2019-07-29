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
package com.dremio.exec.vector.complex.writer;

import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.util.BatchPrinter;

public class TestPromotableWriter extends ExecTest {

  @Test
  public void list() throws Exception {
    TestOutputMutator output = new TestOutputMutator(allocator);
    ComplexWriter rootWriter = new VectorContainerWriter(output);
    try(StructWriter writer = rootWriter.rootAsStruct()){


      StructWriter w = writer.struct("struct");
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
        writer.struct("struct").list("a").startList();
        writer.struct("struct").list("a").bigInt()
        .writeBigInt(3);
        writer.struct("struct").list("a").float4().writeFloat4(4);
        writer.struct("struct").list("a").endList();
        w.end();
      }

      rootWriter.setValueCount(4);

      output.finalizeContainer(4);
      BatchPrinter.printBatch(output.getContainer());
    }
  }

}
