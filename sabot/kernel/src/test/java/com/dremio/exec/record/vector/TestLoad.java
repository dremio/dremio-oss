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
package com.dremio.exec.record.vector;

import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.WritableBatch;
import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

public class TestLoad extends ExecTest {

  private static void log(Object obj) {
    //no op
  }

  @Test
  public void testLoadValueVector() throws Exception {
    final ValueVector fixedV = new IntVector("ints", allocator);
    final ValueVector varlenV = new VarCharVector("chars", allocator);
    final ValueVector nullableVarlenV = new VarCharVector("chars", allocator);

    final List<ValueVector> vectors = Lists.newArrayList(fixedV, varlenV, nullableVarlenV);
    for (final ValueVector v : vectors) {
      AllocationHelper.allocate(v, 100, 50);
      GenerateSampleData.generateTestData(v, 100);
    }

    final WritableBatch writableBatch = WritableBatch.getBatchNoHV(100, vectors, false);
    final RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    final ByteBuf[] byteBufs = writableBatch.getBuffers();
    int bytes = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      bytes += byteBufs[i].writerIndex();
    }
    final ArrowBuf byteBuf = allocator.buffer(bytes);
    int index = 0;
    for (int i = 0; i < byteBufs.length; i++) {
      byteBufs[i].readBytes(NettyArrowBuf.unwrapBuffer(byteBuf), index, byteBufs[i].writerIndex());
      index += byteBufs[i].writerIndex();
    }
    byteBuf.writerIndex(bytes);

    batchLoader.load(writableBatch.getDef(), byteBuf);
    boolean firstColumn = true;
    int recordCount = 0;
    for (final VectorWrapper<?> v : batchLoader) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        log("\t");
      }
      log(v.getField().getName());
      log("[");
      log(v.getField().getType());
      log("]");
    }

    log("");
    for (int r = 0; r < batchLoader.getRecordCount(); r++) {
      boolean first = true;
      recordCount++;
      for (final VectorWrapper<?> v : batchLoader) {
        if (first) {
          first = false;
        } else {
          log("\t");
        }
        final ValueVector vv = v.getValueVector();
        if (getMinorTypeForArrowType(v.getField().getType()) == MinorType.VARCHAR) {
          final Object obj = vv.getObject(r);
          if (obj != null) {
            log(vv.getObject(r));
          } else {
            log("NULL");
          }
        } else {
          log(vv.getObject(r));
        }
      }
      if (!first) {
        log("");
      }
    }
    assertEquals(100, recordCount);
    byteBuf.release();
    batchLoader.clear();
    writableBatch.clear();
  }
}
