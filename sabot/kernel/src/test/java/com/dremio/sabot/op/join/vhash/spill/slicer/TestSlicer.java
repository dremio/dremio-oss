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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecTest;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.google.common.base.Preconditions;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;
import io.netty.util.internal.PlatformDependent;

public class TestSlicer extends ExecTest {

  @Test
  public void simple() throws Exception {
    check(TpchTable.REGION, 0.1, 64_000, 4095);
  }

  @Test
  public void manyPagesPerBatch() throws Exception {
    assertTrue(check(TpchTable.CUSTOMER, 0.1, 64_000, 4095) > 5);
  }

  @Test
  public void manyBatchPerPage() throws Exception {
    assertTrue(check(TpchTable.CUSTOMER, 0.1, 512_000, 200) < .5);
  }

  private double check(TpchTable table, double scale, int pageSize, int batchSize, String... columns) throws Exception {
    Table expected = TpchGenerator.singleGenerator(table, scale, getAllocator(), columns).toTable(batchSize);
    return check(expected, pageSize, batchSize,
      TpchGenerator.singleGenerator(table, scale, allocator, columns));
  }

  private double check(Table expected, int pageSize, int batchSize, Generator generator) throws Exception {
    try (final PagePool pages = new PagePool(getAllocator(), pageSize, 0);
         final ArrowBuf sv2Buf = getFilledSV2(getAllocator(), batchSize)) {
      PageBatchSlicer slicer = new PageBatchSlicer(pages, sv2Buf.memoryAddress(),
        generator.getOutput(), ImmutableBitSet.range(0, generator.getOutput().getSchema().getFieldCount()));
      List<RecordBatchData> actual = new ArrayList<>();
      int inputBatches = 0;
      while(true) {
        int records = generator.next(batchSize);
        inputBatches++;
        if(records == 0) {
          break;
        }
        List<RecordBatchPage> data = new ArrayList<>();
        int ret = slicer.addBatch(records, data);
        Preconditions.checkState(ret == records);

        actual.addAll(data);
      }
      expected.checkValid(actual);
      AutoCloseables.close(actual);
      int poolSize = pages.getPageCount();
      if (PageBatchSlicer.TRACE) {
        System.out.println("Pages: " + poolSize + ", Input Batches: " + inputBatches);
      }
      return poolSize * 1d/inputBatches;
    } finally {
      generator.close();
    }
  }

  private ArrowBuf getFilledSV2(BufferAllocator allocator, int batchSize) {
    ArrowBuf buf = allocator.buffer(batchSize * SelectionVector2.RECORD_SIZE);
    long addr = buf.memoryAddress();
    for (int i = 0; i < batchSize; ++i) {
      PlatformDependent.putShort(addr, (short)i);
      addr += SelectionVector2.RECORD_SIZE;
    }
    return buf;
  }
}
