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
package com.dremio.sabot.exec.fragment;

import static org.mockito.Mockito.when;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.options.OptionManager;

public class TestBatchSize {

  @Test
  public void testRecordBatchAllocs() {
    OptionManager options = Mockito.mock(OptionManager.class);
    when(options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN)).thenReturn(127L);
    when(options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX)).thenReturn(4095L);
    when(options.getOption(ExecConstants.TARGET_BATCH_SIZE_BYTES)).thenReturn(1024 * 1024L);

    int batchSize = PhysicalPlanCreator.calculateBatchCountFromRecordSize(options, 0);
    Assert.assertTrue(batchSize >= (int)(0.95 * 4096));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      final ValueVector bitV = new BitVector("bits", allocator);
      AllocationHelper.allocate(bitV, batchSize, 0);
      Assert.assertEquals(1024, allocator.getAllocatedMemory());
      bitV.close();

      final ValueVector intV = new IntVector("ints", allocator);
      AllocationHelper.allocate(intV, batchSize, 0);
      Assert.assertEquals(4096 * 4, allocator.getAllocatedMemory());
      intV.close();

      final ValueVector longV = new BigIntVector("longs", allocator);
      AllocationHelper.allocate(longV, batchSize, 0);
      Assert.assertEquals(4096 * 8, allocator.getAllocatedMemory());
      longV.close();

      final ValueVector decimalV = new DecimalVector("decimals", allocator, 38, 9);
      AllocationHelper.allocate(decimalV, batchSize, 0);
      Assert.assertEquals(4096 * 16, allocator.getAllocatedMemory());
      decimalV.close();
    }
  }
}
