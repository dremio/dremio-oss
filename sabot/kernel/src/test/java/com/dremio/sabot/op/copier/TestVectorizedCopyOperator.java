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
package com.dremio.sabot.op.copier;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.physical.config.SelectionVectorRemover;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGeneratorWithSV2;
import com.dremio.sabot.CustomGeneratorWithSV2.SelectionVariant;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.copier.VectorizedCopyOperator.Metric;

public class TestVectorizedCopyOperator extends BaseTestOperator {
  static final int NUM_ROWS = 200;

  private void testWithBatchSize(int batchSize, CustomGeneratorWithSV2.SelectionVariant variant) throws Exception {
    try (CustomGeneratorWithSV2 generator = new CustomGeneratorWithSV2(NUM_ROWS, getTestAllocator(), variant)) {
      SelectionVectorRemover pop = new SelectionVectorRemover(PROPS, null);
      OperatorStats stats = validateSingle(pop, VectorizedCopyOperator.class, generator, generator.getExpectedTable(), batchSize);

      int expectedBatches = (generator.totalSelectedRows() + batchSize - 1) / batchSize;
      Assert.assertEquals(expectedBatches, stats.getLongStat(Metric.OUTPUT_BATCH_COUNT));
    }
  }

  @Test
  public void testWithBatch10() throws Exception {
    testWithBatchSize(10, SelectionVariant.SELECT_ALTERNATE);
    testWithBatchSize(10, SelectionVariant.SELECT_ALL);
    testWithBatchSize(10, SelectionVariant.SELECT_NONE);
  }

  @Test
  public void testWithBatch20() throws Exception {
    testWithBatchSize(20, SelectionVariant.SELECT_ALTERNATE);
    testWithBatchSize(10, SelectionVariant.SELECT_ALL);
  }

  @Test
  public void testWithBatch15() throws Exception {
    // 200 is not a multiple of 15
    testWithBatchSize(15, SelectionVariant.SELECT_ALTERNATE);
    testWithBatchSize(10, SelectionVariant.SELECT_ALL);
  }
}
