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
package com.dremio.exec.planner.physical;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorBuilder;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

public class TestHashAggEstimator extends BaseTestOperator {

  HashAggMemoryEstimator estimator;

  /**
   * This method tests TestHashAggEstimator::computeAccumulatorSizeForSinglePartition method - which
   * computes estimated size for one batch of each accumulator in hash agg. Method of testing - For
   * a hash agg that has 6 fixed length accumulators and 2 variable length accumulators, above
   * function is called to get the estimated memory required for all accumulator combined. Then
   * actual vectors of same batch size are created to measure actual space that taken by these
   * accumulator vectors. Actual space should be less that estimated one.
   *
   * @throws Exception
   */
  @Test
  public void testAllocatorVectorSize() throws Exception {

    // Step 1 - Create a dummy hash agg POp

    // 6 integer allocator and 2 variable-length allocators
    HashAggregate conf =
        new HashAggregate(
            OpProps.prototype(),
            null,
            List.of(n("gb")),
            Arrays.asList(
                // fixed length
                n("sum(d1)", "sum-d1"),
                n("count(d1)", "cnt-d1"),
                n("$sum0(d1)", "sum0-d1"),
                n("min(d1)", "min-d1"),
                n("max(d1)", "max-d1"),
                n("count(d2)", "cnt-d2"),
                // variable length
                n("min(d2)", "min-d2"),
                n("max(d2)", "max-d2")),
            true,
            true,
            1f);

    final Table dataInt = t(th("gb", "d1", "d2"), tr("group1", 0, "abc"));

    Generator gen = dataInt.toGenerator(getTestAllocator());
    VectorAccessible incoming = gen.getOutput();

    HashAggregate pop =
        new HashAggregate(
            OpProps.prototype(),
            conf.getChild(),
            conf.getGroupByExprs(),
            conf.getAggrExprs(),
            true,
            true,
            conf.getCardinality());

    for (int batchSize = 128; batchSize <= 4096; batchSize *= 2) {

      // Step 2: Use aggregate expressions in that Pop to generate a HashAgg memory estimator and
      // get an accumulator memory estimate from it
      final OperatorContextImpl context =
          testContext.getNewOperatorContext(getTestAllocator(), pop, batchSize, null);

      try (AutoCloseable options1 = with(ExecConstants.TARGET_BATCH_RECORDS_MAX, batchSize)) {
        final AccumulatorBuilder.MaterializedAggExpressionsResult materializeAggExpressionsResult =
            AccumulatorBuilder.getAccumulatorTypesFromExpressions(
                context.getClassProducer(), pop.getAggrExprs(), incoming);
        estimator =
            new HashAggMemoryEstimator(
                1,
                batchSize,
                32000,
                materializeAggExpressionsResult,
                null,
                testContext.getOptions());
        long computed = estimator.computeAccumulatorSizeForSinglePartition();

        // Step 3: Actually create Arrow vectors corresponding to these accumulators and allocate
        // memory in them for given batch size

        long actual = computeActualVectorSizeFprBatch(batchSize);
        assertTrue(actual <= computed);
      }
    }
    gen.close();
  }

  private long computeActualVectorSizeFprBatch(int batchSize) {

    long capacity = 0;
    try (IntVector iv = new IntVector("int_vector", getTestAllocator());
        VarCharVector vv =
            new VarCharVector(
                new Field("string_vector", FieldType.nullable(MinorType.VARCHAR.getType()), null),
                getTestAllocator())) {

      iv.allocateNew(batchSize);
      long intCapacity = iv.getValidityBuffer().capacity() + iv.getDataBuffer().capacity();

      // we have 4 int accumulators
      intCapacity *= 6;

      capacity += intCapacity;

      long maxVariableBatchSize =
          testContext.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
      vv.allocateNew(maxVariableBatchSize * batchSize, batchSize);

      long stringCapacity =
          vv.getValidityBuffer().capacity()
              + vv.getDataBuffer().capacity()
              + vv.getOffsetBuffer().capacity();

      // we have 2 string accumulators
      stringCapacity *= 2;

      capacity += stringCapacity;
    }

    return capacity;
  }
}
