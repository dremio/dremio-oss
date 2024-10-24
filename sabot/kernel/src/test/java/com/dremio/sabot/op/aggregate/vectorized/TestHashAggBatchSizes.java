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
package com.dremio.sabot.op.aggregate.vectorized;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.AutoCloseables;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.aggregate.hash.ITSpillingHashAgg;
import com.dremio.sabot.op.spi.SingleInputOperator.State;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TestHashAggBatchSizes extends ITSpillingHashAgg {

  /**
   * Make sure Hash Agg output batch size is always less than max permitted by key
   * exec.batch.records.max
   */
  @Test
  public void testBigBatchUniqueKeys() throws Exception {
    // input batch bigger than exec.batch.records.max
    final int inputBatchSize = 3968;
    final DataRow[] inputRows = new DataRow[inputBatchSize];
    final DataRow[] expectedRows = new DataRow[inputBatchSize];
    final List<RecordBatchData> data = new ArrayList<>();
    OpProps props = PROPS.cloneWithNewReserve(100_000).cloneWithMemoryExpensive(true);
    final List<NamedExpression> dim = List.of(n("x"));
    final List<NamedExpression> measure = List.of(n("count(x)", "cnt"));

    final HashAggregate conf =
        new HashAggregate(OpProps.prototype(), null, dim, measure, true, true, 1f);

    for (int i = 0; i < inputBatchSize; i++) {
      inputRows[i] = tr((long) i);
      expectedRows[i] = tr((long) i, (long) 1);
    }
    Table left = t(th("x"), inputRows).orderInsensitive();
    Table expected = t(th("x", "cnt"), expectedRows).orderInsensitive();

    HashAggregate pop =
        new HashAggregate(
            OpProps.prototype(),
            conf.getChild(),
            conf.getGroupByExprs(),
            conf.getAggrExprs(),
            true,
            true,
            conf.getCardinality());

    final int outputBatchSize = 1024;

    try (AutoCloseable options = with(ExecConstants.TARGET_BATCH_RECORDS_MAX, outputBatchSize);
        VectorizedHashAggOperator op =
            newOperator(VectorizedHashAggOperator.class, pop, outputBatchSize);
        Generator leftGen = left.toGenerator(getTestAllocator()); ) {

      VectorAccessible output = op.setup(leftGen.getOutput());

      while (op.getState() != State.DONE) {
        switch (op.getState()) {
          case CAN_CONSUME:
            int count = leftGen.next(inputBatchSize);
            if (count == 0) {
              op.noMoreToConsume();
            } else {
              op.consumeData(count);
            }
            break;
          case CAN_PRODUCE:
            int outputCount = op.outputData();
            if (outputCount > 0) {
              // output batch should be lesser than exec.batch.records.max
              assertThat(outputCount < outputBatchSize).isTrue();
              data.add(new RecordBatchData(output, getTestAllocator()));
            }
            break;
        }
      }

      expected.checkValid(data);
    } finally {
      AutoCloseables.close(data);
    }
  }
}
