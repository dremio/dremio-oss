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
package com.dremio.sabot.op.join.vhash.spill;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.join.BaseTestJoin;
import com.dremio.sabot.join.hash.TestVHashJoinSpill;
import com.dremio.sabot.op.spi.SingleInputOperator;

public class TestHashJoinOperatorShrinkableStates extends TestVHashJoinSpill {

  @Test
  public void testShrinkMemory() throws Exception {
    BaseTestJoin.JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))),
      JoinRelType.INNER);

    final int batchSize = 1096;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long) i);
      rightRows[i] = tr((long) i);
    }

    final Fixtures.Table left = t(th("a"), leftRows);
    final Fixtures.Table right = t(th("b"), rightRows);
    VectorizedSpillingHashJoinOperator op = newOperator(VectorizedSpillingHashJoinOperator.class, joinInfo.operator, batchSize);
    try (Generator leftGen = left.toGenerator(getTestAllocator());
         Generator rightGen = right.toGenerator(getTestAllocator())) {

        op.setup(leftGen.getOutput(), rightGen.getOutput());

        //consume a build batch
        int rightCount = rightGen.next(batchSize);
        op.consumeDataRight(rightCount);
        long shrinkableMemory = op.shrinkableMemory();

        //spill
        op.shrinkMemory(batchSize);
        assertTrue(shrinkableMemory > op.shrinkableMemory());

       shrinkableMemory = op.shrinkableMemory();
        //to run multi-memory releaser
        op.outputData();

        //more memory shrinked in memory releaser
        assertTrue(shrinkableMemory > op.shrinkableMemory());

        //consume a build batch
        rightCount = rightGen.next(batchSize);
        op.consumeDataRight(rightCount);

        //goes into CAN_PRODUCE state
        op.noMoreToConsumeRight();

        shrinkableMemory = op.shrinkableMemory();

        //shrinkable memory stays the same spilling is not allowed in CAN_PRODUCE state
        assertEquals(shrinkableMemory, op.shrinkableMemory());

      }
    }

  @Test
  public void testShrinkInCanConsumeL()  throws Exception {
    BaseTestJoin.JoinInfo joinInfo = getJoinInfo(Arrays.asList(new JoinCondition("EQUALS", f("a"), f("b"))),
      JoinRelType.INNER);

    final int batchSize = 4096;
    final Fixtures.DataRow[] leftRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] rightRows = new Fixtures.DataRow[batchSize];
    final Fixtures.DataRow[] expectedRows = new Fixtures.DataRow[batchSize];
    for (int i = 0; i < batchSize; i++) {
      leftRows[i] = tr((long)i, i+1);
      rightRows[i] = tr((long)i, i+11);
      expectedRows[i] = tr((long)i, i+11, (long)i, i+1);
    }

    final Fixtures.Table left = t(th("a", "aInt"), leftRows);
    final Fixtures.Table right = t(th("b", "bInt"), rightRows);
    final Fixtures.Table expected = t(th("b", "bInt", "a", "aInt"), expectedRows).orderInsensitive();

    VectorizedSpillingHashJoinOperator op = newOperator(VectorizedSpillingHashJoinOperator.class, joinInfo.operator, batchSize);
    final List<RecordBatchData> data = new ArrayList<>();
    try (Generator leftGen = left.toGenerator(getTestAllocator());
         Generator rightGen = right.toGenerator(getTestAllocator())) {
      VectorAccessible output = op.setup(leftGen.getOutput(), rightGen.getOutput());

      boolean doneShrinking = false;
      outside: while(true){
        switch(op.getState()){
          case CAN_CONSUME_L:
            int leftCount = leftGen.next(batchSize);
            if(leftCount > 0){
              op.consumeDataLeft(leftCount);
            }else{
              op.noMoreToConsumeLeft();
            }
            break;
          case CAN_CONSUME_R:
            int rightCount = rightGen.next(batchSize);
            if(rightCount > 0){
              op.consumeDataRight(rightCount);
            }else{
              op.noMoreToConsumeRight();
              assertTrue(op.shrinkableMemory() > 0);
              doneShrinking = op.shrinkMemory(batchSize);
            }
            break;
          case CAN_PRODUCE:
            if (!doneShrinking) {
              doneShrinking = op.shrinkMemory(batchSize);
            } else {
              int outputCount = op.outputData();
              if (outputCount > 0
                || (outputCount == 0 && expected.isExpectZero())) {
                data.add(new RecordBatchData(output, getTestAllocator()));
              }
            }
            break;
          case DONE:
            break outside;
          default:
            throw new UnsupportedOperationException("State is: " + op.getState());
        }
      }

      assertState(op, SingleInputOperator.State.DONE);
      expected.checkValid(data);
    } finally {
      AutoCloseables.close(data);
    }
  }
}
