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
package com.dremio.sabot.sort;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;
import com.dremio.sabot.op.spi.Operator.MasterState;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public class TestSortOperator extends BaseTestOperator {

  private static final TpchTable TABLE = TpchTable.CUSTOMER;
  private static final double SCALE = 1;

  private ExternalSort getRegionSort(){
    return new ExternalSort(PROPS, null, Collections.singletonList(ordering("c_custkey", Direction.ASCENDING, NullDirection.FIRST)), false);
  }

  @Test
  public void lifecycle() throws Exception {

    /**
     * We expect a lifecycle of:
     * SETUP, setup() => CONSUME;
     * CONSUME until noMoreConsume() called
     * Then produce 1..N times
     * Until number of records produced == number of records consumed.
     * Then DONE
     */
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {
      // Test ExternalSort with both QuickSorter (default) and SplayTreeSorter:
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = false (default) when sortOperatorType = 0
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = true when sortOperatorType = 1
      try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType == 0 ? false : true)) {
        final ExternalSortOperator o = newOperator(ExternalSortOperator.class, getRegionSort(), 4095);

        assertState(o, MasterState.NEEDS_SETUP);
        try (TpchGenerator generator = TpchGenerator.singleGenerator(TABLE, SCALE, getTestAllocator())) {

          o.setup(generator.getOutput());

          int inputCount = 0;
          assertState(o, MasterState.CAN_CONSUME);
          while (true) {
            final int count = generator.next(4095);
            if (count == 0) {
              break;
            }

            inputCount += count;
            o.consumeData(count);
            assertState(o, MasterState.CAN_CONSUME);
          }

          o.noMoreToConsume();
          assertState(o, MasterState.CAN_PRODUCE);
          int outputCount = 0;
          while (true) {
            outputCount += o.outputData();
            if (o.getState().getMasterState() == MasterState.DONE) {
              break;
            }
          }
          assertEquals("Records consumed and produced are different.", inputCount, outputCount);
        }
      }
    }
  }

  @Test
  public void closeBeforeSetupLeaksNothing() throws Exception {
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {
      // Test ExternalSort with both QuickSorter (default) and SplayTreeSorter:
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = false (default) when sortOperatorType = 0
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = true when sortOperatorType = 1
      try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType == 0 ? false : true)) {
        newOperator(ExternalSortOperator.class, getRegionSort(), 4095);
      }
    }
  }

  @Test
  public void closeAfterSetupLeaksNothing() throws Exception {
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {
      // Test ExternalSort with both QuickSorter (default) and SplayTreeSorter:
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = false (default) when sortOperatorType = 0
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = true when sortOperatorType = 1
      try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType == 0 ? false : true)) {
        ExternalSortOperator operator = newOperator(ExternalSortOperator.class, getRegionSort(), 4095);
        try (TpchGenerator g = TpchGenerator.singleGenerator(TABLE, SCALE, getTestAllocator())) {
          ; // create schema on vector container but don't generate any records since we're only setting up.
          operator.setup(g.getOutput());
        }
      }
    }
  }

  @Test
  public void closeDuringConsumeLeaksNothing() throws Exception {
    for (int sortOperatorType = 0; sortOperatorType < 2; sortOperatorType++) {
      // Test ExternalSort with both QuickSorter (default) and SplayTreeSorter:
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = false (default) when sortOperatorType = 0
      // EXTERNAL_SORT_ENABLE_SPLAY_SORT = true when sortOperatorType = 1
      try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, sortOperatorType == 0 ? false : true)) {
        final ExternalSortOperator o = newOperator(ExternalSortOperator.class, getRegionSort(), 4095);
        try (TpchGenerator generator = TpchGenerator.singleGenerator(TABLE, SCALE, getTestAllocator())) {
          o.setup(generator.getOutput());
          int count = generator.next(4000);
          o.consumeData(count);
        }
      }
    }
  }

}
