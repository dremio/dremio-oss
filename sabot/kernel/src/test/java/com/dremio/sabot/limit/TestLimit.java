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
package com.dremio.sabot.limit;

import static com.dremio.sabot.Fixtures.Table;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.exec.physical.config.Limit;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.limit.LimitOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public class TestLimit extends BaseTestOperator {

  @Test
  public void firstFive() throws Exception {
    Limit l = new Limit(PROPS, null, 0, 5);
    Table output = t(
        th("c0"),
        tr(1),
        tr(2),
        tr(3),
        tr(4),
        tr(5)
        );

    validateSingle(l, LimitOperator.class, DATA, output);
  }

  @Test
  public void earlyTermination() throws Exception {
    Limit limit = new Limit(PROPS, null, 0, 1);
    try(
        LimitOperator op = newOperator(LimitOperator.class, limit, 100);
        Generator generator = TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, getTestAllocator());
        ){

      op.setup(generator.getOutput());
      int records = generator.next(100);
      op.consumeData(records);
      assertEquals(1, op.outputData());
      assertEquals(SingleInputOperator.State.DONE, op.getState());
    }
  }


  @Test
  public void secondFive() throws Exception {
    Limit l = new Limit(PROPS, null, 5, 10);
    Table output = t(
        th("c0"),
        tr(6),
        tr(7),
        tr(8),
        tr(9),
        tr(10)
        );
    validateSingle(l, LimitOperator.class, DATA, output);
  }

  private static final Table DATA = t(
      th("c0"),
      tr(1),
      tr(2),
      tr(3),
      tr(4),
      tr(5),
      tr(6),
      tr(7),
      tr(8),
      tr(9),
      tr(10),
      tr(11),
      tr(12),
      tr(13),
      tr(14),
      tr(15)
      );

}
