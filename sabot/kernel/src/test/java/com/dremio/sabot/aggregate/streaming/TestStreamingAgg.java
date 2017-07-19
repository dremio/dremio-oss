/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.aggregate.streaming;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;

import org.junit.Test;

import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.aggregate.streaming.StreamingAggOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public class TestStreamingAgg extends BaseTestOperator {

  @Test
  public void oneKeySumCnt() throws Exception {
    StreamingAggregate conf = new StreamingAggregate(null,
        Arrays.asList(n("r_name")),
        Arrays.asList(
            n("sum(r_regionkey)", "sum"),
            n("count(r_regionkey)", "cnt")
            ),
        1f);

    final Table expected = t(
        th("r_name",    "sum", "cnt"),
        tr("AFRICA",      0L, 1L),
        tr("AMERICA",     1L, 1L),
        tr("ASIA",        2L, 1L),
        tr("EUROPE",      3L, 1L),
        tr("MIDDLE EAST", 4L, 1L)
        );

    validateSingle(conf, StreamingAggOperator.class, TpchTable.REGION, 0.1, expected);
  }

  @Test
  public void oneKeySumCntSmallBatch() throws Exception {
    StreamingAggregate conf = new StreamingAggregate(null,
        Arrays.asList(n("r_name")),
        Arrays.asList(
            n("sum(r_regionkey)", "sum"),
            n("count(r_regionkey)", "cnt")
            ),
        1f);

    final Table expected = t(
        th("r_name",    "sum", "cnt"),
        tr("AFRICA",      0L, 1L),
        tr("AMERICA",     1L, 1L),
        tr("ASIA",        2L, 1L),
        tr("EUROPE",      3L, 1L),
        tr("MIDDLE EAST", 4L, 1L)
        );

    assertSingleInput(conf, StreamingAggOperator.class, TpchTable.REGION, 0.1, null, 2, expected);
  }
}