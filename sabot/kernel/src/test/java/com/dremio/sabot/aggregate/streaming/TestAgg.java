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

import static com.dremio.sabot.Fixtures.NULL_BIGINT;
import static com.dremio.sabot.Fixtures.NULL_VARCHAR;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.aggregate.streaming.StreamingAggOperator;

public class TestAgg extends BaseTestOperator {

  @Test
  public void oneKeyStreamAgg() throws Exception {

    Table input = t(
        th("gb", "val", "rare"),
        tr("bye", 1, 1l),
        tr("bye", 5, NULL_BIGINT),
        tr("hello", 1, NULL_BIGINT),
        tr("hello", 2, 1l),
        tr("what", 10, NULL_BIGINT)
        );

    Table output = t(
        th("grouping", "cnt", "sum", "cnt_rare"),
        tr("bye", 2l, 6l, 1l),
        tr("hello", 2l, 3l, 1l),
        tr("what", 1l, 10l, 0l)
        );

    StreamingAggregate agg = new StreamingAggregate(null,
        Collections.singletonList(n("gb", "grouping")),
        Arrays.asList(
            n("count(val)", "cnt"),
            n("sum(val)", "sum"),
            n("count(rare)", "cnt_rare")
            ),
        1f);

    validateSingle(agg, StreamingAggOperator.class, input, output);
  }


  @Test
  public void twoKeyStreamAgg() throws Exception {
    Table input = t(
        th("gb1", "gb2", "val", "rare"),
        tr("bye", "no", 1, 1l),
        tr("bye", "yo", 5, NULL_BIGINT),
        tr("hello", "no", 1, NULL_BIGINT),
        tr("hello", "yo", 2, 1l),
        tr("what", NULL_VARCHAR, 10, NULL_BIGINT)
        );

    Table output = t(
        th("gb1", "gb2", "cnt", "sum", "cnt_rare"),
        tr("bye", "no", 1l, 1l, 1l),
        tr("bye", "yo", 1l, 5l, 0l),
        tr("hello", "no", 1l, 1l, 0l),
        tr("hello", "yo", 1l, 2l, 1l),
        tr("what", NULL_VARCHAR, 1l, 10l, 0l)
        );

    StreamingAggregate agg = new StreamingAggregate(null,
        Arrays.asList(
            n("gb1"),
            n("gb2")
            ),
        Arrays.asList(
            n("count(val)", "cnt"),
            n("sum(val)", "sum"),
            n("count(rare)", "cnt_rare")
            ),
        1f);

    validateSingle(agg, StreamingAggOperator.class, input, output);
  }


}
