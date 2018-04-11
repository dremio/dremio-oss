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
package com.dremio.sabot.filter;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import org.junit.Test;

import com.dremio.exec.physical.config.Filter;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.filter.FilterOperator;

public class TestSimpleFilter extends BaseTestOperator {

  @Test
  public void simpleFilter() throws Exception {

    Filter f = new Filter(null, toExpr("c0 < 10"), 1f);
    Table input = t(
        th("c0"),
        tr(35),
        tr(8),
        tr(22)
        );

    Table output = t(
        th("c0"),
        tr(8)
        );

    validateSingle(f, FilterOperator.class, input, output);
  }

  @Test
  public void varcharFilter() throws Exception {

    Filter f = new Filter(null, toExpr("like(c0, 'hell%')"), 1f);
    Table input = t(
        th("c0"),
        tr("hello"),
        tr("bye")
        );

    Table output = t(
        th("c0"),
        tr("hello")
        );

    validateSingle(f, FilterOperator.class, input, output);
  }


}
