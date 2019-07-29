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
package com.dremio.sabot.sort.external.topn;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.tb;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Collections;
import java.util.Properties;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.TopN;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.sort.topn.TopNOperator;

public class TestTopN extends BaseTestOperator {

  @Test
  public void topNDataWithPurge() throws Exception {

    {
      final Properties props = new Properties();
      props.put(ExecConstants.BATCH_PURGE_THRESHOLD, "1");
      testContext.updateConfig(SabotConfig.create(props));
    }

    Table input = t(
      th("c0"),
      tb(
        tr(35),
        tr(8)
      ),
      tb(
        tr(22),
        tr(17),
        tr(15)
      ),
      tb(
        tr(12),
        tr(42),
        tr(18),
        tr(11),
        tr(94),
        tr(106)
      )
    );

    Table output = t(
      th("c0"),
      tr(8),
      tr(11),
      tr(12),
      tr(15)
    );

    TopN topn = new TopN(PROPS, null, 4, Collections.singletonList(ordering("c0", Direction.ASCENDING, NullDirection.FIRST)), false);
    validateSingle(topn, TopNOperator.class, input, output);
  }

  @Test
  public void topNData() throws Exception {

    Table input = t(
      th("c0"),
      tr(35),
      tr(8),
      tr(22),
      tr(17),
      tr(15),
      tr(12),
      tr(42),
      tr(18),
      tr(11),
      tr(94),
      tr(106)
    );

    Table output = t(
      th("c0"),
      tr(8),
      tr(11),
      tr(12),
      tr(15)
    );

    TopN topn = new TopN(PROPS, null, 4, Collections.singletonList(ordering("c0", Direction.ASCENDING, NullDirection.FIRST)), false);
    validateSingle(topn, TopNOperator.class, input, output);
  }


}
