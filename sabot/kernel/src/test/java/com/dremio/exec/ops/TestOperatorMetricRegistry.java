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
package com.dremio.exec.ops;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import org.junit.Assert;
import org.junit.Test;

public class TestOperatorMetricRegistry {

  @Test
  public void testOperatorMetricRegistry() {
    UserBitShared.CoreOperatorTypeMetricsMap map =
        OperatorMetricRegistry.getCoreOperatorTypeMetricsMap();
    String firstMetricInTableFunction =
        map.getMetricsDef(UserBitShared.CoreOperatorType.TABLE_FUNCTION_VALUE)
            .getMetricDef(ScanOperator.Metric.values().length)
            .getName();
    Assert.assertEquals(
        TableFunctionOperator.Metric.NUM_DATA_FILE.name(), firstMetricInTableFunction);
  }
}
