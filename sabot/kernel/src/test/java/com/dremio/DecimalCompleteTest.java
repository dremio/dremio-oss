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
package com.dremio;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.exec.planner.physical.PlannerSettings;

public class DecimalCompleteTest extends BaseTestQuery {
  @BeforeClass
  public static void turnDecimalOn() throws Exception {
    test(String.format("alter system set \"%s\" = true", PlannerSettings
      .ENABLE_DECIMAL_DATA_TYPE_KEY));
    test(String.format("alter system set \"%s\" = true", PlannerSettings
      .ENABLE_DECIMAL_V2_KEY));
  }

  @AfterClass
  public static void turnDecimalToDefault() throws Exception {
    test(String.format("alter system set \"%s\" = %s", PlannerSettings
      .ENABLE_DECIMAL_DATA_TYPE_KEY, PlannerSettings.ENABLE_DECIMAL_DATA_TYPE.getDefault().getBoolVal()));
    test(String.format("alter system set \"%s\" = %s", PlannerSettings
      .ENABLE_DECIMAL_V2_KEY, PlannerSettings.ENABLE_DECIMAL_V2.getDefault().getBoolVal()));
  }
}
