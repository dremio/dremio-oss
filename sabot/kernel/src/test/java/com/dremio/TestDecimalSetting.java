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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.Test;

public class TestDecimalSetting extends BaseTestQuery {

  @Test
  public void testSettingDecimalFlagAtSessionLevelFails() {
    assertThatThrownBy(
            () ->
                test(
                    String.format(
                        "alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_V2_KEY)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining(
            "Admin related settings can only be set at SYSTEM level scope. Given scope 'SESSION'");
  }

  @Test
  public void testSettingDecimalFlagAtSystemLevelPasses() throws Exception {
    test(String.format("alter system set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_V2_KEY));
  }
}
