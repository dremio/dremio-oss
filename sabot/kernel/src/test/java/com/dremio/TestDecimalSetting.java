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
package com.dremio;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestDecimalSetting extends BaseTestQuery {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testSettingDecimalFlagAtSessionLevelFails() throws Exception {
    exception.expect(UserException.class);
    exception.expectMessage(String.format("Option %s cannot be set at session (or) query level."
      , PlannerSettings.ENABLE_DECIMAL_V2_KEY));
    test(String.format("alter session set \"%s\" = true", PlannerSettings
      .ENABLE_DECIMAL_V2_KEY));
  }

  @Test
  public void testSettingDecimalFlagAtSystemLevelPasses() throws Exception {
    test(String.format("alter system set \"%s\" = true", PlannerSettings
      .ENABLE_DECIMAL_V2_KEY));
  }
}
