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
package com.dremio.dac.service.admin;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test VALIDATOR in SettingsResource.
 */
public class TestSettingsResource {

  @Test
  public void testValidatorWithValidSetting() throws Exception {
    String id = "support.email.addr";
    Setting updatedSetting1 = new Setting.TextSetting(id, "hi@gmail.com, hi@yahoo.in");
    Setting updatedSetting2 = new Setting.TextSetting(id, " hi@gmail.com, hi@yahoo.in ");
    Assert.assertNotNull(SettingsResource.getSettingValidator(id));
    SettingsResource.getSettingValidator(id).validateSetting(id, updatedSetting1);
    SettingsResource.getSettingValidator(id).validateSetting(id, updatedSetting2);
  }

  @Test
  public void testValidatorWithInvalidSetting() {
    String id = "support.email.addr";
    Setting updatedSetting = new Setting.TextSetting(id, "hi@gmail.com hi@yahoo.in");
    Assert.assertNotNull(SettingsResource.getSettingValidator(id));
    try {
      SettingsResource.getSettingValidator(id).validateSetting(id, updatedSetting);
      Assert.fail("This is an Invalid setting and should fail.");
    } catch (Exception e) {
    }
  }
}
