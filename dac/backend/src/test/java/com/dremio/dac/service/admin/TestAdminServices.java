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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Entity;

import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.admin.Setting.TextSetting;
import com.dremio.dac.service.admin.SettingsResource.SettingsWrapperObject;
import com.dremio.exec.ExecConstants;

/**
 * Test admin services.
 */
public class TestAdminServices extends BaseTestServer {

  @Test
  public void getSettingList() {
    final String settingKeyToCheck = ExecConstants.OUTPUT_FORMAT_OPTION;
    final Set<String> list = new HashSet(Arrays.asList(ExecConstants.PARQUET_BLOCK_SIZE,
      ExecConstants.PARQUET_DICT_PAGE_SIZE));

    // check that required settings are returned
    SettingsWrapperObject settings = getSettings(list, false);
    assertEquals(list.size(), settings.getSettings().size());

    // check that set settings are returned in case if includeSetSettings = true

    //check that chosen {@code} settingKeyToCheck was not set initially
    TextSetting setting = getSetting(settingKeyToCheck); // save current value

    deleteSetting(settingKeyToCheck);
    settings = getSettings(list, true);
    assertEquals("Setting should not be returned after deletion", false, hasSetting(settings, settingKeyToCheck));

    addOrUpdateSetting(new TextSetting(settingKeyToCheck, "csv"));
    settings = getSettings(list, true);
    assertEquals("Setting should be returned if it was set and respective flag is provided to API method",
      true, hasSetting(settings, settingKeyToCheck));
    //revert setting to original state
    addOrUpdateSetting(setting);
  }

  @Test
  public void getAndSetSetting() {
    TextSetting setting = getSetting(ExecConstants.OUTPUT_FORMAT_OPTION);
    addOrUpdateSetting(new TextSetting(setting.getId(), "csv"));
    //revert setting to original state
    addOrUpdateSetting(setting);
  }

  private SettingsWrapperObject getSettings(Set<String> requiredSettings, boolean includeSetSettings) {
    return expectSuccess(getBuilder(getAPIv2().path("settings"))
      .buildPost(Entity.entity(new SettingsResource.SettingsRequest(requiredSettings, includeSetSettings), JSON)),
        SettingsWrapperObject.class);
  }

  private TextSetting getSetting(String settingId) {
    return (TextSetting)expectSuccess(getBuilder(getAPIv2().path("settings").path(settingId)).buildGet(), Setting.class);
  }

  private void addOrUpdateSetting(TextSetting update) {
    TextSetting updatedSetting = expectSuccess(getBuilder(getAPIv2().path("settings").path(update.getId())).buildPut(Entity.entity(update, JSON)), Setting.TextSetting.class);
    assertEquals(update.getValue(), updatedSetting.getValue());
  }

  private boolean hasSetting(SettingsWrapperObject settings, String settingKey) {
    return settings.getSettings().stream().anyMatch(s -> s.getId().equals(settingKey));
  }

  private void deleteSetting(String settingKey) {
    expectSuccess(getBuilder(getAPIv2().path("settings").path(settingKey)).buildDelete());
  }
}
