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
package com.dremio.dac.service.admin;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Entity;

import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.admin.Setting.TextSetting;
import com.dremio.dac.service.admin.SettingsResource.SettingsWrapperObject;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.options.SystemOptionManager;

/**
 * Test admin services.
 */
public class TestAdminServices extends BaseTestServer {

  @Test
  public void getSettingList(){
    SettingsWrapperObject settings = expectSuccess(getBuilder(getAPIv2().path("settings")).buildGet(), SettingsWrapperObject.class);
    assertEquals(l(SystemOptionManager.class).settingCount(), settings.getSettings().size());
  }

  @Test
  public void getAndSetSetting(){
    TextSetting setting = (TextSetting) expectSuccess(getBuilder(getAPIv2().path("settings").path(ExecConstants.OUTPUT_FORMAT_OPTION)).buildGet(), Setting.class);
    TextSetting update = new TextSetting(setting.getId(), "csv", true);
    TextSetting updatedSetting = expectSuccess(getBuilder(getAPIv2().path("settings").path(ExecConstants.OUTPUT_FORMAT_OPTION)).buildPut(Entity.entity(update, JSON)), Setting.TextSetting.class);
    assertEquals(update.getValue(), updatedSetting.getValue());
    expectSuccess(getBuilder(getAPIv2().path("settings").path(ExecConstants.OUTPUT_FORMAT_OPTION)).buildPut(Entity.entity(setting, JSON)), Setting.TextSetting.class);
  }
}
