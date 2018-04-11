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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;
import static com.dremio.dac.cmd.upgrade.Upgrade.TASKS_GREATEST_MAX_VERSION;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test for {@code Upgrade}
 */
public class TestUpgrade {

  /**
   * Verify that we don't add a task whose version is higher than the current version,
   * as it would create a loop where user would have to upgrade but would never be able
   * to update the version to a greater version than the task in the kvstore.
   */
  @Test
  public void testMaxTaskVersion() {
    // Making sure that current version is newer that all upgrade tasks
    assertTrue(
        String.format("One task has a newer version (%s) than the current server version (%s)", TASKS_GREATEST_MAX_VERSION, VERSION),
        Upgrade.UPGRADE_VERSION_ORDERING.compare(TASKS_GREATEST_MAX_VERSION, VERSION) <= 0);
  }

}
