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
package com.dremio.dac.cmd.upgrade;

import static com.dremio.common.util.DremioVersionInfo.VERSION;

import com.dremio.common.Version;

/**
 * Legacy methods, constants to support UpgradeTasks
 * before introducing UpgradeTaskStore
 */
@Deprecated
public interface LegacyUpgradeTask {

  // List of Dremio version requiring upgrade tasks
  Version VERSION_106 = new Version("1.0.6", 1, 0, 6, 0, "");
  Version VERSION_203 = new Version("2.0.3", 2, 0, 3, 0, "");
  Version VERSION_205 = new Version("2.0.5", 2, 0, 5, 0, "");
  Version VERSION_2010 = new Version("2.0.10", 2, 0, 10, 0, "");
  Version VERSION_2011 = new Version("2.0.11", 2, 0, 11, 0, "");
  Version VERSION_210 = new Version("2.1.0", 2, 1, 0, 0, "");
  Version VERSION_212 = new Version("2.1.2", 2, 1, 2, 0, "");
  Version VERSION_217 = new Version("2.1.7", 2, 1, 7, 0, "");
  Version VERSION_300 = new Version("3.0.0", 3, 0, 0, 0, "");

  /**
   * This method is here to support legacy tasks
   * please refrain from using this method going forward
   * Upgrade tasks will not need to use min and max versions
   * task will be run or not based on entry(ies) in UpgradeTask store
   * if there are any specific requirements around versions
   * they need to be handled by the task itself
   * @return version
   */
  @Deprecated
  default Version getMaxVersion() {
    return VERSION;
  }
}
