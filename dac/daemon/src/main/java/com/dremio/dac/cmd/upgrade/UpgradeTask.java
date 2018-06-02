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

import com.dremio.common.Version;

/**
 * Base implementation for all upgrade tasks
 */
public abstract class UpgradeTask {
  protected static final Version VERSION_106 = new Version("1.0.6", 1, 0, 6, 0, "");
  protected static final Version VERSION_109 = new Version("1.0.9", 1, 0, 9, 0, "");
  protected static final Version VERSION_111 = new Version("1.1.1", 1, 1, 1, 0, "");
  protected static final Version VERSION_120 = new Version("1.2.0", 1, 2, 0, 0, "");
  protected static final Version VERSION_130 = new Version("1.3.0", 1, 3, 0, 0, "");
  protected static final Version VERSION_150 = new Version("1.5.0", 1, 5, 0, 0, "");
  protected static final Version VERSION_203 = new Version("2.0.3", 2, 0, 3, 0, "");
  protected static final Version VERSION_205 = new Version("2.0.5", 2, 0, 5, 0, "");

  private final String name;
  private final Version minVersion; // task cannot be run if KVStore version is below min
  private final Version maxVersion; // task can be ignored if KVStore version is above max

  public UpgradeTask(String name, Version minVersion, Version maxVersion) {
    this.name = name;
    this.minVersion = minVersion;
    this.maxVersion = maxVersion;
  }

  Version getMinVersion() {
    return minVersion;
  }

  Version getMaxVersion() {
    return maxVersion;
  }

  public abstract void upgrade(UpgradeContext context) throws Exception;

  @Override
  public String toString() {
    return String.format("%s [%s, %s)", name, minVersion.getVersion(), maxVersion.getVersion());
  }
}
