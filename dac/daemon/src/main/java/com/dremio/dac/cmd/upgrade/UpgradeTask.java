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

import java.util.Comparator;

import com.dremio.common.Version;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;

/**
 * Base implementation for all upgrade tasks
 *
 * For upgrade tasks to be picked up, make sure that the package is searched for
 * by {@code com.dremio.common.scanner.ClassPathScanner}
 *
 * Also, use {@code TestUpgrade} to confirm that the task is detected and order
 * is valid.
 */
public abstract class UpgradeTask implements Comparable<UpgradeTask>{
  private static final Comparator<UpgradeTask> COMPARATOR = Comparator
      .comparing(UpgradeTask::getOrder)
      .thenComparing(t -> t.getClass().getSimpleName());

  // List of Dremio version requiring upgrade tasks
  protected static final Version VERSION_106 = new Version("1.0.6", 1, 0, 6, 0, "");
  protected static final Version VERSION_109 = new Version("1.0.9", 1, 0, 9, 0, "");
  protected static final Version VERSION_111 = new Version("1.1.1", 1, 1, 1, 0, "");
  protected static final Version VERSION_120 = new Version("1.2.0", 1, 2, 0, 0, "");
  protected static final Version VERSION_130 = new Version("1.3.0", 1, 3, 0, 0, "");
  protected static final Version VERSION_150 = new Version("1.5.0", 1, 5, 0, 0, "");
  protected static final Version VERSION_203 = new Version("2.0.3", 2, 0, 3, 0, "");
  protected static final Version VERSION_205 = new Version("2.0.5", 2, 0, 5, 0, "");
  protected static final Version VERSION_2010 = new Version("2.0.10", 2, 0, 10, 0, "");
  protected static final Version VERSION_2011 = new Version("2.0.11", 2, 0, 11, 0, "");
  protected static final Version VERSION_210 = new Version("2.1.0", 2, 1, 0, 0, "");
  protected static final Version VERSION_212 = new Version("2.1.2", 2, 1, 2, 0, "");
  protected static final Version VERSION_217 = new Version("2.1.7", 2, 1, 7, 0, "");
  protected static final Version VERSION_300 = new Version("3.0.0", 3, 0, 0, 0, "");

  // A couple of order values
  protected static final int FIRST_ORDER = 1;
  protected static final int URGENT_ORDER = 100;
  protected static final int NORMAL_ORDER = 1000;

  private final String name;
  private final Version minVersion; // task cannot be run if KVStore version is below min
  private final Version maxVersion; // task can be ignored if KVStore version is above max
  private final int order; // task priority. A task with small order will be executed first

  protected UpgradeTask(String name, Version minVersion, Version maxVersion, int order) {
    this.name = Preconditions.checkNotNull(name);
    this.minVersion = Preconditions.checkNotNull(minVersion);
    this.maxVersion = Preconditions.checkNotNull(maxVersion);
    this.order = order;
  }

  public String getName() {
    return name;
  }

  public Version getMinVersion() {
    return minVersion;
  }

  public Version getMaxVersion() {
    return maxVersion;
  }

  public int getOrder() {
    return order;
  }

  public abstract void upgrade(UpgradeContext context) throws Exception;

  /**
   * Compare the current task with another one
   *
   * Tasks are compared based on their priority, and based on their name.
   * A task is smaller if the order is smaller, or if orders are equal
   * if name is smaller (from a lexicographically point of view)
   *
   * @param that another task
   * @return a negative, 0 or positive integer if the current task is smaller,
   * equals to or larger than the argument.
   */
  @Override
  public final int compareTo(UpgradeTask that) {
    return COMPARATOR.compare(this, that);
  }

  @Override
  public String toString() {
    return String.format("%s [%s, %s)", name, minVersion.getVersion(), maxVersion.getVersion());
  }
}
